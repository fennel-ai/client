use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{
    Array, ArrayData, ArrayRef, Decimal128Array, Decimal128Builder, FixedSizeListArray,
    Float64Array, Int32BufferBuilder, LargeListArray, ListArray,
};
use arrow::array::{BooleanBufferBuilder, StructArray};
use arrow::datatypes::{DataType, Fields, TimeUnit, DECIMAL128_MAX_PRECISION};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use itertools::Itertools;
use num_traits::ToPrimitive;
use rust_decimal::Decimal;
use serde_json::json;

use crate::schema::Field;
use crate::types::{DecimalType, StructType};
use crate::value::{Date, List, Map, UTCTimestamp};

use crate::df::Dataframe;
use crate::rowcol::Col;
use crate::schema::Schema;
use crate::types::Type;
use crate::value::Value;
use arrow::error::Result as ArrowResult;

pub fn to_arrow_recordbatch(df: &Dataframe) -> Result<RecordBatch> {
    let schema = to_arrow_schema(df.schema().as_ref());
    let mut arrays = Vec::with_capacity(df.num_cols());
    for col in df.cols() {
        arrays.push(to_arrow_array(col.values(), col.dtype(), false)?);
    }
    Ok(RecordBatch::try_new(Arc::new(schema), arrays)?)
}

// Creates a dataframe with the given schema from an arrow record batch.
// The schema is used to determine the column names and types.
// The record batch is allowed to have more columns than the schema as long as
// the schema columns are a subset of the record batch columns.
pub fn from_arrow_recordbatch(schema: Arc<Schema>, batch: &RecordBatch) -> Result<Dataframe> {
    let mut cols = Vec::with_capacity(batch.num_columns());
    for field in schema.fields() {
        let col_idx = batch
            .schema()
            .index_of(field.name())
            .map_err(|err| anyhow!("column '{}' not found in data: {:?}", field.name(), err))?;
        let col = batch.column(col_idx);
        // types are checked in `from_arrow_array`
        let values = from_arrow_array(col, field.dtype(), false).map_err(|err| {
            anyhow!(
                "error converting column '{}' to values: {:?}",
                field.name(),
                err
            )
        })?;
        cols.push(Col::new(Arc::new(field.clone()), Arc::new(values))?);
    }
    Ok(Dataframe::new(cols)?)
}

// Create an arrow array given a fennel type and list of fennel values.
pub fn to_arrow_array(values: &[Value], dtype: &Type, nullable: bool) -> Result<ArrayRef> {
    match dtype {
        // create an arrow array of all nulls of length equal to the number of values
        Type::Null => Ok(Arc::new(arrow::array::NullArray::new(values.len()))),
        Type::Optional(t) => to_arrow_array(values, t, true),
        Type::Int => {
            let values: Vec<Option<i64>> = values
                .iter()
                .map(|v| match v.as_int() {
                    Ok(v) => Ok(Some(v)),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    t => Err(anyhow!("invalid value {:?}", t)),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(arrow::array::Int64Array::from(values));
            Ok(array)
        }
        Type::Bool => {
            let values: Vec<Option<bool>> = values
                .iter()
                .map(|v| match v.as_bool() {
                    Ok(v) => Ok(Some(v)),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    t => Err(anyhow!("invalid value {:?}", t)),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(arrow::array::BooleanArray::from(values));
            Ok(array)
        }
        Type::Float => {
            let values: Vec<Option<f64>> = values
                .iter()
                .map(|v| match v.as_float() {
                    Ok(v) => Ok(Some(v)),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(arrow::array::Float64Array::from(values));
            Ok(array)
        }
        Type::String => {
            let values: Vec<Option<&str>> = values
                .iter()
                .map(|v| match v.as_str() {
                    Ok(v) => Ok(Some(v)),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(arrow::array::StringArray::from(values));
            Ok(array)
        }
        Type::Bytes => {
            let values: Vec<Option<&[u8]>> = values
                .iter()
                .map(|v| match v.as_bytes() {
                    Ok(v) => Ok(Some(v)),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(arrow::array::BinaryArray::from(values));
            Ok(array)
        }
        Type::Timestamp => {
            let values: Vec<Option<i64>> = values
                .iter()
                .map(|v| match v.as_timestamp() {
                    Ok(v) => Ok(Some(v.micros())),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef =
                // We don't do with_timezone_utc because this method uses "+00:00" as timezone
                // which is not supported by python pyarrow.
                Arc::new(arrow::array::TimestampMicrosecondArray::from(values).with_timezone("UTC"));
            Ok(array)
        }
        Type::Date => {
            let values: Vec<Option<i32>> = values
                .iter()
                .map(|v| match v.as_date() {
                    Ok(v) => Ok(Some(v.days() as i32)),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(None),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?;
            let array: ArrayRef = Arc::new(arrow::array::Date32Array::from(values));
            Ok(array)
        }
        Type::Map(t) => {
            let f_keys: Vec<Value> = values
                .iter()
                .map(|v| match v.as_map() {
                    Ok(v) => Ok(v
                        .keys()
                        .into_iter()
                        .map(|k| Value::String(Arc::new(k.to_string())))
                        .collect()),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(vec![]),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect();
            let f_values: Vec<Value> = values
                .iter()
                .map(|v| match v.as_map() {
                    Ok(v) => Ok(v.values().map(|v| v.clone()).collect::<Vec<_>>()),
                    Err(_) if nullable && matches!(v, Value::None) => Ok(vec![]),
                    _ => Err(anyhow!("invalid value")),
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            let mut offset_buffer = Int32BufferBuilder::new(values.len() + 1);
            let mut null_buffer = BooleanBufferBuilder::new(values.len());

            offset_buffer.append(0);
            let mut cur_offset = 0;
            let num_maps = values.len();
            for v in values.iter() {
                match v.as_map() {
                    Ok(v) => {
                        cur_offset += v.len() as i32;
                        offset_buffer.append(cur_offset);
                        null_buffer.append(true);
                    }
                    Err(_) if nullable && matches!(v, Value::None) => {
                        offset_buffer.append(cur_offset);
                        null_buffer.append(false);
                    }
                    _ => return Err(anyhow!("invalid value")),
                }
            }

            let keys = to_arrow_array(&f_keys, &Type::String, false)?;
            let values = to_arrow_array(&f_values, t.as_ref(), nullable)?;
            let key_field = arrow::datatypes::Field::new("__key__", DataType::Utf8, false);
            let value_field =
                arrow::datatypes::Field::new("__value__", values.data_type().clone(), nullable);
            // Construct the StructArray
            let struct_array = StructArray::from(vec![
                (Arc::new(key_field.clone()), keys),
                (Arc::new(value_field.clone()), values),
            ]);
            // Map will always contain a struct named __entries__ with key and value fields
            // values may be nullable but the struct is always present
            let struct_field = arrow::datatypes::Field::new(
                "__entries__",
                DataType::Struct(Fields::from(vec![key_field, value_field])),
                false,
            );
            // The bool field of map specifies whether or not the keys must be sorted
            let data_type = DataType::Map(Arc::new(struct_field), false);
            let ret = ArrayData::builder(data_type)
                .len(num_maps)
                .add_buffer(offset_buffer.finish())
                .add_child_data(struct_array.to_data())
                .null_bit_buffer(Some(null_buffer.finish().into_inner()))
                .build()?;
            Ok(Arc::new(arrow::array::make_array(ret)))
        }
        Type::Embedding(usize) => to_arrow_fixed_list_array(values, usize, nullable),
        Type::List(t) => to_arrow_list_array(values, t.as_ref(), nullable),
        Type::Between(b) => {
            return to_arrow_array(values, b.dtype(), nullable);
        }
        Type::OneOf(o) => {
            return to_arrow_array(values, o.dtype(), nullable);
        }
        Type::Regex(_) => {
            return to_arrow_array(values, &Type::String, nullable);
        }
        Type::Struct(s) => to_arrow_struct_array(s, values, nullable),
        Type::Decimal(d) => to_arrow_decimal_array(d, values, nullable),
    }
}

pub fn from_arrow_map_array(array: &ArrayRef, dtype: &Type, nullable: &bool) -> Result<Vec<Value>> {
    let map_array = array
        .as_any()
        .downcast_ref::<arrow::array::MapArray>()
        .unwrap();
    let mut result = Vec::with_capacity(map_array.len());

    for i in 0..map_array.len() {
        let map = map_array.value(i);
        let map = map.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = map.column(0);
        let vals = map.column(1);
        let values = from_arrow_array(&vals, dtype, *nullable)?;
        // Fetch keys from cur_offset to vals.len()
        let str_keys = keys
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();

        let mut data = Vec::with_capacity(keys.len());
        for (k, v) in str_keys.into_iter().zip(values.into_iter()) {
            data.push((k.unwrap().to_string(), v));
        }

        // Create a new Map instance
        let map = Arc::new(Map::new(dtype.clone(), &data)?);
        result.push(Value::Map(map));
    }

    Ok(result)
}

pub fn from_arrow_array(array: &ArrayRef, dtype: &Type, nullable: bool) -> Result<Vec<Value>> {
    use arrow::datatypes::DataType::*;
    match (dtype, array.data_type()) {
        (Type::Null, Null) => Ok(vec![Value::None; array.len()]),
        (Type::Optional(_), Null) => Ok(vec![Value::None; array.len()]),
        (Type::Optional(t), _) => from_arrow_array(array, t, true),
        (Type::Int, t) if t != &Utf8 => match t {
            Int64 => from_arrow_int64_array(array, nullable),
            Int32 => from_arrow_int32_array(array, nullable),
            Int16 => from_arrow_int16_array(array, nullable),
            Int8 => from_arrow_int8_array(array, nullable),
            UInt64 => from_arrow_uint64_array(array, nullable),
            UInt32 => from_arrow_uint32_array(array, nullable),
            UInt16 => from_arrow_uint16_array(array, nullable),
            UInt8 => from_arrow_uint8_array(array, nullable),
            Decimal128(_, scale) => {
                if *scale != 0 {
                    return Err(anyhow!("expected decimal ith scale 0 type, got {:?}", t));
                }
                int_from_arrow_decimal_array(array, nullable)
            }
            _ => Err(anyhow!("expected integer type, got {:?}", t)),
        },
        (Type::Bool, Boolean) => {
            let array = array
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::Bool(v)),
                    None if nullable => Ok(Value::None),
                    _ => Err(anyhow!("expected a boolean but found {:?} instead", v)),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
        (Type::Float, t) if t != &Utf8 => match t {
            Int64 => float_from_arrow_int64_array(array, nullable),
            Int32 => float_from_arrow_int32_array(array, nullable),
            Int16 => float_from_arrow_int16_array(array, nullable),
            Int8 => float_from_arrow_int8_array(array, nullable),
            UInt64 => float_from_arrow_uint64_array(array, nullable),
            UInt32 => float_from_arrow_uint32_array(array, nullable),
            UInt16 => float_from_arrow_uint16_array(array, nullable),
            UInt8 => float_from_arrow_uint8_array(array, nullable),
            Float16 => float_from_arrow_float16_array(array, nullable),
            Float32 => float_from_arrow_float32_array(array, nullable),
            Float64 => float_from_arrow_float64_array(array, nullable),
            Decimal128(_, _) => float_from_arrow_decimal_array(array),
            _ => Err(anyhow!("float field is of unsupported type: {:?}", t)),
        },

        (Type::Decimal(_), Decimal128(_, _)) => from_arrow_decimal_array(array),

        (Type::String, Utf8) => from_arrow_str_array(array, nullable),
        (Type::String, LargeUtf8) => str_from_arrow_largeutf8_array(array, nullable),
        (Type::Bytes, Binary) => from_arrow_bytes_array(array, nullable),

        // TODO(mohit): Explore if value::Timestamp should support both Micros and Nanos
        //
        // pandas.Series[datetime] -> TimestampNanosecondArray
        //
        // Question is when we convert this array into pyarrow and then later to pandas series datetime,
        // what will be the value. Need to test this and then decide whether keeping the
        // granularity of timestamp as micros makes sense
        (Type::Timestamp, Timestamp(unit, _)) => match unit {
            TimeUnit::Nanosecond => from_arrow_timestamp_nanoseconds_array(array),
            TimeUnit::Microsecond => from_arrow_timestamp_microseconds_array(array),
            TimeUnit::Millisecond => from_arrow_timestamp_milliseconds_array(array),
            TimeUnit::Second => from_arrow_timestamp_seconds_array(array),
        },
        (Type::Timestamp, t) if t != &Utf8 => match t {
            Int64 => timestamp_from_arrow_int64_array(array, nullable),
            Int32 => timestamp_from_arrow_int32_array(array, nullable),
            Int16 => timestamp_from_arrow_int16_array(array, nullable),
            Int8 => timestamp_from_arrow_int8_array(array, nullable),
            UInt64 => timestamp_from_arrow_uint64_array(array, nullable),
            UInt32 => timestamp_from_arrow_uint32_array(array, nullable),
            UInt16 => timestamp_from_arrow_uint16_array(array, nullable),
            UInt8 => timestamp_from_arrow_uint8_array(array, nullable),
            Date64 => timestamp_from_arrow_date64_array(array, nullable),
            Date32 => timestamp_from_arrow_date32_array(array, nullable),
            _ => Err(anyhow!("timestamp field is of unsupported type: {:?}", t)),
        },
        (Type::Date, Date32) => date_from_arrow_date32_array(array, nullable),
        (Type::Date, Date64) => date_from_arrow_date64_array(array, nullable),
        (Type::Date, t) if t != &Utf8 => match t {
            Int64 => date_from_arrow_int64_array(array, nullable),
            Int32 => date_from_arrow_int32_array(array, nullable),
            Int16 => date_from_arrow_int16_array(array, nullable),
            Int8 => date_from_arrow_int8_array(array, nullable),
            UInt64 => date_from_arrow_uint64_array(array, nullable),
            UInt32 => date_from_arrow_uint32_array(array, nullable),
            UInt16 => date_from_arrow_uint16_array(array, nullable),
            UInt8 => date_from_arrow_uint8_array(array, nullable),
            Date64 => date_from_arrow_date64_array(array, nullable),
            Date32 => date_from_arrow_date32_array(array, nullable),
            _ => Err(anyhow!("date field is of unsupported type: {:?}", t)),
        },
        (Type::Embedding(usize), FixedSizeList(field, x)) => {
            if *x != *usize as i32 {
                return Err(anyhow!("expected the length of the lists to match"));
            }
            if embedding_arrow_field() != **field {
                return Err(anyhow!(
                    "expected embedding field to match, found: {:?}",
                    field
                ));
            }
            let fixed_size_list_array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| anyhow!("unable to downcast array to fixed size list array"))?;
            from_arrow_fixed_list_array(fixed_size_list_array, usize)
        }
        (Type::Embedding(dim), List(_d)) => {
            let list_array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| anyhow!("unable to downcast array to list array"))?;

            let fixed_size_list_array = convert_to_fixed_size_list_array(list_array, *dim as i32)?;

            from_arrow_fixed_list_array(&fixed_size_list_array, dim)
        }
        (Type::List(t), List(_)) | (Type::List(t), LargeList(_)) => {
            let res = match array.data_type() {
                DataType::List(_) => from_arrow_list_array(array, t.as_ref(), nullable),
                DataType::LargeList(_) => from_arrow_large_list_array(array, t.as_ref(), nullable),
                _ => unreachable!(),
            };
            res.with_context(|| format!("unable to parse list of type {:?}", t))
        }
        (Type::Map(t), Map(s, nullable)) => {
            // assert that the types match

            // Extract key and value types from the map type
            let (k, v) = match s.data_type() {
                // Map type is a list type containing the struct
                // Struct type with two fields: key and value
                DataType::Struct(fields) => {
                    if fields.len() != 2 {
                        return Err(anyhow!(
                            "expected map type to have two fields, found: {:?}",
                            fields.len()
                        ));
                    }
                    let k = fields[0].data_type();
                    let v = fields[1].data_type();
                    (k, v)
                }
                _ => return Err(anyhow!("expected map type to be a struct, found: {:?}", s)),
            };

            if to_arrow_dtype(t.as_ref(), t.is_nullable()) != *v {
                return Err(anyhow!(
                    "expected map value type: {:?} to match, found: {:?}",
                    t,
                    k
                ));
            }

            if *k != Utf8 {
                return Err(anyhow!(
                    "expected map key type: {:?} to match, found: {:?}",
                    Utf8,
                    k
                ));
            }

            from_arrow_map_array(array, t.as_ref(), nullable)
        }
        (Type::Between(between_type), dt) => {
            return if *between_type.dtype() == Type::Int {
                if let Int64 = dt {
                    let values = from_arrow_int64_array(array, nullable)?;
                    for v in values.iter() {
                        if !v.matches(dtype) {
                            return Err(anyhow!(
                                "expected between type: {:?} to match, found: {:?}",
                                between_type,
                                dt
                            ));
                        }
                    }
                    return Ok(values);
                }
                Err(anyhow!(
                    "expected between type: {:?} to match, found: {:?}",
                    between_type,
                    dt
                ))
            } else {
                let values = match dt {
                    Float16 => float_from_arrow_float16_array(array, nullable),
                    Float32 => float_from_arrow_float32_array(array, nullable),
                    Float64 => float_from_arrow_float64_array(array, nullable),
                    _ => Err(anyhow!(
                        "expected between type: {:?} to match, found: {:?}",
                        between_type,
                        dt
                    )),
                }?;
                // Check that all values match between type
                for v in values.iter() {
                    if !v.matches(dtype) {
                        return Err(anyhow!(
                            "expected between type: {:?} to match, found: {:?}",
                            between_type,
                            dt
                        ));
                    }
                }
                return Ok(values);
            };
        }
        (Type::OneOf(one_of_type), dt) => {
            if *one_of_type.dtype() == Type::Int {
                if let Int64 = dt {
                    let values = from_arrow_int64_array(array, nullable)?;
                    for v in values.iter() {
                        if !v.matches(dtype) {
                            return Err(anyhow!(
                                "expected oneof type: {:?} to match, found: {:?}",
                                one_of_type.dtype(),
                                dt
                            ));
                        }
                    }
                    return Ok(values);
                }
                return Err(anyhow!(
                    "expected oneof type: {:?} to match, found: {:?}",
                    one_of_type.dtype(),
                    dt
                ));
            } else {
                if let Utf8 = dt {
                    let values = from_arrow_str_array(array, nullable)?;
                    for v in values.iter() {
                        if !v.matches(dtype) {
                            return Err(anyhow!(
                                "expected oneof type: {:?} to match, found: {:?}",
                                one_of_type.dtype(),
                                dt
                            ));
                        }
                    }
                    return Ok(values);
                }
                return Err(anyhow!(
                    "expected oneof type: {:?} to match, found: {:?}",
                    one_of_type.dtype(),
                    dt
                ));
            }
        }
        (Type::Regex(r), Utf8) => {
            let values = from_arrow_str_array(array, nullable)?;
            for v in values.iter() {
                if !v.matches(dtype) {
                    return Err(anyhow!(
                        "expected regex type: {:?} to match, found: {:?}",
                        r,
                        v
                    ));
                }
            }

            Ok(values)
        }
        (Type::Struct(s), Struct(_)) => {
            let array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut values = Vec::with_capacity(array.len());
            let mut all_column_values = Vec::with_capacity(s.num_fields());
            for f in s.fields().iter() {
                let col = &array.column_by_name(f.name());
                if col.is_none() {
                    if f.dtype().is_nullable() {
                        all_column_values.push(vec![Value::None; array.len()]);
                        continue;
                    } else {
                        return Err(anyhow!("column {} is not nullable", f.name()));
                    }
                }
                let col = col.unwrap();
                if col.len() != array.len() {
                    return Err(anyhow!(
                        "column length mismatch {} vs {}",
                        col.len(),
                        array.len()
                    ));
                }
                all_column_values.push(from_arrow_array(col, f.dtype(), nullable)?);
            }

            if all_column_values.len() == 0 {
                return Ok(values);
            }

            for row_index in 0..all_column_values[0].len() {
                let mut struct_fields = Vec::with_capacity(s.num_fields());

                // Check if all values for the row are None, and its Optional[Struct] set it to None.
                if nullable
                    && all_column_values
                        .iter()
                        .all(|column| column[row_index] == Value::None)
                {
                    values.push(Value::None);
                    continue;
                }

                for (i, f) in s.fields().iter().enumerate() {
                    struct_fields.push((f.name().into(), all_column_values[i][row_index].clone()));
                }

                let fennel_struct = crate::value::Struct::new(struct_fields)?;
                values.push(Value::Struct(Arc::new(fennel_struct)));
            }
            Ok(values)
        }
        // Sometimes, data is serialized as json string (e.g. semi-structured
        // snowflake types). As a fallback, we try to parse the string as json
        // and then convert it to the expected type.
        (_, Utf8) => {
            // parse utf8 as json and then try to convert it to the expected type
            array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap() // safe to unwrap since we checked the type above
                .iter()
                .map(|v| match (v, nullable) {
                    (None, true) => Ok(Value::None),
                    (None, false) => {
                        bail!("error parsing {} from string: missing string", dtype)
                    }
                    (Some(str), true) if str.eq("null") => Ok(Value::None),
                    (Some(str), true) if str.len() == 0 => Ok(Value::None),
                    (Some(str), false) if str.len() == 0 => {
                        bail!("error parsing {} from string: empty string", dtype)
                    }
                    (Some(str), _) => match Value::from_json_parsed(dtype, &json!(str)) {
                        Ok(v) => Ok(v),
                        // last effort: assume str is already properly json encoded
                        Err(_) => Value::from_json(dtype, str),
                    },
                })
                .collect::<Result<Vec<_>>>()
        }
        (_, LargeUtf8) => {
            // parse utf8 as json and then try to convert it to the expected type
            array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
                .unwrap() // safe to unwrap since we checked the type above
                .iter()
                .map(|v| match (v, nullable) {
                    (None, true) => Ok(Value::None),
                    (None, false) => {
                        bail!("error parsing {} from string: missing string", dtype)
                    }
                    (Some(str), true) if str.eq("null") => Ok(Value::None),
                    (Some(str), true) if str.len() == 0 => Ok(Value::None),
                    (Some(str), false) if str.len() == 0 => {
                        bail!("error parsing {} from string: empty string", dtype)
                    }
                    (Some(str), _) => match Value::from_json_parsed(dtype, &json!(str)) {
                        Ok(v) => Ok(v),
                        // last effort: assume str is already properly json encoded
                        Err(_) => Value::from_json(dtype, str),
                    },
                })
                .collect::<Result<Vec<_>>>()
        }
        (expected, actual) => bail!("expected type: {:?}, found: {:?}", expected, actual),
    }
}

/// Returns Arrow schema from Dataframe schema
pub fn to_arrow_schema(schema: &Schema) -> arrow::datatypes::Schema {
    let fields = schema
        .fields()
        .iter()
        .map(to_arrow_field)
        .collect::<Vec<_>>();
    arrow::datatypes::Schema::new(fields)
}

pub fn to_arrow_field(field: &Field) -> arrow::datatypes::Field {
    let dtype = to_arrow_dtype(field.dtype(), false);
    arrow::datatypes::Field::new(field.name(), dtype, field.is_nullable())
}

/// Returns field type for column data type.
/// The parameter nullable is useful for struct types, where if a struct
/// is optional all the sub fields of the struct have to be nullable,
/// since arrow is a columnar format.
///
/// This never errors out because every Fennel type has a corresponding
/// Arrow type.
pub fn to_arrow_dtype(dtype: &Type, nullable: bool) -> DataType {
    use arrow::datatypes::DataType::*;
    match dtype {
        Type::Null => Null,
        Type::Int => Int64,
        Type::Float => Float64,
        Type::String => Utf8,
        Type::Bytes => Binary,
        Type::Bool => Boolean,
        Type::Timestamp => Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Type::Date => Date32,
        Type::Embedding(n) => {
            let arrow_field = embedding_arrow_field();
            FixedSizeList(Arc::new(arrow_field), *n as i32)
        }
        Type::Optional(t) => to_arrow_dtype(t, true),
        Type::List(t) => {
            // We ignore the external nullability of the list type, because arrow data type for T
            // in List[T] is determined by the internal nullability of T, and not the external nullability.
            let dtype = to_arrow_dtype(t, t.is_nullable());
            let arrow_field = list_arrow_field(dtype, t.is_nullable());
            List(Arc::new(arrow_field))
        }
        Type::Map(t) => {
            let key_field = arrow::datatypes::Field::new("__key__", Utf8, false);
            let (value_type, nullable) = to_arrow_dtype_with_nullability(t);
            let value_field = arrow::datatypes::Field::new("__value__", value_type, nullable);
            let struct_type = Struct(Fields::from(vec![key_field, value_field]));
            let struct_field = arrow::datatypes::Field::new("__entries__", struct_type, false);
            Map(Arc::new(struct_field), false)
        }
        Type::Between(b) => match b.dtype() {
            Type::Float => Float64,
            Type::Int => Int64,
            _ => unreachable!(),
        },
        Type::OneOf(o) => match o.dtype() {
            Type::Int => Int64,
            Type::String => Utf8,
            _ => unreachable!(),
        },
        Type::Regex(_) => Utf8,
        Type::Struct(s) => {
            let fields: Vec<_> = s
                .fields()
                .iter()
                .map(|f| {
                    let dtype = to_arrow_dtype(f.dtype(), nullable);
                    arrow::datatypes::Field::new(
                        f.name(),
                        dtype,
                        f.dtype().is_nullable() || nullable,
                    )
                })
                .collect();

            Struct(Fields::from(fields))
        }
        Type::Decimal(d) => Decimal128(DECIMAL128_MAX_PRECISION, d.scale() as i8),
    }
}

fn to_arrow_dtype_with_nullability(dtype: &Type) -> (DataType, bool) {
    if let Type::Optional(t) = dtype {
        (to_arrow_dtype(t, true), true)
    } else {
        (to_arrow_dtype(dtype, false), false)
    }
}

//-----------------------------------------------------------------------------
// Decode / Encode a RecordBatch
//-----------------------------------------------------------------------------

/// Encode a RecordBatch to Bytes
pub fn serialize_recordbatch(record_batch: &RecordBatch) -> Result<Bytes> {
    let mut writer_buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut writer_buffer, &record_batch.schema())?;
        writer.write(&record_batch)?;
        writer.finish()?;
    } // drop the writer
    Ok(writer_buffer.into())
}

/// Decode a RecordBatch from a Bytes
pub fn deserialize_recordbatch(data: Bytes) -> Result<RecordBatch> {
    let cursor = std::io::Cursor::new(data);
    let mut reader = StreamReader::try_new(cursor, None)?;

    if let Some(read_result) = reader.next() {
        match read_result {
            Ok(record_batch) => Ok(record_batch),
            Err(err) => Err(anyhow!("error reading record batch: {}", err)),
        }
    } else {
        Err(anyhow!("no record batch found"))
    }
}

//-----------------------------------------------------------------------------
// Private helpers below
//-----------------------------------------------------------------------------
//
macro_rules! from_arrow_int_array {
    ($name:ident, $arrow_type:ty) => {
        fn $name(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
            let array = array.as_any().downcast_ref::<$arrow_type>().unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::Int(v as i64)),
                    None if nullable => Ok(Value::None),
                    None => Err(anyhow!(
                        "expected an integer but found None instead for column"
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
    };
}

from_arrow_int_array!(from_arrow_int64_array, arrow::array::Int64Array);
from_arrow_int_array!(from_arrow_int32_array, arrow::array::Int32Array);
from_arrow_int_array!(from_arrow_int16_array, arrow::array::Int16Array);
from_arrow_int_array!(from_arrow_int8_array, arrow::array::Int8Array);
from_arrow_int_array!(from_arrow_uint64_array, arrow::array::UInt64Array);
from_arrow_int_array!(from_arrow_uint32_array, arrow::array::UInt32Array);
from_arrow_int_array!(from_arrow_uint16_array, arrow::array::UInt16Array);
from_arrow_int_array!(from_arrow_uint8_array, arrow::array::UInt8Array);

/// from_arrow_decimal_array converts an arrow decimal array to a vector of values of type Float.
fn int_from_arrow_decimal_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            anyhow!(
                "Failed to cast to Decimal128Array, found: {:?}",
                array.data_type()
            )
        })?;
    let scale = array.scale();
    let decimal_scale: u32 = scale
        .try_into()
        .map_err(|_| anyhow!("Invalid scale value: {}", scale))?;
    if decimal_scale != 0 {
        return Err(anyhow!(
            "Invalid scale {} found in Decimal128Array for conversion to int.",
            decimal_scale
        ));
    }
    let mut values = Vec::with_capacity(array.len());
    for val in array {
        match val {
            Some(v) => {
                let decimal =
                    Decimal::try_from_i128_with_scale(v, decimal_scale).map_err(|err| {
                        anyhow!(
                            "failed to parse arrow decimal value {} with scale {} with error: {:?}",
                            v,
                            decimal_scale,
                            err
                        )
                    })?;
                let as_i64 = decimal
                    .to_i64()
                    .ok_or_else(|| anyhow!("Failed to convert decimal {} to int", v))?;
                values.push(Value::Int(as_i64));
            }
            None if nullable => values.push(Value::None),
            None => {
                return Err(anyhow!(
                    "expected an decimal with scale 0 but found None instead"
                ));
            }
        }
    }
    Ok(values)
}

macro_rules! timestamp_from_arrow_native_array {
    ($name:ident, $arrow_type:ty) => {
        fn $name(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
            let array = array.as_any().downcast_ref::<$arrow_type>().unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::Timestamp(UTCTimestamp::from_json_parsed(&json!(v))?)),
                    None if nullable => Ok(Value::None),
                    None => Err(anyhow!("expected an integer but found None instead")),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
    };
}

timestamp_from_arrow_native_array!(timestamp_from_arrow_int64_array, arrow::array::Int64Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_int32_array, arrow::array::Int32Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_int16_array, arrow::array::Int16Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_int8_array, arrow::array::Int8Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_uint32_array, arrow::array::UInt32Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_uint64_array, arrow::array::UInt64Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_uint16_array, arrow::array::UInt16Array);
timestamp_from_arrow_native_array!(timestamp_from_arrow_uint8_array, arrow::array::UInt8Array);

macro_rules! date_from_arrow_native_array {
    ($name:ident, $arrow_type:ty) => {
        fn $name(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
            let array = array.as_any().downcast_ref::<$arrow_type>().unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::Date(Date::from_json_parsed(&json!(v))?)),
                    None if nullable => Ok(Value::None),
                    None => Err(anyhow!("expected an integer but found None instead")),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
    };
}

date_from_arrow_native_array!(date_from_arrow_int64_array, arrow::array::Int64Array);
date_from_arrow_native_array!(date_from_arrow_int32_array, arrow::array::Int32Array);
date_from_arrow_native_array!(date_from_arrow_int16_array, arrow::array::Int16Array);
date_from_arrow_native_array!(date_from_arrow_int8_array, arrow::array::Int8Array);
date_from_arrow_native_array!(date_from_arrow_uint32_array, arrow::array::UInt32Array);
date_from_arrow_native_array!(date_from_arrow_uint64_array, arrow::array::UInt64Array);
date_from_arrow_native_array!(date_from_arrow_uint16_array, arrow::array::UInt16Array);
date_from_arrow_native_array!(date_from_arrow_uint8_array, arrow::array::UInt8Array);

macro_rules! float_from_arrow_native_array {
    ($name:ident, $arrow_type:ty) => {
        fn $name(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
            let array = array.as_any().downcast_ref::<$arrow_type>().unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::Float(v.into())),
                    None if nullable => Ok(Value::None),
                    None => Err(anyhow!("expected a non-null float but found None instead")),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
    };
}

macro_rules! float_from_arrow_int_array {
    ($name:ident, $arrow_type:ty) => {
        fn $name(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
            let array = array.as_any().downcast_ref::<$arrow_type>().unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::Float(v as f64)),
                    None if nullable => Ok(Value::None),
                    None => Err(anyhow!("expected a non-null float but found None instead")),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
    };
}

float_from_arrow_native_array!(float_from_arrow_float64_array, Float64Array);
float_from_arrow_native_array!(float_from_arrow_float32_array, arrow::array::Float32Array);
float_from_arrow_native_array!(float_from_arrow_float16_array, arrow::array::Float16Array);
float_from_arrow_int_array!(float_from_arrow_int8_array, arrow::array::Int8Array);
float_from_arrow_int_array!(float_from_arrow_int16_array, arrow::array::Int16Array);
float_from_arrow_int_array!(float_from_arrow_int32_array, arrow::array::Int32Array);
float_from_arrow_int_array!(float_from_arrow_int64_array, arrow::array::Int64Array);
float_from_arrow_int_array!(float_from_arrow_uint8_array, arrow::array::UInt8Array);
float_from_arrow_int_array!(float_from_arrow_uint16_array, arrow::array::UInt16Array);
float_from_arrow_int_array!(float_from_arrow_uint32_array, arrow::array::UInt32Array);
float_from_arrow_int_array!(float_from_arrow_uint64_array, arrow::array::UInt64Array);

/// from_arrow_decimal_array converts an arrow decimal array to a vector of values of type Float.
fn float_from_arrow_decimal_array(array: &ArrayRef) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            anyhow!(
                "Failed to cast to Decimal128Array, found: {:?}",
                array.data_type()
            )
        })?;
    let scale = array.scale();
    let decimal_scale: u32 = scale
        .try_into()
        .map_err(|_| anyhow!("Invalid scale value: {}", scale))?;
    let mut values = Vec::with_capacity(array.len());
    for val in array {
        match val {
            Some(v) => {
                let decimal =
                    Decimal::try_from_i128_with_scale(v, decimal_scale).map_err(|err| {
                        anyhow!(
                            "failed to parse arrow decimal value {} with scale {} with error: {:?}",
                            v,
                            decimal_scale,
                            err
                        )
                    })?;
                let as_f64 = decimal
                    .to_f64()
                    .ok_or_else(|| anyhow!("Failed to convert decimal {} to float", v))?;
                values.push(Value::Float(as_f64));
            }
            None => values.push(Value::None),
        }
    }
    Ok(values)
}

/// from_arrow_decimal_array converts an arrow decimal array to a vector of values of type DecimalType.
fn from_arrow_decimal_array(array: &ArrayRef) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            anyhow!(
                "Failed to cast to Decimal128Array, found: {:?}",
                array.data_type()
            )
        })?;
    let scale = array.scale();
    let decimal_scale: u32 = scale
        .try_into()
        .map_err(|_| anyhow!("Invalid scale value: {}", scale))?;
    let mut values = Vec::with_capacity(array.len());
    for val in array {
        match val {
            Some(v) => {
                let decimal =
                    Decimal::try_from_i128_with_scale(v, decimal_scale).map_err(|err| {
                        anyhow!(
                            "failed to parse arrow decimal value {} with scale {} with error: {:?}",
                            v,
                            decimal_scale,
                            err
                        )
                    })?;
                values.push(Value::Decimal(Arc::new(decimal)));
            }
            None => values.push(Value::None),
        }
    }
    Ok(values)
}

macro_rules! str_from_arrow_native_array {
    ($name:ident, $arrow_type:ty) => {
        fn $name(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
            let array = array.as_any().downcast_ref::<$arrow_type>().unwrap();
            let values = array
                .iter()
                .map(|v| match v {
                    Some(v) => Ok(Value::String(Arc::new(v.into()))),
                    None if nullable => Ok(Value::None),
                    None => Err(anyhow!("expected a non-null string but found None instead")),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(values)
        }
    };
}

str_from_arrow_native_array!(
    str_from_arrow_largeutf8_array,
    arrow::array::LargeStringArray
);

fn from_arrow_str_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::String(Arc::new(v.to_string()))),
            None if nullable => Ok(Value::None),
            _ => Err(anyhow!("expected a string but found {:?} instead", v)),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn from_arrow_bytes_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Bytes(Bytes::from(v.to_vec()))),
            None if nullable => Ok(Value::None),
            _ => Err(anyhow!("expected a bytes but found {:?} instead", v)),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn from_arrow_timestamp_microseconds_array(array: &ArrayRef) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
        .unwrap();
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Timestamp(UTCTimestamp::from(v))),
            None => Ok(Value::None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn from_arrow_timestamp_nanoseconds_array(array: &ArrayRef) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap();
    let values = array
        .iter()
        .map(|v| match v {
            Some(nanos) => Ok(Value::Timestamp(UTCTimestamp::from(nanos / 1000))),
            None => Ok(Value::None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn from_arrow_timestamp_milliseconds_array(array: &ArrayRef) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampMillisecondArray>()
        .unwrap();
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Timestamp(UTCTimestamp::from(v * 1000))),
            None => Ok(Value::None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

/// Converts a date64 array to Value::Timestamp. An arrow date64 is an
/// i64 representing the number of milliseconds since the unix epoch.
fn timestamp_from_arrow_date64_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::Date64Array>()
        .ok_or_else(|| anyhow!("Failed to cast to Date64Array"))?;
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Timestamp(UTCTimestamp::from(v * 1000))),
            None if nullable => Ok(Value::None),
            None => Err(anyhow!("expected a date64 but found None instead")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

/// Converts a date32 array to Value::Timestamp. An arrow date32 is an
/// i32 representing the number of days since the unix epoch.
fn timestamp_from_arrow_date32_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .ok_or_else(|| anyhow!("Failed to cast to Date32Array"))?;
    let values = array
        .iter()
        .map(|v| match v {
            // convert days to microseconds
            Some(v) => Ok(Value::Timestamp(UTCTimestamp::from(
                v as i64 * (24 * 60 * 60) as i64 * 1_000_000_i64,
            ))),
            None if nullable => Ok(Value::None),
            None => Err(anyhow!("expected a date32 but found None instead")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn from_arrow_timestamp_seconds_array(array: &ArrayRef) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::TimestampSecondArray>()
        .unwrap();
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Timestamp(UTCTimestamp::from(v * 1000 * 1000))),
            None => Ok(Value::None),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

/// Converts a date64 array to Value::Date. An arrow date64 is an
/// i64 representing the number of milliseconds since the unix epoch.
fn date_from_arrow_date64_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::Date64Array>()
        .ok_or_else(|| anyhow!("Failed to cast to Date64Array"))?;
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Date(Date::from_milli_secs(v))),
            None if nullable => Ok(Value::None),
            None => Err(anyhow!("expected a date64 but found None instead")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

/// Converts a date32 array to Value::Date. An arrow date32 is an
/// i32 representing the number of days since the unix epoch.
fn date_from_arrow_date32_array(array: &ArrayRef, nullable: bool) -> Result<Vec<Value>> {
    let array = array
        .as_any()
        .downcast_ref::<arrow::array::Date32Array>()
        .ok_or_else(|| anyhow!("Failed to cast to Date64Array"))?;
    let values = array
        .iter()
        .map(|v| match v {
            Some(v) => Ok(Value::Date(Date::from(v as i64))),
            None if nullable => Ok(Value::None),
            None => Err(anyhow!("expected a date32 but found None instead")),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

fn embedding_arrow_field() -> arrow::datatypes::Field {
    arrow::datatypes::Field::new("__embedding__", DataType::Float64, false)
}

fn list_arrow_field(dtype: DataType, is_nullable: bool) -> arrow::datatypes::Field {
    arrow::datatypes::Field::new("__list__", dtype, is_nullable)
}

fn to_arrow_fixed_list_array(values: &[Value], usize: &usize, nullable: bool) -> Result<ArrayRef> {
    // TODO(mohit): Try to early validate that teh datatype of Value is `embedding`

    // create a embedding type - this should exactly match the `embedding` arrow type we
    // construct for the record batch schema
    let embedding_type = Arc::new(embedding_arrow_field());
    let data_type = DataType::FixedSizeList(embedding_type, *usize as i32);

    // create a buffer of Option<f64> values - see this as a flattened slice
    let mut float_data: Vec<Option<f64>> = Vec::with_capacity(*usize * values.len());

    // create a null buffer which indicates which entries in the fixed list are valid
    // i.e have non-null values and which are not i.e. null
    let mut null_buffer = BooleanBufferBuilder::new(values.len());

    // fill the `float_data` and `null_buffer` values
    for v in values {
        match v.as_embedding() {
            Ok(embedding) => {
                // assert that the length matches
                if embedding.len() != *usize {
                    return Err(anyhow!("embeddings length does not match expected length"));
                }

                // add the data and mark this set of entries as valid
                float_data.extend(embedding.iter().map(|e| Some(e.clone())));
                null_buffer.append(true);
            }
            Err(_) => {
                // if the data type is allowed to be None, fill expected number of None
                // values and mark this batch of entries as invalid
                if nullable {
                    float_data.extend(vec![None; *usize]);
                    null_buffer.append(false);
                } else {
                    return Err(anyhow!(
                        "invalid data for Embedding; should be vec<f64>, found: {:?}",
                        v
                    ));
                }
            }
        };
    }

    // construct the fixed length array
    let array_data_builder = ArrayData::builder(data_type)
        .len(values.len())
        // Fixed List builder requires a single array data
        .add_child_data(Float64Array::from(float_data).into_data())
        .null_bit_buffer(Some(null_buffer.finish().into_inner()));
    let resp = Arc::new(FixedSizeListArray::from(array_data_builder.build()?));
    Ok(resp)
}

/// to_arrow_list_array converts a vector of Values to an Arrow ListArray.
///
/// `values` correspond to a column in Fennel dataframe and `dtype` to be the underlying type of the list.
/// `nullable` is set to true if the dtype of the column is optional.
///
/// The conversion is assymetric and convuluted because of how we have done df -> arrow conversion historically.
/// We compute the Fennel datatype -> arrow datatype in a different function, where the logic is a bit different. Then
/// we compute it again in value conversions (because the Arrow data structures need to be provided one) but this is
/// a bit different from the previous function.
///
/// TODO(DEV-3231, mohit): Try to unify/build better abstractions for dtype and value conversions for df -> arrow.
fn to_arrow_list_array(values: &[Value], dtype: &Type, nullable: bool) -> Result<ArrayRef> {
    // List type's nullability should be determined by the dtype and not if the parent field is nullable
    let arrow_type = to_arrow_dtype(dtype, dtype.is_nullable());
    let list_type = Arc::new(list_arrow_field(arrow_type, dtype.is_nullable()));
    let data_type = DataType::List(list_type);

    // preprocess the values to fail early but also compute the capacity of the flattened data vec
    let mut cap = 0;
    for val in values {
        match val {
            Value::None if nullable => {}
            // If parent type is not nullable, then the list should not be nullable, throw an error
            Value::None => {
                return Err(anyhow!(
                    "invalid data for List; should be List<{:?}>, found: None",
                    dtype
                ));
            }
            Value::List(list_val) => {
                // verify the dtype of the list, must match the expected dtype
                if dtype != list_val.dtype() {
                    return Err(anyhow!(
                        "list dtype does not match expected dtype: {:?} != {:?}",
                        dtype,
                        list_val.dtype()
                    ));
                }
                cap += list_val.len();
            }
            _ => {
                return Err(anyhow!(
                    "invalid data for List; should be List<{:?}>, nullable: {}, found: {:?}",
                    dtype,
                    nullable,
                    val
                ));
            }
        }
    }

    // create data buffer - this is a flattened slice
    let mut data: Vec<Value> = Vec::with_capacity(cap);

    // create offset buffer - this is required to specify where the list starts from and ends
    let mut offset_buffer = Int32BufferBuilder::new(values.len() + 1);

    // create null buffer
    let mut null_buffer = BooleanBufferBuilder::new(values.len());

    let mut offset = 0;
    // push the first offset to mark the beginning of an entry
    offset_buffer.append(offset as i32);

    for value in values {
        match value {
            Value::None if nullable => {
                // if the data type is allowed to be None, fill expected number of None
                null_buffer.append(false);
                // NOTE: offset is not incremented here, since we are not adding any data
            }
            Value::None => {
                // should be unreachable since we checked above
                unreachable!(
                    "invalid data for List; should be List<{:?}>, found: None",
                    dtype
                );
            }
            Value::List(list_val) => {
                // construct the data buffer
                let vals = list_val.iter().map(|v| v.clone()).collect::<Vec<Value>>();
                offset += vals.len();
                data.extend(vals);
                // a value was found, mark it as valid
                null_buffer.append(true);
            }
            _ => {
                return Err(anyhow!(
                    "invalid data for List; should be List<{:?}>, found: {:?}",
                    dtype,
                    value
                ));
            }
        }
        offset_buffer.append(offset as i32);
    }

    // nullable here should be defined by the dtype
    let arrow_array = to_arrow_array(&data, dtype, dtype.is_nullable())?;

    let list_data = ArrayData::builder(data_type)
        .len(values.len())
        .add_child_data(arrow_array.to_data())
        .add_buffer(offset_buffer.finish())
        .null_bit_buffer(Some(null_buffer.finish().into_inner()))
        .build()?;

    Ok(Arc::new(ListArray::from(list_data)))
}

fn from_arrow_large_list_array(
    array: &ArrayRef,
    inner: &Type,
    nullable: bool,
) -> Result<Vec<Value>> {
    let array = array.as_any().downcast_ref::<LargeListArray>();
    if array.is_none() {
        return Err(anyhow!("expected the array input to be ListArray"));
    }
    let array = array.unwrap(); // this is fine since we just asserted above

    let mut output = Vec::with_capacity(array.len());
    for elem in array.iter() {
        if let Some(elem) = elem {
            let list_val = match from_arrow_array(&elem, inner, nullable) {
                Ok(vals) => Value::List(Arc::new(List::new(inner.clone(), &vals)?)),
                Err(e) if !nullable => {
                    // sometimes an empty list is returned as None
                    // try parsing again but this time with nullable set to True
                    let vals = from_arrow_array(&elem, inner, true)?;
                    if vals.len() == 0 || (vals.len() == 1 && vals[0].eq(&Value::None)) {
                        Value::List(Arc::new(List::new(inner.clone(), &[])?))
                    } else {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            };
            output.push(list_val);
        } else {
            if !nullable {
                return Err(anyhow!(
                    "invalid data for List; data type is not optional: {:?}, found null: {:?}",
                    inner,
                    elem
                ));
            }
            output.push(Value::None);
        }
    }
    Ok(output)
}

// Function to convert ListArray to FixedSizeListArray
fn convert_to_fixed_size_list_array(
    list_array: &ListArray,
    element_size: i32,
) -> ArrowResult<Arc<FixedSizeListArray>> {
    let field =
        arrow::datatypes::Field::new("__embedding__", arrow::datatypes::DataType::Float64, false);

    // Collect all values into a single flat array
    let mut values: Vec<Option<f64>> = Vec::with_capacity(list_array.len() * element_size as usize);

    for i in 0..list_array.len() {
        let value_ref = list_array.value(i);
        let sublist = value_ref
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                arrow::error::ArrowError::CastError(
                    "Failed to cast sublist to Float64Array".to_string(),
                )
            })?;

        if sublist.len() as i32 != element_size {
            return Err(arrow::error::ArrowError::InvalidArgumentError(format!(
                "Sublist at index {} does not match the fixed size of {} elements.",
                i, element_size
            )));
        }
        values.extend(sublist.values().iter().map(|v| Some(*v)));
    }

    let value_array = Float64Array::from(values);
    // Create the FixedSizeListArray
    let fixed_size_list_array = FixedSizeListArray::try_new(
        field.into(),
        element_size,
        Arc::new(value_array) as ArrayRef,
        None,
    )?;

    Ok(Arc::new(fixed_size_list_array))
}

fn from_arrow_fixed_list_array(array: &FixedSizeListArray, len: &usize) -> Result<Vec<Value>> {
    // assert that the data type is correct including the value type
    let data_type = array.data_type();
    let expected_type = DataType::FixedSizeList(Arc::new(embedding_arrow_field()), *len as i32);
    if !data_type.eq(&expected_type) {
        return Err(anyhow!(
            "data type mismatch to convert from fixed list. Expected: {:?}, got: {:?}",
            expected_type,
            data_type
        ));
    }

    // convert the data back to a vector of Embeddings
    let mut embedding_vec = Vec::with_capacity(array.len());

    // fixed length array has a single value
    for i in 0..array.len() {
        let array_slice = array.value(i);
        let float_arr = array_slice.as_any().downcast_ref::<Float64Array>();
        if float_arr.is_none() {
            return Err(anyhow!("expected the array slice to be of type Float64"));
        }
        let float_arr = float_arr.unwrap();

        // if all the values are null, set Value::None
        if float_arr.null_count() == *len {
            embedding_vec.push(Value::None);
            continue;
        }

        // if there are no null values, copy the values into the output
        if float_arr.null_count() == 0 {
            embedding_vec.push(Value::Embedding(Arc::new(
                float_arr.values().into_iter().cloned().collect_vec(),
            )));
            continue;
        }

        // this should never happen
        return Err(anyhow!(
            "found float array with: {} null values which is not allowed for embeddings",
            float_arr.null_count()
        ));
    }
    Ok(embedding_vec)
}

fn from_arrow_list_array(array: &ArrayRef, inner: &Type, nullable: bool) -> Result<Vec<Value>> {
    let array = array.as_any().downcast_ref::<ListArray>();
    if array.is_none() {
        return Err(anyhow!("expected the array input to be ListArray"));
    }
    let array = array.unwrap(); // this is fine since we just asserted above
    let mut output = Vec::with_capacity(array.len());
    for elem in array.iter() {
        if let Some(elem) = elem {
            let list_val = match from_arrow_array(&elem, inner, nullable) {
                Ok(vals) => Value::List(Arc::new(List::new(inner.clone(), &vals)?)),
                Err(e) if !nullable => {
                    // sometimes an empty list is returned as None
                    // try parsing again but this time with nullable set to True
                    let vals = from_arrow_array(&elem, inner, true)?;
                    if vals.len() == 0 || (vals.len() == 1 && vals[0].eq(&Value::None)) {
                        Value::List(Arc::new(List::new(inner.clone(), &[])?))
                    } else {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            };
            output.push(list_val);
        } else {
            if !nullable {
                return Err(anyhow!(
                    "invalid data for List; data type is not optional: {:?}, found null: {:?}",
                    inner,
                    elem
                ));
            }
            output.push(Value::None);
        }
    }
    Ok(output)
}

fn to_arrow_struct_array(
    s: &Box<StructType>,
    values: &[Value],
    nullable: bool,
) -> Result<ArrayRef> {
    let mut arrow_arrays_per_fields = Vec::with_capacity(s.num_fields());

    if values.is_empty() {
        for f in s.fields().iter() {
            let arrow_type = to_arrow_dtype(f.dtype(), nullable);
            let field =
                arrow::datatypes::Field::new(f.name(), arrow_type.clone(), f.dtype().is_nullable());
            arrow_arrays_per_fields
                .push((Arc::new(field), arrow::array::new_empty_array(&arrow_type)));
        }
        let struct_array = StructArray::from(arrow_arrays_per_fields);
        return Ok(Arc::new(struct_array) as ArrayRef);
    }

    let value_structs = values
        .iter()
        .map(|v| v.as_struct(nullable))
        .collect::<Result<Vec<_>>>()?;

    // Collect all values for each field.
    let mut values_per_field = HashMap::new();

    let type_fields = s.fields();
    for row_index in 0..value_structs.len() {
        // Iterate parallely over the fields and the values of the current row, since both
        // are sorted by name.
        let mut value_struct_index = 0;
        let mut type_index = 0;

        // Safe to unwrap since index bounds are met
        let obj = value_structs.get(row_index).unwrap();
        // If the value itself is None, insert None for all fields
        if obj.is_none() {
            if !nullable {
                return Err(anyhow!("struct is not nullable but has null values"));
            }
            for f in s.fields().iter() {
                values_per_field
                    .entry(f.name().into())
                    .or_insert_with(|| Vec::with_capacity(values.len()))
                    .push(Value::None);
            }
            continue;
        }
        let obj = obj.unwrap().data();
        while type_index < s.num_fields() {
            let type_field = type_fields.get(type_index).unwrap();
            if value_struct_index == obj.len() {
                if !type_field.dtype().is_nullable() {
                    return Err(anyhow!(
                        "field {:?} is not nullable but missing from data",
                        type_field.name()
                    ));
                }
                type_index += 1;
            }

            let (field_name, val) = obj.get(value_struct_index).unwrap();
            if field_name == type_field.name() {
                values_per_field
                    .entry(field_name.clone())
                    .or_insert_with(|| Vec::with_capacity(values.len()))
                    .push(val.clone());
                value_struct_index += 1;
                type_index += 1;
            } else if type_field.name() < field_name as &str {
                // Any field which is missing from data, should be nullable
                if type_field.dtype().is_nullable() {
                    values_per_field
                        .entry(type_field.name().into())
                        .or_insert_with(|| Vec::with_capacity(values.len()))
                        .push(Value::None);
                    type_index += 1;
                } else {
                    return Err(anyhow!(
                        "field {:?} is not nullable but missing from data",
                        type_field.name()
                    ));
                }
            } else {
                // Additional field in data, not in type
                return Err(anyhow!(
                    "Additional field {:?} found in data but not in type",
                    field_name
                ));
            }
        }
    }

    let num_rows = values.len();
    for f in s.fields().iter() {
        let (field_name, dtype) = (f.name(), f.dtype());
        let values = values_per_field.get(field_name);
        let dtype = if nullable {
            Type::optional(dtype.clone())
        } else {
            dtype.clone()
        };
        let arrow_type = to_arrow_dtype(&dtype, nullable);

        if values.is_none() {
            if !dtype.is_nullable() {
                return Err(anyhow!(
                    "field {:?} is not nullable but has no values",
                    field_name
                ));
            } else {
                arrow_arrays_per_fields.push((
                    Arc::new(arrow::datatypes::Field::new(
                        field_name,
                        DataType::Null,
                        true,
                    )),
                    arrow::array::new_null_array(&arrow_type, num_rows),
                ));
                continue;
            }
        }
        let arrow_array = to_arrow_array(&values.unwrap(), &dtype, dtype.is_nullable())?;
        let arrow_field = arrow::datatypes::Field::new(field_name, arrow_type, dtype.is_nullable());
        arrow_arrays_per_fields.push((Arc::new(arrow_field), arrow_array));
    }

    let struct_array = StructArray::from(arrow_arrays_per_fields);
    Ok(Arc::new(struct_array) as ArrayRef)
}

fn to_arrow_decimal_array(
    d: &Box<DecimalType>,
    values: &[Value],
    nullable: bool,
) -> Result<ArrayRef> {
    let scale = d.scale();
    let mut decimal_array = Decimal128Builder::with_capacity(values.len())
        .with_precision_and_scale(DECIMAL128_MAX_PRECISION, scale as i8)?;

    for value in values {
        match value.as_decimal() {
            Ok(decimal) => {
                let i128_value = decimal.mantissa();
                decimal_array.append_option(Some(i128_value));
            }
            Err(_) => {
                if nullable {
                    decimal_array.append_option(None);
                } else {
                    return Err(anyhow!(
                        "invalid data for Decimal; should be vec<Decimal128>, found: {:?}",
                        value
                    ));
                }
            }
        }
    }

    Ok(Arc::new(decimal_array.finish()))
}

/// Returns true if an arrow array of type `arrow_type` and
/// nullable flag `nullable` can be safely promoted to a Fennel type `dtype`.
/// If this is true, then there should never be a runtime error when converting
/// the arrow array of the given type to an array of the Fennel type.
///
/// Note that this is a much stronger condition than just being able to convert/
/// parse the arrow array to the Fennel type. This function checks if the
/// conversion can be done without any data loss or runtime errors.
pub fn can_safely_promote(atype: &DataType, nullable: bool, dtype: &Type) -> bool {
    if let Type::Optional(inner) = dtype {
        match atype {
            DataType::Null => true,
            _ => can_safely_promote(atype, false, inner.as_ref()),
        }
    } else if nullable {
        false
    } else {
        match dtype {
            Type::Null => matches!(atype, DataType::Null),
            Type::Optional(_) => unreachable!(),
            Type::Int => matches!(
                atype,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ),
            Type::Float => matches!(
                atype,
                DataType::Float16
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ),
            Type::Decimal(_) => matches!(atype, DataType::Decimal128(_, _)),
            Type::Bool => matches!(atype, DataType::Boolean),
            Type::Timestamp => matches!(
                atype,
                DataType::Timestamp(TimeUnit::Second, None)
                    | DataType::Timestamp(TimeUnit::Millisecond, None)
                    | DataType::Timestamp(TimeUnit::Microsecond, None)
                    | DataType::Timestamp(TimeUnit::Nanosecond, None)
            ),
            Type::Date => matches!(atype, DataType::Date32 | DataType::Date64),
            Type::String => matches!(atype, DataType::Utf8),
            Type::Bytes => matches!(atype, DataType::Binary),
            Type::Embedding(usize) => match atype {
                DataType::FixedSizeList(field, len) => {
                    *len == *usize as i32
                        && can_safely_promote(field.data_type(), field.is_nullable(), &Type::Float)
                }
                _ => false,
            },
            Type::List(t) => match atype {
                DataType::List(field) => {
                    can_safely_promote(field.data_type(), field.is_nullable(), t)
                }
                _ => false,
            },
            Type::Map(t) => match atype {
                DataType::Map(struc, _sorted) => {
                    // Map type is a list type containing the struct
                    // Struct type with two fields: key and value
                    match struc.data_type() {
                        DataType::Struct(fields) => {
                            if fields.len() != 2 {
                                return false;
                            }
                            let k = fields[0].data_type();
                            let v = fields[1].data_type();
                            can_safely_promote(&k, fields[0].is_nullable(), &Type::String)
                                && can_safely_promote(&v, fields[1].is_nullable(), t)
                        }
                        _ => false,
                    }
                }
                _ => false,
            },
            Type::Struct(s) => match atype {
                DataType::Struct(afields) => {
                    if s.fields().len() != afields.len() {
                        return false;
                    }
                    for f in s.fields().iter() {
                        let a = afields.iter().find(|a| a.name() == f.name());
                        if a.is_none() {
                            return false;
                        }
                        let a = a.unwrap();
                        if !can_safely_promote(a.data_type(), a.is_nullable(), f.dtype()) {
                            return false;
                        }
                    }
                    return true;
                }
                _ => false,
            },
            // Dynamic types like OneOf, Between, Regex can not be safely promoted
            // from any arrow type
            Type::Between(_) => false,
            Type::OneOf(_) => false,
            Type::Regex(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Date32Array, Date64Array};
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::Int32Type;
    use arrow::record_batch::RecordBatch;
    use DataType;

    use crate::rowcol::Row;
    use crate::schema::DSSchema;
    use crate::types::{Between, CompiledRegex, OneOf, StructType};
    use crate::value::{Struct, UTCTimestamp};

    use super::*;

    const ALPHA_NUMBERIC_REGEX: &str = "[a-zA-Z]+[0-9]";

    fn primitive_df() -> Dataframe {
        Dataframe::new(vec![
            Col::new(
                Arc::new(Field::new("a".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b".to_string(), Type::Float)),
                Arc::new(vec![
                    Value::Float(1.0),
                    Value::Float(2.0),
                    Value::Float(3.0),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("c".to_string(), Type::String)),
                Arc::new(vec![
                    Value::String(Arc::new("a".to_string())),
                    Value::String(Arc::new("b".to_string())),
                    Value::String(Arc::new("c".to_string())),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("d".to_string(), Type::Bool)),
                Arc::new(vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("e".to_string(), Type::Timestamp)),
                Arc::new(vec![
                    Value::Timestamp(UTCTimestamp::from(1)),
                    Value::Timestamp(UTCTimestamp::from(2)),
                    Value::Timestamp(UTCTimestamp::from(3)),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("f".to_string(), Type::Bytes)),
                Arc::new(vec![
                    Value::Bytes(Bytes::from("random hello world string")),
                    Value::Bytes(Bytes::from("another random string")),
                    Value::Bytes(Bytes::from(vec![1, 2, 3])),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("g".to_string(), Type::Null)),
                Arc::new(vec![Value::None, Value::None, Value::None]),
            )
            .unwrap(),
        ])
        .unwrap()
    }

    fn primitive_optional_df() -> Dataframe {
        Dataframe::new(vec![
            Col::new(
                Arc::new(Field::new(
                    "a".to_string(),
                    Type::Optional(Box::new(Type::Int)),
                )),
                Arc::new(vec![Value::Int(1), Value::Int(2), Value::None]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new(
                    "b".to_string(),
                    Type::Optional(Box::new(Type::Float)),
                )),
                Arc::new(vec![Value::Float(1.0), Value::None, Value::Float(2.0)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new(
                    "c".to_string(),
                    Type::Optional(Box::new(Type::String)),
                )),
                Arc::new(vec![
                    Value::String(Arc::new("a".to_string())),
                    Value::String(Arc::new("b".to_string())),
                    Value::None,
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new(
                    "d".to_string(),
                    Type::Optional(Box::new(Type::Bool)),
                )),
                Arc::new(vec![Value::None, Value::Bool(false), Value::None]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new(
                    "e".to_string(),
                    Type::Optional(Box::new(Type::Timestamp)),
                )),
                Arc::new(vec![Value::None, Value::None, Value::None]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new(
                    "f".to_string(),
                    Type::Optional(Box::new(Type::Bytes)),
                )),
                Arc::new(vec![
                    Value::None,
                    Value::Bytes(Bytes::from("hello world")),
                    Value::None,
                ]),
            )
            .unwrap(),
        ])
        .unwrap()
    }

    #[test]
    fn test_from_record_batch_inconsistent_schemas() {
        // schema length do not match
        {
            let ddschema = DSSchema::new(
                vec![Field::new("f1".to_string(), Type::String)],
                "ts".to_string(),
                vec![Field::new("f3".to_string(), Type::Int)],
            )
            .expect("failed to create ds schema");
            let schema = Arc::new(ddschema.to_schema());

            let a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
            let ts: ArrayRef =
                Arc::new(TimestampMicrosecondArray::from(vec![10 as i64, 20 as i64]));
            let b: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
            let record_batch =
                RecordBatch::try_from_iter(vec![("f1", a.clone()), ("f3", b.clone())])
                    .expect("failed to create record batch");

            // record batch is missing ts
            from_arrow_recordbatch(schema.clone(), &record_batch).unwrap_err();

            // record batch has more columns than schema. This is OK.
            let record_batch = RecordBatch::try_from_iter(vec![
                ("f1", a.clone()),
                ("f3", b.clone()),
                ("ts", ts.clone()),
                ("a2", a.clone()),
            ])
            .expect("failed to create record batch");
            from_arrow_recordbatch(schema.clone(), &record_batch).unwrap();
        }
    }

    #[test]
    fn test_primitive() {
        let df1 = primitive_df();
        let rb = to_arrow_recordbatch(&df1).unwrap();
        let schema = df1.schema().clone();
        let df2 = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(df1, df2);
    }

    #[test]
    fn test_optional_type_with_null_array() {
        let arr = arrow::array::new_null_array(&DataType::Utf8, 2);
        let dtype = Type::Optional(Box::new(Type::String));
        let values = from_arrow_array(&arr, &dtype, true).unwrap();
        assert_eq!(values, vec![Value::None, Value::None]);
        // Test with a different type.
        let arr = arrow::array::new_null_array(&DataType::Int8, 2);
        let dtype = Type::Optional(Box::new(Type::Int));
        let values = from_arrow_array(&arr, &dtype, true).unwrap();
        assert_eq!(values, vec![Value::None, Value::None]);
    }

    #[test]
    fn test_primitive_optional_types() {
        let df1 = primitive_optional_df();
        let rb = to_arrow_recordbatch(&df1).unwrap();
        let df2 = from_arrow_recordbatch(df1.schema().clone(), &rb).unwrap();
        assert_eq!(df1, df2);
    }

    #[test]
    fn test_empty() {
        // verify that empty dataframes can be converted to arrow and back
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Float),
                Field::new("c".to_string(), Type::String),
                Field::new("d".to_string(), Type::Optional(Box::new(Type::Bool))),
                Field::new("e".to_string(), Type::Bytes),
            ])
            .unwrap(),
        );
        let df1 = Dataframe::from_rows(schema.clone(), &[]).unwrap();
        let rb = to_arrow_recordbatch(&df1).unwrap();
        let df2 = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(df1, df2);
        assert_eq!(df1.schema(), df2.schema());
        assert_eq!(0, df2.num_rows());
    }

    #[test]
    fn test_embedding_type() {
        // failure scenarios
        {
            // the length embedding provided does not match
            let schema = Arc::new(
                Schema::new(vec![Field::new("foo".to_string(), Type::Embedding(2))]).unwrap(),
            );
            let df = Dataframe::from_rows(
                schema.clone(),
                &[Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Embedding(Arc::new(vec![1.0, 2.0]))]),
                )
                .unwrap()],
            )
            .unwrap();

            let rb = to_arrow_recordbatch(&df).unwrap();

            from_arrow_recordbatch(
                Arc::new(
                    Schema::new(vec![Field::new("foo".to_string(), Type::Embedding(3))]).unwrap(),
                ),
                &rb,
            )
            .unwrap_err();

            to_arrow_array(
                &[Value::None, Value::Embedding(Arc::new(vec![10.0, 20.0]))],
                &Type::Embedding(1),
                true,
            )
            .unwrap_err();
        }

        // nullable disabled
        {
            to_arrow_array(
                &[Value::None, Value::Embedding(Arc::new(vec![10.0, 20.0]))],
                &Type::Embedding(2),
                false,
            )
            .unwrap_err();

            to_arrow_array(
                &[Value::Int(2), Value::Embedding(Arc::new(vec![10.0, 20.0]))],
                &Type::Embedding(2),
                false,
            )
            .unwrap_err();
        }

        // Type list also works
        {
            let raw_schema = Arc::new(
                Schema::new(vec![Field::new(
                    "foo".to_string(),
                    Type::List(Box::new(Type::Float)),
                )])
                .unwrap(),
            );
            let df = Dataframe::from_rows(
                raw_schema.clone(),
                &[
                    Row::new(
                        raw_schema.clone(),
                        Arc::new(vec![Value::List(Arc::new(
                            List::new(
                                Type::Float,
                                &vec![Value::Float(1.0), Value::Float(2.0), Value::Float(4.0)],
                            )
                            .unwrap(),
                        ))]),
                    )
                    .unwrap(),
                    Row::new(
                        raw_schema.clone(),
                        Arc::new(vec![Value::List(Arc::new(
                            List::new(
                                Type::Float,
                                &vec![Value::Float(11.0), Value::Float(132.0), Value::Float(41.0)],
                            )
                            .unwrap(),
                        ))]),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let rb = to_arrow_recordbatch(&df).unwrap();
            let embedding_schema = Arc::new(
                Schema::new(vec![Field::new("foo".to_string(), Type::Embedding(3))]).unwrap(),
            );
            let df2 = Dataframe::from_rows(
                embedding_schema.clone(),
                &[
                    Row::new(
                        embedding_schema.clone(),
                        Arc::new(vec![Value::Embedding(Arc::new(vec![1.0, 2.0, 4.0]))]),
                    )
                    .unwrap(),
                    Row::new(
                        embedding_schema.clone(),
                        Arc::new(vec![Value::Embedding(Arc::new(vec![11.0, 132.0, 41.0]))]),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let actual_df = from_arrow_recordbatch(embedding_schema.clone(), &rb).unwrap();
            assert_eq!(actual_df, df2);

            // Different embedding size should throw error
            let df = Dataframe::from_rows(
                raw_schema.clone(),
                &[
                    Row::new(
                        raw_schema.clone(),
                        Arc::new(vec![Value::List(Arc::new(
                            List::new(
                                Type::Float,
                                &vec![Value::Float(1.0), Value::Float(2.0), Value::Float(4.0)],
                            )
                            .unwrap(),
                        ))]),
                    )
                    .unwrap(),
                    Row::new(
                        raw_schema.clone(),
                        Arc::new(vec![Value::List(Arc::new(
                            List::new(Type::Float, &vec![Value::Float(11.0), Value::Float(41.0)])
                                .unwrap(),
                        ))]),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let rb = to_arrow_recordbatch(&df).unwrap();
            from_arrow_recordbatch(embedding_schema.clone(), &rb).unwrap_err();
        }

        // sunny scenarios

        // create a dataframe with Embedding field and try to encode to RecordBatch and back
        {
            let schema = Arc::new(
                Schema::new(vec![
                    Field::new("foo".to_string(), Type::Embedding(5)),
                    Field::new("bar".to_string(), Type::Int),
                    Field::new(
                        "optional_foo".to_string(),
                        Type::Optional(Box::new(Type::Embedding(2))),
                    ),
                ])
                .unwrap(),
            );
            let df = Dataframe::from_rows(
                schema.clone(),
                &[
                    Row::new(
                        schema.clone(),
                        Arc::new(vec![
                            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
                            Value::Int(10),
                            Value::None,
                        ]),
                    )
                    .unwrap(),
                    Row::new(
                        schema.clone(),
                        Arc::new(vec![
                            Value::Embedding(Arc::new(vec![10.0, 20.0, 30.0, 40.0, 50.0])),
                            Value::Int(20),
                            Value::Embedding(Arc::new(vec![20.0, 40.0])),
                        ]),
                    )
                    .unwrap(),
                ],
            )
            .unwrap();
            let rb = to_arrow_recordbatch(&df).unwrap();

            // assert that the column has custom data type
            let col = rb.column(0);
            assert!(col.data_type().eq(&DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(
                    "__embedding__".to_string(),
                    DataType::Float64,
                    false,
                )),
                5,
            )));

            // convert back to df and see that the type of the field is Embedding
            let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
            let col = actual_df.select("foo").unwrap();
            assert!(col.dtype().eq(&Type::Embedding(5)));
            // assert values
            assert_eq!(
                col.values(),
                &[
                    Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
                    Value::Embedding(Arc::new(vec![10.0, 20.0, 30.0, 40.0, 50.0]))
                ]
            );

            // assert the optional embedding field
            let col = actual_df.select("optional_foo").unwrap();
            assert!(col
                .dtype()
                .eq(&Type::Optional(Box::new(Type::Embedding(2)))));

            // assert values
            assert_eq!(
                col.values(),
                &[Value::None, Value::Embedding(Arc::new(vec![20.0, 40.0]))]
            )
        }

        // create a col with Embedding type and try to encode to Array and back
        {
            let arr = to_arrow_array(
                &[Value::None, Value::Embedding(Arc::new(vec![10.0, 20.0]))],
                &Type::Embedding(2),
                true,
            )
            .unwrap();
            assert!(arr.data_type().eq(&DataType::FixedSizeList(
                Arc::new(arrow::datatypes::Field::new(
                    "__embedding__".to_string(),
                    DataType::Float64,
                    false,
                )),
                2,
            )));

            let actual = from_arrow_array(&arr, &Type::Embedding(2), true).unwrap();
            assert_eq!(
                actual,
                vec![Value::None, Value::Embedding(Arc::new(vec![10.0, 20.0]))]
            );

            let _ = to_arrow_array(
                &[
                    Value::Embedding(Arc::new(vec![1.0, 2.0])),
                    Value::Embedding(Arc::new(vec![10.0, 20.0])),
                ],
                &Type::Embedding(2),
                true,
            )
            .unwrap(); // success
        }
    }

    #[test]
    fn test_timestamp_type() {
        // Test int array.
        {
            let arr = to_arrow_array(
                &[Value::Int(2344), Value::Int(2758793422349)],
                &Type::Int,
                false,
            )
            .unwrap();
            let actual = from_arrow_array(&arr, &Type::Timestamp, false).unwrap();
            assert_eq!(
                actual,
                vec![
                    Value::Timestamp(UTCTimestamp::from_secs(2344)),
                    Value::Timestamp(UTCTimestamp::from_millis(2758793422349)),
                ]
            );
        }
        // Test string array.
        {
            let vals = vec![
                Value::String(Arc::new("2023-09-11T12:52:20.000000".to_string())),
                // can parse even when string is already json encoded
                Value::String(Arc::new("\"2023-09-11T12:52:20.000000\"".to_string())),
                Value::String(Arc::new("2023-09-11T12:52:20.000000+08:30".to_string())),
                Value::String(Arc::new("2023-09-11T12:52:20.000000-08:30".to_string())),
                Value::String(Arc::new("2023-09-11 12:52:20.000000-08:30".to_string())),
                Value::String(Arc::new("2016-09-20 23:15:17.063+00".to_string())),
                Value::String(Arc::new("1971-01-03".to_string())),
                Value::String(Arc::new("1969-Dec-25".to_string())),
                Value::String(Arc::new("1970.33".to_string())),
                Value::String(Arc::new("Dec 2, 2023".to_string())),
            ];
            let arr = to_arrow_array(&vals, &Type::String, false).unwrap();
            let actual = from_arrow_array(&arr, &Type::Timestamp, false).unwrap();
            assert_eq!(
                actual,
                vec![
                    Value::Timestamp(UTCTimestamp::from(1694436740000000)),
                    Value::Timestamp(UTCTimestamp::from(1694436740000000)),
                    Value::Timestamp(UTCTimestamp::from(1694406140000000)),
                    Value::Timestamp(UTCTimestamp::from(1694467340000000)),
                    Value::Timestamp(UTCTimestamp::from(1694467340000000)),
                    Value::Timestamp(UTCTimestamp::from(1474413317063000)),
                    Value::Timestamp(UTCTimestamp::from(31708800000000)),
                    Value::Timestamp(UTCTimestamp::from(-604800000000)),
                    Value::Timestamp(UTCTimestamp::from(2764800000000)),
                    Value::Timestamp(UTCTimestamp::from(1701475200000000)),
                ]
            );
        }

        // Test date32 and date64
        {
            let expected_secs: Vec<i64> = vec![
                0,           // Jan  1, 1970
                2764800,     // Feb  2, 1970
                -604800,     // Dec 25, 1969
                31708800,    // Jan  3, 1971
                1701475200,  // Dec  2, 2023
                -1701475200, // Feb  1, 1916
            ];

            let expected_ts: Vec<_> = expected_secs
                .iter()
                .map(|t| Value::Timestamp(UTCTimestamp::from_secs(*t)))
                .collect();

            let days_since_epoch = vec![
                0,      // Jan 1, 1970
                32,     // Feb 2, 1970
                -7,     // Dec 25, 1969
                367,    // Jan 3, 1971
                19693,  // Dec 2, 2023
                -19693, // Feb 1, 1916
            ];

            let date32_arr = Arc::new(Date32Array::new(days_since_epoch.into(), None)) as ArrayRef;
            let timestamps = from_arrow_array(&date32_arr, &Type::Timestamp, false).unwrap();
            assert_eq!(timestamps, expected_ts);

            // date64 - millis since epoch
            let epoch_millis: Vec<i64> = expected_secs.iter().map(|t| t * 1000).collect();
            let date64_arr = Arc::new(Date64Array::new(epoch_millis.into(), None)) as ArrayRef;
            let timestamps = from_arrow_array(&date64_arr, &Type::Timestamp, false).unwrap();
            assert_eq!(timestamps, expected_ts);
        }
    }

    #[test]
    fn test_date_type() {
        // Test int array.
        {
            let arr = to_arrow_array(&[Value::Int(1), Value::Int(2)], &Type::Int, false).unwrap();
            let actual = from_arrow_array(&arr, &Type::Date, false).unwrap();
            assert_eq!(
                actual,
                vec![Value::Date(Date::from(1)), Value::Date(Date::from(2)),]
            );
        }
        // Test string array.
        {
            let vals = vec![
                Value::String(Arc::new("1971-01-03".to_string())),
                // can parse even when string is already json encoded
                Value::String(Arc::new("\"1971-01-03\"".to_string())),
                Value::String(Arc::new("1969-Dec-25".to_string())),
                Value::String(Arc::new("1970.33".to_string())),
                Value::String(Arc::new("Dec 2, 2023".to_string())),
            ];
            let arr = to_arrow_array(&vals, &Type::String, false).unwrap();
            let actual = from_arrow_array(&arr, &Type::Date, false).unwrap();
            assert_eq!(
                actual,
                vec![
                    Value::Date(Date::try_from("1971-01-03").unwrap()),
                    Value::Date(Date::try_from("1971-01-03").unwrap()),
                    Value::Date(Date::try_from("1969-Dec-25").unwrap()),
                    Value::Date(Date::try_from("1970.33").unwrap()),
                    Value::Date(Date::try_from("Dec 2, 2023").unwrap()),
                ]
            );
        }

        // Test date32 and date64
        {
            let epoch_millis: Vec<i64> = vec![
                0,              // Jan  1, 1970
                2764800000,     // Feb  2, 1970
                -604800000,     // Dec 25, 1969
                31708800000,    // Jan  3, 1971
                1701475200000,  // Dec  2, 2023
                -1701475200000, // Feb  1, 1916
            ];

            let expected_dates: Vec<_> = epoch_millis
                .iter()
                .map(|t| Value::Date(Date::from_milli_secs(*t)))
                .collect();

            let days_since_epoch = vec![
                0,      // Jan 1, 1970
                32,     // Feb 2, 1970
                -7,     // Dec 25, 1969
                367,    // Jan 3, 1971
                19693,  // Dec 2, 2023
                -19693, // Feb 1, 1916
            ];

            let date32_arr = Arc::new(Date32Array::new(days_since_epoch.into(), None)) as ArrayRef;
            let dates = from_arrow_array(&date32_arr, &Type::Date, false).unwrap();
            assert_eq!(dates, expected_dates);

            // date64 - millis since epoch
            let date64_arr = Arc::new(Date64Array::new(epoch_millis.into(), None)) as ArrayRef;
            let dates = from_arrow_array(&date64_arr, &Type::Date, false).unwrap();
            assert_eq!(dates, expected_dates);
        }
    }

    #[test]
    fn test_bytes() {
        let data = vec![
            Value::Bytes(Bytes::from("hello world")),
            Value::Bytes(Bytes::from("my world")),
            Value::Bytes(Bytes::from(vec![7, 8, 9])),
        ];
        let arr = to_arrow_array(&data, &Type::Bytes, false).unwrap();
        let actual = from_arrow_array(&arr, &Type::Bytes, false).unwrap();
        assert_eq!(actual, data);
    }

    #[test]
    fn test_decimal_to_float() {
        {
            let expected_vals: Vec<i128> = vec![
                20,
                -30,
                // loss of accuracy
                18_446_744_073_709_551_6150i128,
                // loss of accuracy
                -9_223_372_036_854_775_8080i128,
            ];
            let mut decimal128_arr = Decimal128Builder::with_capacity(expected_vals.len())
                .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 2)
                .unwrap();
            decimal128_arr.append_values(&expected_vals, &vec![true, true, true, true]);

            let decimal128_arr = Arc::new(decimal128_arr.finish()) as ArrayRef;
            let actual = from_arrow_array(&decimal128_arr, &Type::Float, false).unwrap();
            let actual_f64 = actual
                .iter()
                .map(|v| v.as_float().unwrap())
                .collect::<Vec<_>>();
            assert_eq!(
                actual_f64,
                vec![0.20, -0.30, 1.8446744073709553e18, -9.223372036854776e17]
            );
        }
    }

    #[test]
    fn test_decimal_to_int() {
        let expected_vals: Vec<i128> = vec![20, -30, 10000, 100000];
        let mut decimal128_arr = Decimal128Builder::with_capacity(expected_vals.len())
            .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 0)
            .unwrap();
        decimal128_arr.append_values(&expected_vals, &vec![true, true, true, true]);

        let decimal128_arr = Arc::new(decimal128_arr.finish()) as ArrayRef;
        let actual = from_arrow_array(&decimal128_arr, &Type::Int, false).unwrap();
        let actual_i64 = actual
            .iter()
            .map(|v| v.as_int().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(actual_i64, vec![20, -30, 10000, 100000]);
    }

    #[test]
    fn test_invalid_decimal_to_int() {
        let expected_vals: Vec<i128> = vec![20, -30, 10000, 100000];
        let mut decimal128_arr = Decimal128Builder::with_capacity(expected_vals.len())
            .with_precision_and_scale(DECIMAL128_MAX_PRECISION, 2)
            .unwrap();
        decimal128_arr.append_values(&expected_vals, &vec![true, true, true, true]);

        let decimal128_arr = Arc::new(decimal128_arr.finish()) as ArrayRef;
        assert!(from_arrow_array(&decimal128_arr, &Type::Int, false).is_err());
    }

    #[test]
    fn test_list_type_invalid_type() {
        // array type
        {
            // type mismatch
            to_arrow_array(
                &[
                    Value::None,
                    Value::List(Arc::new(List::new(Type::Int, &[Value::Int(1)]).unwrap())),
                ],
                &Type::List(Box::new(Type::Float)),
                true,
            )
            .unwrap_err();

            // can parse from int -> float
            let data = vec![Some(vec![]), None, Some(vec![Some(1), Some(2), Some(3)])];
            let list_array: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data));
            from_arrow_array(&list_array, &Type::List(Box::new(Type::Float)), true).unwrap(); // note unwrap, not unwrap_err

            // optional mismatch
            to_arrow_array(
                &[
                    Value::None,
                    Value::List(Arc::new(List::new(Type::Int, &[Value::Int(1)]).unwrap())),
                ],
                &Type::List(Box::new(Type::Int)),
                false,
            )
            .unwrap_err();

            let data = vec![Some(vec![]), None, Some(vec![Some(1), Some(2), Some(3)])];
            let list_array: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data));
            from_arrow_array(&list_array, &Type::List(Box::new(Type::Float)), false).unwrap_err();
        }

        // recordbatch type
        {
            let data = vec![Some(vec![]), None, Some(vec![Some(1), Some(2), Some(3)])];
            let list_array: ArrayRef =
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data));
            let input = RecordBatch::try_from_iter(vec![("a", list_array)]).unwrap();

            // can parse as float
            from_arrow_recordbatch(
                Arc::new(
                    Schema::new(vec![Field::new(
                        "a".to_string(),
                        Type::Optional(Box::new(Type::List(Box::new(Type::Float)))),
                    )])
                    .unwrap(),
                ),
                &input,
            )
            .unwrap(); // note unwrap, not unwrap_err

            // optional mismatch
            from_arrow_recordbatch(
                Arc::new(
                    Schema::new(vec![Field::new(
                        "a".to_string(),
                        Type::List(Box::new(Type::Float)),
                    )])
                    .unwrap(),
                ),
                &input,
            )
            .unwrap_err();
        }
    }

    #[test]
    fn test_list_type_array_translations() {
        // primitive type, nullable
        {
            let data = vec![
                Value::None,
                Value::List(Arc::new(
                    List::new(
                        Type::Optional(Box::new(Type::Int)),
                        &vec![Value::Int(2), Value::None],
                    )
                    .unwrap(),
                )),
                Value::List(Arc::new(
                    List::new(Type::Optional(Box::new(Type::Int)), &vec![Value::Int(1)]).unwrap(),
                )),
                Value::List(Arc::new(
                    List::new(Type::Optional(Box::new(Type::Int)), &vec![Value::None]).unwrap(),
                )),
            ];
            let array = to_arrow_array(
                &data,
                &Type::Optional(Box::new(Type::List(Box::new(Type::Optional(Box::new(
                    Type::Int,
                )))))),
                false,
            )
            .unwrap();
            assert_eq!(
                array.data_type(),
                &DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "__list__".to_string(),
                    DataType::Int64,
                    true,
                )))
            );

            let vals = from_arrow_array(
                &array,
                &Type::Optional(Box::new(Type::List(Box::new(Type::Optional(Box::new(
                    Type::Int,
                )))))),
                false,
            )
            .unwrap();
            assert_eq!(vals, data,);
        }

        // primitive type, not nullable
        {
            let data = vec![
                Value::List(Arc::new(
                    List::new(Type::Int, &vec![Value::Int(2), Value::Int(10)]).unwrap(),
                )),
                Value::List(Arc::new(
                    List::new(Type::Int, &vec![Value::Int(1)]).unwrap(),
                )),
                Value::List(Arc::new(
                    List::new(
                        Type::Int,
                        &vec![Value::Int(100), Value::Int(200), Value::Int(300)],
                    )
                    .unwrap(),
                )),
            ];
            let array = to_arrow_array(&data, &Type::List(Box::new(Type::Int)), false).unwrap();
            assert_eq!(
                array.data_type(),
                &DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "__list__".to_string(),
                    DataType::Int64,
                    false,
                )))
            );

            let vals = from_arrow_array(&array, &Type::List(Box::new(Type::Int)), false).unwrap();
            assert_eq!(vals, data,);
        }

        // complex type
        {
            // List[List[int]]
            let data = vec![
                Value::List(Arc::new(
                    List::new(
                        Type::List(Box::new(Type::Int)),
                        &vec![
                            Value::List(Arc::new(
                                List::new(Type::Int, &vec![Value::Int(1), Value::Int(2)]).unwrap(),
                            )),
                            Value::List(Arc::new(
                                List::new(
                                    Type::Int,
                                    &vec![Value::Int(20), Value::Int(30), Value::Int(40)],
                                )
                                .unwrap(),
                            )),
                        ],
                    )
                    .unwrap(),
                )),
                Value::None,
                Value::List(Arc::new(
                    List::new(
                        Type::List(Box::new(Type::Int)),
                        &vec![
                            Value::List(Arc::new(
                                List::new(Type::Int, &vec![Value::Int(-1), Value::Int(-2)])
                                    .unwrap(),
                            )),
                            Value::List(Arc::new(
                                List::new(
                                    Type::Int,
                                    &vec![Value::Int(-20), Value::Int(-30), Value::Int(-40)],
                                )
                                .unwrap(),
                            )),
                        ],
                    )
                    .unwrap(),
                )),
            ];

            let array = to_arrow_array(
                &data,
                &Type::Optional(Box::new(Type::List(Box::new(Type::List(Box::new(
                    Type::Int,
                )))))),
                false,
            )
            .unwrap();
            assert_eq!(
                array.data_type(),
                &DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "__list__".to_string(),
                    DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "__list__".to_string(),
                        DataType::Int64,
                        false,
                    ))),
                    false,
                )))
            );

            let vals = from_arrow_array(
                &array,
                &Type::Optional(Box::new(Type::List(Box::new(Type::List(Box::new(
                    Type::Int,
                )))))),
                false,
            )
            .unwrap();
            assert_eq!(vals, data,);
        }
    }

    #[test]
    fn test_list_type_recordbatch_translations() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new(
                    "foo".to_string(),
                    Type::Optional(Box::new(Type::List(Box::new(Type::Float)))),
                ),
                Field::new("bar".to_string(), Type::List(Box::new(Type::Int))),
                Field::new(
                    "bar_optional".to_string(),
                    Type::List(Box::new(Type::Optional(Box::new(Type::Int)))),
                ),
            ])
            .unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(
                    schema.clone(),
                    Arc::new(vec![
                        Value::List(Arc::new(
                            List::new(
                                Type::Float,
                                &[Value::Float(10.0), Value::Float(20.0), Value::Float(30.0)],
                            )
                            .unwrap(),
                        )),
                        Value::List(Arc::new(
                            List::new(Type::Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                        )),
                        Value::List(Arc::new(
                            List::new(
                                Type::Optional(Box::new(Type::Int)),
                                &[Value::None, Value::Int(0)],
                            )
                            .unwrap(),
                        )),
                    ]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![
                        Value::None,
                        Value::List(Arc::new(
                            List::new(Type::Int, &[Value::Int(10), Value::Int(20)]).unwrap(),
                        )),
                        Value::List(Arc::new(
                            List::new(Type::Optional(Box::new(Type::Int)), &[Value::Int(0)])
                                .unwrap(),
                        )),
                    ]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(
            rb.column(0).data_type(),
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "__list__".to_string(),
                DataType::Float64,
                false,
            )))
        );
        assert_eq!(
            rb.column(1).data_type(),
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "__list__".to_string(),
                DataType::Int64,
                false,
            )))
        );
        assert_eq!(
            rb.column(2).data_type(),
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "__list__".to_string(),
                DataType::Int64,
                true,
            )))
        );

        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_between_type_recordbatch_translations() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new(
                    "foo".to_string(),
                    Type::Between(
                        Between::new(
                            Type::Float,
                            Value::Float(1.0),
                            Value::Float(2.0),
                            false,
                            false,
                        )
                        .unwrap(),
                    ),
                ),
                Field::new(
                    "bar".to_string(),
                    Type::Between(
                        Between::new(Type::Int, Value::Int(2), Value::Int(6), false, false)
                            .unwrap(),
                    ),
                ),
            ])
            .unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Float(1.6), Value::Int(2)]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Float(1.7), Value::Int(3)]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(rb.column(0).data_type(), &DataType::Float64);
        assert_eq!(rb.column(1).data_type(), &DataType::Int64);

        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_oneof_type_recordbatch_translations() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new(
                    "foo".to_string(),
                    Type::OneOf(
                        OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)])
                            .unwrap(),
                    ),
                ),
                Field::new(
                    "bar".to_string(),
                    Type::OneOf(
                        OneOf::new(
                            Type::String,
                            vec![
                                Value::String(Arc::new("a".to_string())),
                                Value::String(Arc::new("b".to_string())),
                            ],
                        )
                        .unwrap(),
                    ),
                ),
                Field::new(
                    "baz".to_string(),
                    Type::Regex(CompiledRegex::new(ALPHA_NUMBERIC_REGEX.to_string()).unwrap()),
                ),
            ])
            .unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(
                    schema.clone(),
                    Arc::new(vec![
                        Value::Int(2),
                        Value::String(Arc::new("a".to_string())),
                        Value::String(Arc::new("Rahul8".to_string())),
                    ]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![
                        Value::Int(3),
                        Value::String(Arc::new("b".to_string())),
                        Value::String(Arc::new("Shyam7".to_string())),
                    ]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(rb.column(0).data_type(), &DataType::Int64);
        assert_eq!(rb.column(1).data_type(), &DataType::Utf8);
        assert_eq!(rb.column(2).data_type(), &DataType::Utf8);

        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_map_simple_value_type() {
        let schema = Arc::new(
            Schema::new(vec![Field::new(
                "foo".to_string(),
                Type::Map(Box::new(Type::Int)),
            )])
            .unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Map(Arc::new(
                        Map::new(
                            Type::Int,
                            &[
                                ("aa".to_string(), Value::Int(1)),
                                ("bb".to_string(), Value::Int(2)),
                                ("ee".to_string(), Value::Int(5)),
                                ("eeee".to_string(), Value::Int(445)),
                            ],
                        )
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Map(Arc::new(
                        Map::new(
                            Type::Int,
                            &[
                                ("cc".to_string(), Value::Int(11)),
                                ("dd".to_string(), Value::Int(22)),
                            ],
                        )
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Map(Arc::new(
                        Map::new(Type::Int, &[("qq".to_string(), Value::Int(22))]).unwrap(),
                    ))]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();

        let key_field = arrow::datatypes::Field::new("__key__", DataType::Utf8, false);
        let value_field = arrow::datatypes::Field::new("__value__", DataType::Int64, false);
        let fields = vec![key_field.clone(), value_field.clone()];
        let struct_field = arrow::datatypes::Field::new(
            "__entries__",
            DataType::Struct(Fields::from(fields)),
            false,
        );
        assert_eq!(
            rb.column(0).data_type(),
            &DataType::Map(Arc::new(struct_field.clone()), false)
        );
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);

        // Test single element map
        let df = Dataframe::from_rows(
            schema.clone(),
            &[Row::new(
                schema.clone(),
                Arc::new(vec![Value::Map(Arc::new(
                    Map::new(Type::Int, &[("aa".to_string(), Value::Int(1))]).unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(
            rb.column(0).data_type(),
            &DataType::Map(Arc::new(struct_field.clone()), false)
        );
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);

        // Test empty map
        let df = Dataframe::from_rows(
            schema.clone(),
            &[Row::new(
                schema.clone(),
                Arc::new(vec![Value::Map(Arc::new(
                    Map::new(Type::Int, &[]).unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(
            rb.column(0).data_type(),
            &DataType::Map(Arc::new(struct_field), false)
        );
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_map_complex_value_type() {
        let schema = Arc::new(
            Schema::new(vec![Field::new(
                "foo".to_string(),
                Type::Map(Box::new(Type::Map(Box::new(Type::List(Box::new(
                    Type::Int,
                )))))), // Map<Map<List<Int>>>
            )])
            .unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[Row::new(
                schema.clone(),
                Arc::new(vec![Value::Map(Arc::new(
                    Map::new(
                        Type::Map(Box::new(Type::List(Box::new(Type::Int)))),
                        &[
                            (
                                "aa".to_string(),
                                Value::Map(Arc::new(
                                    Map::new(
                                        Type::List(Box::new(Type::Int)),
                                        &[(
                                            "bb".to_string(),
                                            Value::List(Arc::new(
                                                List::new(Type::Int, &[Value::Int(1)]).unwrap(),
                                            )),
                                        )],
                                    )
                                    .unwrap(),
                                )),
                            ),
                            (
                                "cc".to_string(),
                                Value::Map(Arc::new(
                                    Map::new(
                                        Type::List(Box::new(Type::Int)),
                                        &[(
                                            "dd".to_string(),
                                            Value::List(Arc::new(
                                                List::new(
                                                    Type::Int,
                                                    &[Value::Int(2), Value::Int(45)],
                                                )
                                                .unwrap(),
                                            )),
                                        )],
                                    )
                                    .unwrap(),
                                )),
                            ),
                        ],
                    )
                    .unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();

        let key_field = arrow::datatypes::Field::new("__key__", DataType::Utf8, false);
        let value_field = arrow::datatypes::Field::new(
            "__value__",
            DataType::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "__entries__",
                    DataType::Struct(Fields::from(vec![
                        arrow::datatypes::Field::new("__key__", DataType::Utf8, false),
                        arrow::datatypes::Field::new(
                            "__value__",
                            DataType::List(Arc::new(arrow::datatypes::Field::new(
                                "__list__",
                                DataType::Int64,
                                false,
                            ))),
                            false,
                        ),
                    ])),
                    false,
                )),
                false,
            ),
            false,
        );
        let fields = vec![key_field.clone(), value_field.clone()];
        let struct_field = arrow::datatypes::Field::new(
            "__entries__",
            DataType::Struct(Fields::from(fields)),
            false,
        );
        assert_eq!(
            rb.column(0).data_type(),
            &DataType::Map(Arc::new(struct_field.clone()), false)
        );
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);

        // Test empty map
        let df = Dataframe::from_rows(
            schema.clone(),
            &[Row::new(
                schema.clone(),
                Arc::new(vec![Value::Map(Arc::new(
                    Map::new(Type::Map(Box::new(Type::List(Box::new(Type::Int)))), &[]).unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(
            rb.column(0).data_type(),
            &DataType::Map(Arc::new(struct_field), false)
        );
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_struct_simple_value_type() {
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", Type::String),
                ],
            )
            .unwrap(),
        ));

        let schema = Arc::new(
            Schema::new(vec![Field::new("foo".to_string(), struct_type.clone())]).unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Struct(Arc::new(
                        Struct::new(vec![
                            ("field1".into(), Value::Int(1)),
                            ("field2".into(), Value::String(Arc::new("foo".into()))),
                        ])
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Struct(Arc::new(
                        Struct::new(vec![
                            ("field1".into(), Value::Int(2)),
                            ("field2".into(), Value::String(Arc::new("bar".into()))),
                        ])
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();

        let fields = vec![
            arrow::datatypes::Field::new("field1", DataType::Int64, false),
            arrow::datatypes::Field::new("field2", DataType::Utf8, false),
        ];
        let struct_field = arrow::datatypes::Field::new(
            "test_struct",
            DataType::Struct(Fields::from(fields)),
            false,
        );
        assert_eq!(rb.column(0).data_type(), struct_field.data_type());
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);

        // Test empty struct

        let df = Dataframe::from_rows(schema.clone(), &[]).unwrap();
        let rb = to_arrow_recordbatch(&df).unwrap();
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);

        // Providing a struct with missing fields should fail

        assert!(Row::new(
            schema.clone(),
            Arc::new(vec![Value::Struct(Arc::new(
                Struct::new(vec![("field1".into(), Value::Int(1),)]).unwrap()
            ))]),
        )
        .is_err());
    }

    #[test]
    fn test_parse_from_utf8() {
        use Value::Int;
        let cases: Vec<(Type, Vec<&str>, Option<Vec<Value>>)> = vec![
            (
                Type::List(Box::new(Type::Int)),
                vec!["[1, 2, 3]", "[]", "[4, 5, 6]"],
                Some(vec![
                    Value::List(Arc::new(
                        List::new(Type::Int, &[Int(1), Int(2), Int(3)]).unwrap(),
                    )),
                    Value::List(Arc::new(List::new(Type::Int, &[]).unwrap())),
                    Value::List(Arc::new(
                        List::new(Type::Int, &[Int(4), Int(5), Int(6)]).unwrap(),
                    )),
                ]),
            ),
            (
                Type::List(Box::new(Type::Int)),
                vec!["[1, 2, 3]", "[]", "[4, 5, 6]", "null"],
                None,
            ),
            (Type::List(Box::new(Type::Int)), vec!["", "null"], None),
            (
                Type::List(Box::new(Type::optional(Type::Int))),
                vec!["[1, 2, 3]", "[]", "[4, 5, 6, null]", "[null]"],
                Some(vec![
                    Value::List(Arc::new(
                        List::new(Type::optional(Type::Int), &[Int(1), Int(2), Int(3)]).unwrap(),
                    )),
                    Value::List(Arc::new(List::new(Type::optional(Type::Int), &[]).unwrap())),
                    Value::List(Arc::new(
                        List::new(
                            Type::optional(Type::Int),
                            &[Int(4), Int(5), Int(6), Value::None],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        List::new(Type::optional(Type::Int), &[Value::None]).unwrap(),
                    )),
                ]),
            ),
            (
                Type::optional(Type::List(Box::new(Type::Int))),
                vec!["[null]"],
                None, // should fail because the list is optional, but the values are not
            ),
            (
                Type::optional(Type::List(Box::new(Type::Int))),
                vec!["[1, 2, 3]", "[]", "[4, 5, 6]", "null"],
                Some(vec![
                    Value::List(Arc::new(
                        List::new(Type::Int, &[Int(1), Int(2), Int(3)]).unwrap(),
                    )),
                    Value::List(Arc::new(List::new(Type::Int, &[]).unwrap())),
                    Value::List(Arc::new(
                        List::new(Type::Int, &[Int(4), Int(5), Int(6)]).unwrap(),
                    )),
                    Value::None,
                ]),
            ),
            (
                Type::List(Box::new(Type::optional(Type::Timestamp))),
                vec![
                    "[\"2021-01-01T00:00:00Z\", \"2021-01-02T00:00:00Z\", null]",
                    "[null, null]",
                ],
                Some(vec![
                    Value::List(Arc::new(
                        List::new(
                            Type::optional(Type::Timestamp),
                            &[
                                Value::Timestamp(
                                    UTCTimestamp::try_from("2021-01-01T00:00:00Z").unwrap(),
                                ),
                                Value::Timestamp(
                                    UTCTimestamp::try_from("2021-01-02T00:00:00Z").unwrap(),
                                ),
                                Value::None,
                            ],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        List::new(Type::optional(Type::Timestamp), &[Value::None, Value::None])
                            .unwrap(),
                    )),
                ]),
            ),
            (
                Type::Timestamp,
                vec![
                    "\"2021-01-01T00:00:00Z\"",
                    "\"2021-01-02T00:00:00Z\"",
                    "null",
                ],
                None, // None because "null" is not a valid timestamp
            ),
            (
                Type::optional(Type::Timestamp),
                vec![
                    "\"2021-01-01T00:00:00Z\"",
                    "\"2021-01-02T00:00:00Z\"",
                    "null",
                ],
                Some(vec![
                    Value::Timestamp(UTCTimestamp::try_from("2021-01-01T00:00:00Z").unwrap()),
                    Value::Timestamp(UTCTimestamp::try_from("2021-01-02T00:00:00Z").unwrap()),
                    Value::None,
                ]),
            ),
            (
                Type::Float,
                vec!["1.0", "2.0", "3.0"],
                Some(vec![
                    Value::Float(1.0),
                    Value::Float(2.0),
                    Value::Float(3.0),
                ]),
            ),
            (
                Type::optional(Type::Float),
                vec!["1.0", "2.0", "3.0", "null"],
                Some(vec![
                    Value::Float(1.0),
                    Value::Float(2.0),
                    Value::Float(3.0),
                    Value::None,
                ]),
            ),
        ];
        for case in cases {
            // construct a DF with a single column of type string
            let df = Dataframe::new(vec![Col::from("foo", Type::String, case.1.clone()).unwrap()])
                .unwrap();
            let rb = to_arrow_recordbatch(&df).unwrap();
            // assert that recordbatch has utf8 type
            assert_eq!(rb.columns().len(), 1);
            assert_eq!(rb.column(0).data_type(), &DataType::Utf8);
            let schema =
                Arc::new(Schema::new(vec![Field::new("foo".to_string(), case.0.clone())]).unwrap());
            let result = from_arrow_recordbatch(schema.clone(), &rb);
            match &case.2 {
                Some(expected) => {
                    assert!(
                        result.is_ok(),
                        "case failed: {:?}, result: {:?}",
                        case,
                        result
                    );
                    let result = result.unwrap();
                    let col = Col::from("foo", case.0.clone(), expected.clone()).unwrap();
                    let expected_df = Dataframe::new(vec![col]).unwrap();
                    assert_eq!(result, expected_df);
                }
                None => {
                    assert!(result.is_err());
                }
            }
        }
    }

    #[test]
    fn test_struct_from_utf8() {
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", Type::String),
                    Field::new("field3", Type::Optional(Box::new(Type::Float))),
                ],
            )
            .unwrap(),
        ));
        // now construct a DF that has a single column of type optional string
        // and fill it with json strings that represent the struct inluding
        // cases where the string is null, it's empty, or it's missing fields
        let df = Dataframe::new(vec![Col::from(
            "foo",
            Type::Optional(Box::new(Type::String)),
            vec![
                Some(""),
                Some("null"),
                None,
                Some(r#"{"field1": 1, "field2": "foo"}"#),
                Some(r#"{"field1": 1, "field2": "bar", "field3": 3.14}"#),
            ],
        )
        .unwrap()])
        .unwrap();
        let rb = to_arrow_recordbatch(&df).unwrap();
        // assert that recordbatch has utf8 type
        assert_eq!(rb.column(0).data_type(), &DataType::Utf8);
        // now try to convert it back to DF in struct format
        // should fail if the struct type isn't made optional
        let schema = Arc::new(
            Schema::new(vec![Field::new("foo".to_string(), struct_type.clone())]).unwrap(),
        );
        from_arrow_recordbatch(schema.clone(), &rb).unwrap_err();
        // but should work if the struct type is optional
        let schema = Arc::new(
            Schema::new(vec![Field::new(
                "foo".to_string(),
                Type::Optional(Box::new(struct_type)),
            )])
            .unwrap(),
        );
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        let expected_df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(schema.clone(), Arc::new(vec![Value::None])).unwrap(),
                Row::new(schema.clone(), Arc::new(vec![Value::None])).unwrap(),
                Row::new(schema.clone(), Arc::new(vec![Value::None])).unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Struct(Arc::new(
                        Struct::new(vec![
                            ("field1".into(), Value::Int(1)),
                            ("field2".into(), Value::String(Arc::new("foo".into()))),
                            ("field3".into(), Value::None),
                        ])
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Struct(Arc::new(
                        Struct::new(vec![
                            ("field1".into(), Value::Int(1)),
                            ("field2".into(), Value::String(Arc::new("bar".into()))),
                            ("field3".into(), Value::Float(3.14)),
                        ])
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(actual_df, expected_df);
    }

    #[test]
    fn test_struct_simple_value_type_with_optional_fields() {
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", Type::String),
                    Field::new("field3", Type::Optional(Box::new(Type::Float))),
                ],
            )
            .unwrap(),
        ));

        let schema = Arc::new(
            Schema::new(vec![Field::new("foo".to_string(), struct_type.clone())]).unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Struct(Arc::new(
                        Struct::new(vec![
                            ("field1".into(), Value::Int(1)),
                            ("field2".into(), Value::String(Arc::new("foo".into()))),
                            ("field3".into(), Value::None),
                        ])
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
                Row::new(
                    schema.clone(),
                    Arc::new(vec![Value::Struct(Arc::new(
                        Struct::new(vec![
                            ("field1".into(), Value::Int(2)),
                            ("field2".into(), Value::String(Arc::new("bar".into()))),
                            ("field3".into(), Value::Float(3.14)),
                        ])
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();

        let fields = vec![
            arrow::datatypes::Field::new("field1", DataType::Int64, false),
            arrow::datatypes::Field::new("field2", DataType::Utf8, false),
            arrow::datatypes::Field::new("field3", DataType::Float64, true),
        ];
        let struct_field = arrow::datatypes::Field::new(
            "test_struct",
            DataType::Struct(Fields::from(fields)),
            false,
        );
        assert_eq!(rb.column(0).data_type(), struct_field.data_type());
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_struct_complex_value_type() {
        // The following test case has a struct with nested structs and list of structs.

        let nested_struct_type = Type::Struct(Box::new(
            StructType::new(
                "nested_struct".into(),
                vec![
                    Field::new("nested_field1", Type::Int),
                    Field::new("nested_field2", Type::String),
                ],
            )
            .unwrap(),
        ));

        let inner_struct_type = Type::Struct(Box::new(
            StructType::new(
                "inner_struct".into(),
                vec![
                    Field::new(
                        "inner_field1",
                        Type::Optional(Box::new(nested_struct_type.clone())),
                    ),
                    Field::new(
                        "inner_field2",
                        Type::List(Box::new(nested_struct_type.clone())),
                    ),
                ],
            )
            .unwrap(),
        ));

        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", inner_struct_type.clone()),
                ],
            )
            .unwrap(),
        ));

        let schema = Arc::new(
            Schema::new(vec![Field::new("foo".to_string(), struct_type.clone())]).unwrap(),
        );
        let df = Dataframe::from_rows(
            schema.clone(),
            &[Row::new(
                schema.clone(),
                Arc::new(vec![Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("field1".into(), Value::Int(1)),
                        (
                            "field2".into(),
                            Value::Struct(Arc::new(
                                Struct::new(vec![
                                    (
                                        "inner_field1".into(),
                                        Value::Struct(Arc::new(
                                            Struct::new(vec![
                                                ("nested_field1".into(), Value::Int(3)),
                                                (
                                                    "nested_field2".into(),
                                                    Value::String(Arc::new("foo".into())),
                                                ),
                                            ])
                                            .unwrap(),
                                        )),
                                    ),
                                    (
                                        "inner_field2".into(),
                                        Value::List(Arc::new(
                                            List::new(
                                                nested_struct_type.clone(),
                                                &[Value::Struct(Arc::new(
                                                    Struct::new(vec![
                                                        ("nested_field1".into(), Value::Int(4)),
                                                        (
                                                            "nested_field2".into(),
                                                            Value::String(Arc::new("bar".into())),
                                                        ),
                                                    ])
                                                    .unwrap(),
                                                ))],
                                            )
                                            .unwrap(),
                                        )),
                                    ),
                                ])
                                .unwrap(),
                            )),
                        ),
                    ])
                    .unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();

        // Construct the expected schema
        let nested_fields_null = vec![
            arrow::datatypes::Field::new("nested_field1", DataType::Int64, true),
            arrow::datatypes::Field::new("nested_field2", DataType::Utf8, true),
        ];
        let nested_fields_non_null = vec![
            arrow::datatypes::Field::new("nested_field1", DataType::Int64, false),
            arrow::datatypes::Field::new("nested_field2", DataType::Utf8, false),
        ];
        let inner_fields = vec![
            arrow::datatypes::Field::new(
                "inner_field1",
                DataType::Struct(Fields::from(nested_fields_null.clone())),
                true,
            ),
            arrow::datatypes::Field::new(
                "inner_field2",
                DataType::List(Arc::new(arrow::datatypes::Field::new(
                    "__list__",
                    DataType::Struct(Fields::from(nested_fields_non_null.clone())),
                    false,
                ))),
                false,
            ),
        ];
        let fields = vec![
            arrow::datatypes::Field::new("field1", DataType::Int64, false),
            arrow::datatypes::Field::new(
                "field2",
                DataType::Struct(Fields::from(inner_fields.clone())),
                false,
            ),
        ];
        let struct_field = arrow::datatypes::Field::new(
            "test_struct",
            DataType::Struct(Fields::from(fields.clone())),
            false,
        );
        assert_eq!(rb.column(0).data_type(), struct_field.data_type());
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);

        // Insert rows with None values
        let df = Dataframe::from_rows(
            schema.clone(),
            &[Row::new(
                schema.clone(),
                Arc::new(vec![Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("field1".into(), Value::Int(1)),
                        (
                            "field2".into(),
                            Value::Struct(Arc::new(
                                Struct::new(vec![
                                    ("inner_field1".into(), Value::None),
                                    (
                                        "inner_field2".into(),
                                        Value::List(Arc::new(
                                            List::new(
                                                nested_struct_type.clone(),
                                                &[Value::Struct(Arc::new(
                                                    Struct::new(vec![
                                                        ("nested_field1".into(), Value::Int(4)),
                                                        (
                                                            "nested_field2".into(),
                                                            Value::String(Arc::new("bar".into())),
                                                        ),
                                                    ])
                                                    .unwrap(),
                                                ))],
                                            )
                                            .unwrap(),
                                        )),
                                    ),
                                ])
                                .unwrap(),
                            )),
                        ),
                    ])
                    .unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(rb.column(0).data_type(), struct_field.data_type());
        let actual_df = from_arrow_recordbatch(schema.clone(), &rb).unwrap();
        assert_eq!(actual_df, df);
    }

    #[test]
    fn test_serialize_deserialize() {
        let primitive_df = primitive_df();
        let primitive_record_batch = to_arrow_recordbatch(&primitive_df).unwrap();

        let primitive_optional_df = primitive_optional_df();
        let primitive_optional_record_batch = to_arrow_recordbatch(&primitive_optional_df).unwrap();

        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Float),
                Field::new("c".to_string(), Type::String),
                Field::new("d".to_string(), Type::Optional(Box::new(Type::Bool))),
            ])
            .unwrap(),
        );
        let df1 = Dataframe::from_rows(schema.clone(), &[]).unwrap();
        let empty_record_batch = to_arrow_recordbatch(&df1).unwrap();

        let test_cases = vec![
            primitive_record_batch,
            primitive_optional_record_batch,
            empty_record_batch,
        ];

        for record_batch in test_cases {
            let serialized = serialize_recordbatch(&record_batch).unwrap();
            let deserialized = deserialize_recordbatch(serialized).unwrap();
            assert_eq!(record_batch, deserialized);
        }
    }

    #[test]
    #[ignore = "not implemented"]
    fn test_non_primitives_with_optional() {
        todo!()
    }

    #[test]
    fn test_optional_list_dtype() {
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", Type::String),
                ],
            )
            .unwrap(),
        ));
        let dtype = Type::Optional(Box::new(Type::List(Box::new(struct_type))));

        // try converting to arrow dtype
        let field = Field::new("complex", dtype);
        let arrow_field = to_arrow_field(&field);
        assert!(arrow_field.is_nullable());
        assert_eq!(arrow_field.name(), "complex");
        assert_eq!(
            arrow_field.data_type(),
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "__list__",
                DataType::Struct(Fields::from(vec![
                    arrow::datatypes::Field::new("field1", DataType::Int64, false),
                    arrow::datatypes::Field::new("field2", DataType::Utf8, false),
                ])),
                false,
            )))
        );
    }

    #[test]
    fn test_optional_list_arrow() {
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", Type::String),
                ],
            )
            .unwrap(),
        ));
        let dtype = Type::Optional(Box::new(Type::List(Box::new(struct_type))));
        let field = Field::new("complex", dtype);

        let df = Dataframe::from_rows(
            Arc::new(Schema::new(vec![field.clone()]).unwrap()),
            &[
                Row::new(
                    Arc::new(Schema::new(vec![field.clone()]).unwrap()),
                    Arc::new(vec![Value::List(Arc::new(
                        List::new(
                            Type::Struct(Box::new(
                                StructType::new(
                                    "test_struct".into(),
                                    vec![
                                        Field::new("field1", Type::Int),
                                        Field::new("field2", Type::String),
                                    ],
                                )
                                .unwrap(),
                            )),
                            &[
                                Value::Struct(Arc::new(
                                    Struct::new(vec![
                                        ("field1".into(), Value::Int(1)),
                                        ("field2".into(), Value::String(Arc::new("foo".into()))),
                                    ])
                                    .unwrap(),
                                )),
                                Value::Struct(Arc::new(
                                    Struct::new(vec![
                                        ("field1".into(), Value::Int(2)),
                                        ("field2".into(), Value::String(Arc::new("bar".into()))),
                                    ])
                                    .unwrap(),
                                )),
                            ],
                        )
                        .unwrap(),
                    ))]),
                )
                .unwrap(),
                Row::new(
                    Arc::new(Schema::new(vec![field.clone()]).unwrap()),
                    Arc::new(vec![Value::None]),
                )
                .unwrap(),
            ],
        )
        .unwrap();

        let rb = to_arrow_recordbatch(&df).unwrap();
        assert_eq!(rb.num_rows(), 2);
        let schema = rb.schema();
        let fields = schema.fields();
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name(), "complex");
        assert_eq!(
            fields[0].data_type(),
            &DataType::List(Arc::new(arrow::datatypes::Field::new(
                "__list__",
                DataType::Struct(Fields::from(vec![
                    arrow::datatypes::Field::new("field1", DataType::Int64, false),
                    arrow::datatypes::Field::new("field2", DataType::Utf8, false),
                ])),
                /*nullable=*/ false,
            )))
        );
        assert!(fields[0].is_nullable());
        // assert single column
        let columns = rb.columns();
        assert_eq!(columns.len(), 1);
        // can be converted into a list array
        let list_array = columns[0].as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_array.len(), 2);
        // has 1 null value
        assert_eq!(list_array.null_count(), 1);
        assert_eq!(list_array.value_offsets(), &[0, 2, 2]);
        let list_values = list_array.values();
        // can be converted into a struct array
        let list_values = list_values.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(list_values.len(), 2);
        // none of them are null and have correct values
        let field1 = list_values
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(field1.len(), 2);
        assert_eq!(field1.value(0), 1);
        assert_eq!(field1.value(1), 2);
        let field2 = list_values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(field2.len(), 2);
        assert_eq!(field2.value(0), "foo");
        assert_eq!(field2.value(1), "bar");
    }

    #[test]
    /// Test conversion from arrow array of type F16, F32, F64 +
    /// relevant int types to Fennel float type
    fn test_from_arrow_to_float_conversion() {
        let float_arrays: Vec<ArrayRef> = vec![
            Arc::new(arrow::array::Float32Array::from(vec![1.1, 1.2, 1.3])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![1.1, 1.2, 1.3])) as ArrayRef,
        ];
        for array in float_arrays.iter() {
            assert_eq!(
                from_arrow_array(&array, &Type::Float, false).unwrap(),
                vec![Value::Float(1.1), Value::Float(1.2), Value::Float(1.3)]
            );
        }
        let int_arrays: Vec<ArrayRef> = vec![
            Arc::new(arrow::array::Int8Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::Int16Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::UInt8Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::UInt16Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::UInt32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(arrow::array::UInt64Array::from(vec![1, 2, 3])) as ArrayRef,
        ];
        for array in int_arrays.iter() {
            assert_eq!(
                from_arrow_array(&array, &Type::Float, false).unwrap(),
                vec![Value::Float(1.0), Value::Float(2.0), Value::Float(3.0)]
            );
        }
    }

    #[test]
    fn test_struct_parse_complex() {
        // checks for two cases: 1) If our struct has element of type Optional[t]
        // and arrow doesn't have that field at all, we can still parse - just as
        // nulls. 2) If arrow struct has additional fields that our type doesn't
        // have, we ignore those fields.
        let fstruct = StructType::new(
            "fennel_struct".into(),
            vec![
                Field::new("field1", Type::Int),
                Field::new("field2", Type::Optional(Box::new(Type::String))),
            ],
        )
        .unwrap();
        let actual_schema = Arc::new(
            Schema::new(vec![Field::new(
                "struct_field",
                Type::Struct(Box::new(fstruct.clone())),
            )])
            .unwrap(),
        );
        let arrow_schema = Arc::new(
            Schema::new(vec![Field::new(
                "struct_field",
                Type::Struct(Box::new(
                    StructType::new(
                        "some_other_name".into(),
                        vec![
                            Field::new("field1", Type::Int),
                            Field::new("field3", Type::Bool),
                        ],
                    )
                    .unwrap(),
                )),
            )])
            .unwrap(),
        );
        let df = Dataframe::from_rows(
            arrow_schema.clone(),
            &[Row::new(
                arrow_schema.clone(),
                Arc::new(vec![Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("field1".into(), Value::Int(1)),
                        ("field3".into(), Value::Bool(true)),
                    ])
                    .unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();
        let rb = to_arrow_recordbatch(&df).unwrap();
        let actual_df = from_arrow_recordbatch(actual_schema.clone(), &rb).unwrap();
        let expected_df = Dataframe::from_rows(
            actual_schema.clone(),
            &[Row::new(
                actual_schema.clone(),
                Arc::new(vec![Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("field1".into(), Value::Int(1)),
                        ("field2".into(), Value::None),
                    ])
                    .unwrap(),
                ))]),
            )
            .unwrap()],
        )
        .unwrap();
        assert_eq!(actual_df, expected_df);
    }

    #[test]
    fn test_can_safely_promote() {
        use crate::types;
        use DataType as DT;
        use Type::*;
        // All Fennel types besides optional
        let all = vec![
            Int,
            Float,
            String,
            Bool,
            Date,
            Timestamp,
            Bytes,
            Decimal(types::DecimalType::new(1).unwrap()),
            List(Box::new(Int)),
            Map(Box::new(Int)),
            Embedding(16),
            Struct(Box::new(StructType::new("test".into(), vec![]).unwrap())),
            OneOf(types::OneOf::new(Int, vec![Value::Int(1), Value::Int(2)]).unwrap()),
            Between(types::Between::new(Int, Value::Int(1), Value::Int(2), false, false).unwrap()),
            Regex(types::CompiledRegex::new(".*".to_string()).unwrap()),
        ];
        let primitives = [
            (DT::Int8, Int),
            (DT::Int16, Int),
            (DT::Int32, Int),
            (DT::Int64, Int),
            (DT::UInt8, Int),
            (DT::UInt16, Int),
            (DT::UInt32, Int),
            (DT::UInt64, Int),
            (DT::Float16, Float),
            (DT::Float32, Float),
            (DT::Float64, Float),
            (DT::Int8, Float),
            (DT::Int16, Float),
            (DT::Int32, Float),
            (DT::Int64, Float),
            (DT::UInt8, Float),
            (DT::UInt16, Float),
            (DT::UInt32, Float),
            (DT::UInt64, Float),
            (DT::Utf8, String),
            (DT::Boolean, Bool),
            (DT::Date32, Date),
            (DT::Date64, Date),
            (DT::Timestamp(TimeUnit::Second, None), Timestamp),
            (DT::Timestamp(TimeUnit::Millisecond, None), Timestamp),
            (DT::Timestamp(TimeUnit::Microsecond, None), Timestamp),
            (DT::Timestamp(TimeUnit::Nanosecond, None), Timestamp),
            (DT::Binary, Bytes),
            (
                DT::Decimal128(1, 1),
                Decimal(types::DecimalType::new(1).unwrap()),
            ),
        ];
        let mut cases = vec![];
        // add optional valid cases
        for (at, ft) in primitives.iter() {
            cases.push((at.clone(), false, ft.clone(), true));
            cases.push((at.clone(), true, ft.clone(), false));
            cases.push((at.clone(), false, Optional(Box::new(ft.clone())), true));
            cases.push((at.clone(), true, Optional(Box::new(ft.clone())), true));
        }
        // add list valid cases
        for (at, ft) in primitives.iter() {
            let afield = arrow::datatypes::Field::new("__list__".to_string(), at.clone(), false);
            let at = DT::List(Arc::new(afield.clone()));
            let ft = List(Box::new(ft.clone()));
            cases.push((at.clone(), false, ft.clone(), true));
            cases.push((at.clone(), true, ft.clone(), false));
            cases.push((at.clone(), false, Optional(Box::new(ft.clone())), true));
            cases.push((at.clone(), true, Optional(Box::new(ft.clone())), true));
        }
        // add map valid cases
        for (at, ft) in primitives.iter() {
            let vfield = arrow::datatypes::Field::new("__value__".to_string(), at.clone(), false);
            let kfield = arrow::datatypes::Field::new("__key__".to_string(), DataType::Utf8, false);
            let structtype = DT::Struct(Fields::from(vec![kfield.clone(), vfield.clone()]));
            let structfield =
                arrow::datatypes::Field::new("__map__".to_string(), structtype, false);
            let at = DT::Map(Arc::new(structfield.clone()), false);
            let ft = Map(Box::new(ft.clone()));
            cases.push((at.clone(), false, ft.clone(), true));
            cases.push((at.clone(), true, ft.clone(), false));
            cases.push((at.clone(), false, Optional(Box::new(ft.clone())), true));
            cases.push((at.clone(), true, Optional(Box::new(ft.clone())), true));
            // but if the key is not a string, it should fail
            let kfield =
                arrow::datatypes::Field::new("__key__".to_string(), DataType::Int64, false);
            let structtype = DT::Struct(Fields::from(vec![kfield.clone(), vfield.clone()]));
            let structfield =
                arrow::datatypes::Field::new("__map__".to_string(), structtype, false);
            let at = DT::Map(Arc::new(structfield.clone()), false);
            cases.push((at.clone(), false, ft.clone(), false));
        }
        // add all valid cases with arrow type Null
        for ft in all.iter() {
            cases.push((DT::Null, false, Optional(Box::new(ft.clone())), true));
            cases.push((DT::Null, true, Optional(Box::new(ft.clone())), true));
            cases.push((DT::Null, true, ft.clone(), false));
            cases.push((DT::Null, false, ft.clone(), false));
        }
        for (data_type, nullable, dtype, expected) in cases.iter() {
            assert_eq!(
                can_safely_promote(data_type, *nullable, dtype),
                *expected,
                "data_type: {:?}, nullable: {}, dtype: {:?}",
                data_type,
                nullable,
                dtype
            );
        }
    }
}
