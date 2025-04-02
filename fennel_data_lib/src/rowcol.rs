use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::iter::zip;
use std::ops::Deref;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use apache_avro::schema::{NamesRef, Schema as AvroSchema};
use apache_avro::types::Value as AvroValue;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use derivative::Derivative;
use integer_encoding::VarIntWriter;
use itertools::Itertools;
use protofish::context::Context as ProtoContext;
use protofish::decode::MessageValue;
use protofish::decode::Value as ProtobufValue;
use serde_json::Value as JsonValue;
use xxhash_rust::xxh3::Xxh3;

use super::schema::{Field, Schema};
use super::types::Type;
use super::value::Value;

pub type Series = Arc<Vec<Value>>;
pub type TSSeries = Arc<Vec<crate::value::UTCTimestamp>>;
pub type BoolSeries = Arc<Vec<bool>>;

// we store codec in 3 bits so this list is limited to 8 codecs

#[derive(Debug)]
pub enum RowCodec {
    V1_13bithash = 0,
}

/// Row represents an ordered and typed collection of values
/// A valid row has a schema and a valid value for each field in the schema.
/// Since the Row can only be created using new, which does the validation,
/// we can assume that the row is always valid.
/// Row is NOT super cheap to clone because it clones each value.
#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub struct Row {
    schema: Arc<Schema>,
    values: Arc<Vec<Value>>,
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        if self.schema() != other.schema() {
            return false;
        }
        self.schema()
            .sorted_fields()
            .zip(other.schema().sorted_fields())
            .all(|((idx1, _), (idx2, _))| self.values[idx1] == other.values[idx2])
    }
}

impl Row {
    pub fn empty() -> Self {
        Row {
            schema: Arc::new(Schema::new(vec![]).unwrap()),
            values: Arc::new(Vec::new()),
        }
    }

    pub fn new(schema: Arc<Schema>, values: Arc<Vec<Value>>) -> Result<Self> {
        if schema.len() != values.len() {
            return Err(anyhow!(
                "unable to create row: schema has {} fields, but {} values",
                schema.len(),
                values.len()
            ));
        }
        for (f, v) in zip(schema.fields(), values.iter()) {
            // NOTE: The schema may be not exactly equal to values. We guarantee to be matching
            if !v.matches(f.dtype()) {
                return Err(anyhow!(
                    "value {:?} does not match type {:?} for field {:?}",
                    v,
                    f.dtype(),
                    f.name(),
                ));
            }
        }
        Ok(Row { schema, values })
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns a new row with the given field set to the given value.
    /// Fails if the field does not exist in the schema or the value does not
    /// match the field type.
    pub fn set(self, name: impl AsRef<str>, value: impl Into<Value>) -> Result<Self> {
        let name = name.as_ref();
        let field = self
            .schema
            .find(name)
            .ok_or_else(|| anyhow!("unable to set field {:?}: field not found in schema", name,))?;
        let value: Value = value.into();
        if !value.matches(field.dtype()) {
            return Err(anyhow!(
                "unable to set field {:?}: value does not match type {:?}",
                name,
                field.dtype()
            ));
        }
        // can unwrap because we know the field exists
        let idx = self.schema.index_of(name).unwrap();
        let mut values = self.values;
        Arc::make_mut(&mut values)[idx] = value;
        Self::new(self.schema.clone(), values)
    }

    pub fn project(&self, cols: &[impl AsRef<str>]) -> Result<Self> {
        let schema = Arc::new(self.schema.project(cols)?);
        let values = cols
            .iter()
            .map(|c| {
                self.get(c.as_ref())
                    .ok_or(anyhow!("unable to find column {:?}", c.as_ref()))
            })
            .map_ok(|c| c.clone())
            .collect::<Result<Vec<_>>>()?;
        Self::new(schema, Arc::new(values))
    }

    pub fn select(&self, cols: &[impl AsRef<str>]) -> Result<Vec<&Value>> {
        cols.iter()
            .map(|c| {
                self.get(c.as_ref())
                    .ok_or(anyhow!("unable to find column {:?}", c.as_ref()))
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn strip(&self, cols: &[&str]) -> Result<Self> {
        let schema = Arc::new(self.schema.strip(cols)?);
        let mut values = Vec::with_capacity(schema.len());
        for (i, v) in self.values.iter().enumerate() {
            if !cols.contains(&self.schema.fields()[i].name()) {
                values.push(v.clone());
            }
        }
        Self::new(schema, Arc::new(values))
    }

    /// Get the value for the given field name if it exists, else None
    pub fn get(&self, field_name: &str) -> Option<&Value> {
        let idx = self.schema.index_of(field_name)?;
        self.values.get(idx)
    }

    /// Get the value at the given index if it exists, else None
    pub fn at(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn values_clone(&self) -> Arc<Vec<Value>> {
        self.values.clone()
    }

    pub fn from_json(schema: Arc<Schema>, json: &str) -> Result<Self> {
        Self::from_json_parsed(schema, &serde_json::from_str(json)?)
    }

    // Schema can be a subset of the data in JsonValue.
    pub fn from_json_parsed(schema: Arc<Schema>, v: &JsonValue) -> Result<Self> {
        if !v.is_object() {
            return Err(anyhow!("unable to create row: json is not an object"));
        }
        let v = v.as_object().unwrap();
        let mut values = Vec::with_capacity(schema.len());
        for f in schema.fields() {
            let v = match v.get(f.name()) {
                Some(v) => Value::from_json_parsed(f.dtype(), v).with_context(|| {
                    format!(
                        "Failed to create row: unable to parse json value for field {}",
                        f.name()
                    )
                })?,
                None if f.is_nullable() => Value::None,
                None => {
                    return Err(anyhow!(
                        "unable to create row: missing field {} in json",
                        f.name()
                    ));
                }
            };
            values.push(v);
        }
        Ok(Self::new(schema, Arc::new(values))?)
    }

    pub fn from_proto_parsed(
        _dsname: &str,
        row_schema: Arc<Schema>,
        proto_value: MessageValue,
        proto_context: &ProtoContext,
    ) -> Result<Option<Self>> {
        // Map the field number to a list of field values. A single field number can have multiple
        // values when the field type is a list of complex types.
        let mut proto_fields_hash: HashMap<u64, Vec<ProtobufValue>> =
            HashMap::with_capacity(proto_value.fields.len());
        for field in proto_value.fields {
            proto_fields_hash
                .entry(field.number)
                .or_insert(Vec::new())
                .push(field.value);
        }
        let msg_info = proto_context.resolve_message(proto_value.msg_ref);
        let mut values = Vec::with_capacity(row_schema.fields().len());
        for field in row_schema.fields() {
            let field_name = field.name();
            let field_type = field.dtype();
            let msg_field = msg_info.get_field_by_name(field_name).ok_or_else(|| {
                anyhow!(
                    "Protobuf message doesn't contain field corresponding to name {:?}",
                    field_name
                )
            })?;
            let proto_value = proto_fields_hash.remove(&msg_field.number);
            let val = Value::from_proto_parsed(
                field_name,
                field_type,
                proto_context,
                proto_value,
                &msg_field,
            )
            .map_err(|e| {
                anyhow!(
                    "unable to parse proto value for field: {} due to error: {:?}",
                    msg_field.name,
                    e
                )
            })?;
            values.push(val);
        }
        Ok(Some(Self::new(row_schema, Arc::new(values))?))
    }

    pub fn from_avro_parsed<T>(
        row_schema: Arc<Schema>,
        avro_value: AvroValue,
        avro_schema: T,
        schema_refs: Option<&NamesRef<'_>>,
    ) -> Result<Option<Self>>
    where
        T: Deref<Target = AvroSchema>,
    {
        match (avro_value, avro_schema.deref()) {
            (avro_value, AvroSchema::Ref { name }) => {
                let schema = schema_refs
                    .ok_or(anyhow!(
                        "schema contains references, but no schema refs provided"
                    ))?
                    .get(name)
                    .ok_or(anyhow!("schema ref {} not found", name))?;
                Self::from_avro_parsed(row_schema, avro_value, *schema, schema_refs)
            }
            (AvroValue::Record(fields), AvroSchema::Record(record_schema)) => {
                let fields_hash: HashMap<&str, &Type> = row_schema
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f.dtype()))
                    .collect();

                let mut hash: HashMap<String, Value> = HashMap::new();
                for (name, av) in fields.into_iter() {
                    if fields_hash.contains_key(name.as_str()) {
                        let dtype = fields_hash.get(name.as_str()).unwrap();
                        let field_schema = &record_schema.fields[*record_schema.lookup.get(&name).unwrap()].schema;
                        let v = Value::from_avro_parsed(*dtype, av, field_schema, schema_refs)
                            .map_err(|e| anyhow!("unable to parse avro value for field {}: {:?}", name, e))?;
                        hash.insert(name, v);
                    }
                }

                let mut values = Vec::with_capacity(row_schema.len());
                for f in row_schema.fields() {
                    let v = match hash.remove(f.name()) {
                        Some(v) => v,
                        None if f.is_nullable() => Value::None,
                        None => {
                            bail!(
                                "unable to create row: missing field {} in avro",
                                f.name()
                            );
                        }
                    };
                    values.push(v);
                }
                Ok(Some(Self::new(row_schema, Arc::new(values))?))
            }
            // For optional value, the avro representation is Union.
            (AvroValue::Union(position, v), AvroSchema::Union(us)) => {
                let variant_schema = &us.variants()[position as usize];
                Self::from_avro_parsed(row_schema, *v, variant_schema, schema_refs)
            }
            (AvroValue::Null, AvroSchema::Null) => Ok(None),
            (value, schema) => bail!(
                "unable to create row: avro value not a record or does not match schema: value: {:?}, schema: {:?}",
                value, schema
            ),
        }
    }

    /// to_json serializes the row to a JSON value
    pub fn to_json(&self) -> Result<JsonValue> {
        let mut json_value = serde_json::Map::with_capacity(self.len());
        for (f, val) in zip(self.schema.fields(), self.values.iter()) {
            json_value.insert(f.name().to_string(), val.to_json());
        }
        Ok(JsonValue::Object(json_value))
    }

    /// to_debezium_json serializes the row to a debezium compatible JSON value
    pub fn to_debezium_json(&self) -> Result<JsonValue> {
        let mut json_value = serde_json::Map::with_capacity(self.len());
        for (f, val) in zip(self.schema.fields(), self.values.iter()) {
            json_value.insert(f.name().to_string(), val.to_debezium_json()?);
        }
        Ok(JsonValue::Object(json_value))
    }

    pub fn to_bytes(rows: &[impl AsRef<Self>]) -> Result<Vec<Bytes>> {
        let mut ret = Vec::with_capacity(rows.len());
        let mut buf = BytesMut::new();
        for r in rows {
            let mut writer = buf.writer();
            r.as_ref().encode(&mut writer)?;
            buf = writer.into_inner();
            ret.push(buf.split().freeze());
        }
        Ok(ret)
    }

    pub fn from_bytes(schema: Arc<Schema>, bytes: impl AsRef<[u8]>) -> Result<Self> {
        let mut reader = bytes.as_ref();
        Self::decode(schema, &mut reader)
    }

    /// Write the row to a writer in efficient binary format.
    /// Encoding adds a 2 byte prefix to the encoded row - the first
    /// 3 bits are the codec version and the remaining 13 bits are the
    /// schema hash. This allows the schema to be validated on decode (note:
    /// future codecs may choose to have different # of bits for different
    /// purposes).
    /// Encoding is independent of the order of the fields in the schema.
    pub fn encode(&self, mut writer: &mut dyn Write) -> Result<()> {
        let h = self.schema.hash();
        let lower = (h & 0x1fff) as u16; // lower 13 bits
        let header = lower << 3 | (RowCodec::V1_13bithash as u16);
        writer.write_u16::<BigEndian>(header)?;

        // then write the values
        let values = self.values();
        let values = self.schema.sorted_fields().map(|(idx, _)| &values[idx]);
        let dtypes = self.schema.sorted_fields().map(|(_, f)| f.dtype());
        Value::encode_many_iter(&mut writer, self.schema.len(), values, dtypes)?;
        Ok(())
    }

    /// Read an encoded row from the reader as per the given schema.
    pub fn decode(schema: Arc<Schema>, reader: &mut impl Read) -> Result<Self> {
        let header: u16 = reader.read_u16::<BigEndian>()?;
        if header & 0b111 != (RowCodec::V1_13bithash as u16) {
            panic!(
                "unable to decode row: codec mismatch: expected {:?}, got {}",
                RowCodec::V1_13bithash,
                header & 0b111
            );
        }
        let schema_hash: u16 = header >> 3;
        let expected = (schema.hash() & 0x1fff) as u16;
        if schema_hash != expected {
            panic!(
                "unable to decode row: schema hash mismatch: expected {}, got {}",
                expected, schema_hash
            );
        }
        let indices = schema.sorted_fields().map(|(idx, _)| idx);
        let mut dtypes = schema.sorted_fields().map(|(_, f)| f.dtype());
        let values = Value::decode_many(reader, &mut dtypes)?;
        // now reorder the values to match the input schema
        let mut ret = vec![Value::None; schema.len()];
        for (f, v) in zip(indices, values.into_iter()) {
            ret[f] = v;
        }
        Self::new(schema.clone(), Arc::new(ret))
    }

    /// Hash the row using xxhash
    /// This hash is guaranteed to be stable across versions of the library
    /// and can be used in persistent storage. This hash is independent of the
    /// order of the fields in the schema and depends on names, types, & values.
    pub fn hash(&self) -> u64 {
        let mut buf = Vec::new();
        for (_idx, f) in self.schema.sorted_fields() {
            buf.write_varint(f.name().len() as u64).unwrap();
            buf.write_all(f.name().as_bytes()).unwrap();
            let dtype = f.dtype();
            dtype.encode(&mut buf).unwrap();
        }
        let values = self
            .schema
            .sorted_fields()
            .map(|(idx, _)| self.values.get(idx).unwrap());
        let dtypes = self.schema.sorted_fields().map(|(_, f)| f.dtype());
        Value::encode_many_iter(&mut buf, self.schema.len(), values, dtypes).unwrap();
        xxhash_rust::xxh3::xxh3_64(&buf)
    }

    /// Returns an ordered value hash (not depending on type and name) of each row in the dataframe.
    pub fn value_hash(&self) -> anyhow::Result<u64> {
        let mut hasher = Xxh3::new();
        let mut buf = Vec::new();
        let dtypes = zip(self.values.as_ref(), self.schema.fields().iter()).map(|(v, f)| match v {
            // Only encode as type None for None value
            Value::None => f.dtype(),
            _ => f.dtype().inner(),
        });
        Value::encode_many_iter(&mut buf, self.schema.len(), self.values.iter(), dtypes)
            .context("failed to encode row values")?;
        hasher.update(&buf);
        anyhow::Ok(hasher.digest())
    }

    pub fn random(schema: impl AsRef<Schema>) -> Self {
        let schema = schema.as_ref();
        let values = schema
            .fields()
            .iter()
            .map(|f| Value::random(f.dtype()))
            .collect();
        Self::new(Arc::new(schema.clone()), Arc::new(values)).unwrap()
    }
}

impl AsRef<Row> for Row {
    fn as_ref(&self) -> &Row {
        self
    }
}

impl Hash for Row {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash());
    }
}

impl Eq for Row {}

/// Col represents a column of values with a name and type.
/// Values can be appended to the column, but the type can not be changed.
/// Col is NOT super cheap to clone because it clones each value.
#[derive(Debug, Clone, PartialEq)]
pub struct Col {
    field: Arc<Field>,
    values: Arc<Vec<Value>>,
}

impl Col {
    /// Create a new column with the given name and type.
    pub fn new(field: Arc<Field>, values: Arc<Vec<Value>>) -> Result<Self> {
        for v in values.iter() {
            if !v.matches(field.dtype()) {
                return Err(anyhow!(
                    "can not create column `{}`: value {:?} does not match type {:?}",
                    field.name(),
                    v,
                    field.dtype()
                ));
            }
        }
        Ok(Col { field, values })
    }

    pub fn name(&self) -> &str {
        &self.field.name()
    }

    pub fn dtype(&self) -> &Type {
        &self.field.dtype()
    }

    pub fn is_nullable(&self) -> bool {
        self.field.is_nullable()
    }

    pub fn get_null_index(&self) -> Vec<usize> {
        self.values
            .iter()
            .enumerate()
            .filter(|&(_, val)| val.eq(&Value::None))
            .map(|(i, _)| i)
            .collect()
    }

    pub fn values(&self) -> &[Value] {
        self.values.as_slice()
    }

    // Return a reference to the the value of the column
    // Useful to copy column around in a non zero copy way
    pub fn values_clone(&self) -> Arc<Vec<Value>> {
        self.values.clone()
    }
    pub fn field(&self) -> &Field {
        self.field.as_ref()
    }

    /// Return a new column with the same values but a new name
    pub fn rename(&self, name: String) -> Result<Self> {
        let field = Field::new(name, self.field.dtype().clone());
        Self::new(Arc::new(field), self.values.clone())
    }

    /// Append a value to the column.
    /// The value must match the column's type, else an error is returned.
    pub fn push(&mut self, v: Value) -> Result<()> {
        if !v.matches(self.dtype()) {
            return Err(anyhow!(
                "can not append to column: value {:?} does not match type {:?}",
                v,
                self.dtype()
            ));
        }
        Arc::make_mut(&mut self.values).push(v);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Get the value at the given index if it exists, else None
    pub fn index(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// Get the number of null values in the column - returns None if the column is not nullable
    /// and Some(k) if the column is nullable and has k null values.
    pub fn null_count(&self) -> Option<usize> {
        match self.dtype().is_nullable() {
            true => Some(
                self.values
                    .iter()
                    .filter(|v| matches!(v, Value::None))
                    .count(),
            ),
            false => None,
        }
    }

    /// Extend the column with the values from another column.
    /// The other column must have the same type, else an error is returned.
    /// If multiple structs own an Arc to the same data, this will clone the data.
    pub fn extend(&mut self, other: &Self) -> Result<()> {
        if self.dtype() != other.dtype() {
            return Err(anyhow!(
                "can not extend column: types do not match: {:?} != {:?}",
                self.dtype(),
                other.dtype()
            ));
        }
        Arc::make_mut(&mut self.values).extend_from_slice(other.values());
        Ok(())
    }

    /// Create a new column with the given name and a vector of types that can
    /// be converted to Value. This is useful for testing.
    pub fn from(
        name: &str,
        dtype: Type,
        values: impl IntoIterator<Item = impl Into<Value>>,
    ) -> Result<Self> {
        let field = Arc::new(Field::new(name.to_string(), dtype.clone()));
        let values: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
        // if dtype is Timestamp, try to convert each value to a Timestamp
        let values = match dtype {
            Type::Timestamp => {
                let ret: Result<Vec<_>> = values
                    .into_iter()
                    .map(|v| v.try_into())
                    .map_ok(|ts| Value::Timestamp(ts))
                    .collect();
                ret?
            }
            Type::Optional(t) if t.as_ref() == &Type::Timestamp => {
                let ret: Result<Vec<_>> = values
                    .into_iter()
                    .map(|v| match v {
                        Value::None => Ok(Value::None),
                        _ => v.try_into().map(Value::Timestamp),
                    })
                    .collect();
                ret?
            }
            _ => values,
        };
        Self::new(field, Arc::new(values))
    }

    pub fn from_json(field: Arc<Field>, json: &str) -> Result<Self> {
        Self::from_json_parsed(field, &serde_json::from_str(json)?)
    }

    // v is a JSON array of values.
    pub fn from_json_parsed(field: Arc<Field>, v: &JsonValue) -> Result<Self> {
        if !v.is_array() {
            return Err(anyhow!("unable to create column: json is not an array"));
        }
        let v = v.as_array().unwrap();
        let values = v
            .iter()
            .map(|v| {
                Value::from_json_parsed(field.dtype(), v).map_err(|e| {
                    anyhow!(
                        "error parsing json value for field `{:?}`: {}",
                        field.name(),
                        e.to_string()
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(field, Arc::new(values))?)
    }

    /// to_json serializes a `Col` into a JSON value
    ///
    /// The format of the JSON value is a list of values - [val1,val2,val3.....]
    pub fn to_json(&self) -> Result<JsonValue> {
        let mut values = Vec::with_capacity(self.len());
        for v in self.values() {
            values.push(v.to_json());
        }
        Ok(JsonValue::Array(values))
    }

    /// Write the column to a writer in an efficient binary format
    /// This doesn't write the column name/type and so the reader
    /// must know the schema of the column.
    pub fn encode(&self, mut writer: &mut dyn Write) -> Result<()> {
        let dtypes = vec![self.dtype(); self.len()];
        let values = self.values.iter().map(|v| v).collect::<Vec<_>>();
        Value::encode_many(&mut writer, &values, &dtypes)?;
        Ok(())
    }

    pub fn decode(
        name: impl Into<String>,
        dtype: impl Into<Type>,
        reader: &mut impl Read,
    ) -> Result<Self> {
        let name: String = name.into();
        let dtype = dtype.into();
        let mut dtype_iter = std::iter::repeat(&dtype);
        let values = Value::decode_many(reader, &mut dtype_iter)?;
        Self::from(name.as_str(), dtype, values)
    }

    pub fn random(dtype: impl AsRef<Type>, len: usize) -> Self {
        let dtype = dtype.as_ref();
        let values = (0..len).map(|_| Value::random(dtype)).collect();
        Self::new(
            Arc::new(Field::new("foo".to_string(), dtype.clone())),
            Arc::new(values),
        )
        .unwrap()
    }
}

// Convert a column of type Timestamp to a TSSeries
impl TryInto<TSSeries> for Col {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<TSSeries> {
        match self.dtype() {
            Type::Timestamp => {
                let values = self
                    .values
                    .iter()
                    .map(|v| v.clone().try_into())
                    .collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(values))
            }
            _ => Err(anyhow!(
                "can not convert column to series: type {:?} is not Timestamp",
                self.dtype()
            )),
        }
    }
}

// Convert a column of type Bool to a BoolSeries.
impl TryInto<BoolSeries> for Col {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<BoolSeries> {
        match self.dtype() {
            Type::Bool => {
                let values = self
                    .values
                    .iter()
                    .map(|v| v.as_bool())
                    .collect::<Result<Vec<bool>>>()?;
                Ok(Arc::new(values))
            }
            _ => Err(anyhow!(
                "can not convert column to series: type {:?} is not Bool",
                self.dtype()
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use apache_avro::schema::Schema as AvroSchema;
    use apache_avro::types::Record as AvroRecord;
    use apache_avro::Reader as AvroReader;
    use apache_avro::Writer as AvroWriter;

    use crate::value::{List, Map, UTCTimestamp};

    use super::*;

    #[test]
    fn test_row_basic() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ])
            .unwrap(),
        );
        let r1 = Row::new(
            schema.clone(),
            Arc::new(vec![Value::Int(1), Value::Bool(false)]),
        )
        .unwrap();
        assert_eq!(2, r1.len());
        assert_eq!(Some(&Value::Int(1)), r1.get("a"));
        assert_eq!(Some(&Value::Bool(false)), r1.get("b"));
        // non-existent field returns None
        assert_eq!(None, r1.get("c"));
        assert_eq!(vec![Value::Int(1), Value::Bool(false)], r1.values());
        assert_eq!(&schema, r1.schema());

        let mut buf = Vec::new();
        r1.encode(&mut buf).unwrap();
        let r2 = Row::decode(schema, &mut buf.as_slice()).unwrap();
        assert_eq!(r1.clone(), r2.clone());

        // decoding works with reordered fields too
        let schema2 = Arc::new(
            Schema::new(vec![
                Field::new("b".to_string(), Type::Bool),
                Field::new("a".to_string(), Type::Int),
            ])
            .unwrap(),
        );
        let r3 = Row::decode(schema2.clone(), &mut buf.as_slice()).unwrap();
        // decoded row has the same schema as the input schema
        assert_eq!(r3.schema().clone(), schema2.clone());
        assert_eq!(r1.len(), r3.len());
        for f in schema2.fields() {
            assert_eq!(r1.get(f.name()), r3.get(f.name()), "field: {}", f.name());
        }

        // and decoding does not work if the schema is wrong - types do not match
        let schema3 = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Bool),
                Field::new("b".to_string(), Type::Int),
            ])
            .unwrap(),
        );
        Row::decode(schema3, &mut buf.as_slice()).unwrap_err();

        // ..or the names do not match
        let schema4 = Arc::new(
            Schema::new(vec![
                Field::new("x".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ])
            .unwrap(),
        );
        Row::decode(schema4, &mut buf.as_slice()).unwrap_err();

        // ..or the number of fields do not match (even if it's a subset)
        let schema5 = Arc::new(Schema::new(vec![Field::new("a".to_string(), Type::Int)]).unwrap());
        Row::decode(schema5, &mut buf.as_slice()).unwrap_err();
    }

    #[test]
    fn test_row_new_invalid() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ])
            .unwrap(),
        );
        // wrong number of values
        Row::new(schema.clone(), Arc::new(vec![Value::Int(1)])).unwrap_err();
        // wrong type
        Row::new(schema.clone(), Arc::new(vec![Value::Int(1), Value::Int(2)])).unwrap_err();
    }

    #[test]
    fn test_col_basic() {
        let mut c1 = Col::new(
            Arc::new(Field::new("a".to_string(), Type::Int)),
            Arc::new(vec![Value::Int(1), Value::Int(2)]),
        )
        .unwrap();
        assert_eq!("a", c1.name());
        assert_eq!(&Type::Int, c1.dtype());
        assert_eq!(false, c1.is_nullable());
        assert_eq!(2, c1.len());
        assert_eq!(&[Value::Int(1), Value::Int(2)], c1.values());
        assert_eq!(&Field::new("a".to_string(), Type::Int), c1.field());
        assert_eq!(Some(&Value::Int(1)), c1.index(0));
        assert_eq!(Some(&Value::Int(2)), c1.index(1));
        assert_eq!(None, c1.index(2));
        // can not append a value of the wrong type
        c1.push(Value::Float(2.0)).unwrap_err();
        c1.push(Value::List(Arc::new(
            List::new(Type::Int, &[Value::Int(2)]).unwrap(),
        )))
        .unwrap_err();
        // can append a value of the correct type
        c1.push(Value::Int(3)).unwrap();
        assert_eq!("a", c1.name());
        assert_eq!(&Type::Int, c1.dtype());
        assert_eq!(false, c1.is_nullable());
        assert_eq!(3, c1.len());
        assert_eq!(&[Value::Int(1), Value::Int(2), Value::Int(3)], c1.values());
        assert_eq!(&Field::new("a".to_string(), Type::Int), c1.field());
        assert_eq!(Some(&Value::Int(1)), c1.index(0));
        assert_eq!(Some(&Value::Int(2)), c1.index(1));
        assert_eq!(Some(&Value::Int(3)), c1.index(2));

        // can rename a column
        let c2 = c1.rename("b".to_string()).unwrap();
        assert_eq!(&Field::new("a".to_string(), Type::Int), c1.field());
        assert_eq!(&Field::new("b".to_string(), Type::Int), c2.field());
    }

    #[test]
    fn test_col_extend() {
        let mut c1 = Col::new(
            Arc::new(Field::new("a".to_string(), Type::Int)),
            Arc::new(vec![Value::Int(1), Value::Int(2)]),
        )
        .unwrap();
        assert_eq!(2, c1.len());
        // can not extend a column with a value of the wrong type
        c1.extend(
            &Col::new(
                Arc::new(Field::new("a".to_string(), Type::Float)),
                Arc::new(vec![Value::Float(2.0)]),
            )
            .unwrap(),
        )
        .unwrap_err();

        // but can extend a column with a value of the correct type (even if field name is different)
        c1.extend(
            &Col::new(
                Arc::new(Field::new("b".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(3), Value::Int(4)]),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(4, c1.len());
        assert_eq!(
            &[Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)],
            c1.values()
        );
    }

    #[test]
    fn test_col_new_invalid() {
        // wrong type
        Col::new(
            Arc::new(Field::new("a".to_string(), Type::Int)),
            Arc::new(vec![Value::Float(1.0)]),
        )
        .unwrap_err();
        // wrong type
        Col::new(
            Arc::new(Field::new("a".to_string(), Type::Int)),
            Arc::new(vec![Value::List(Arc::new(
                List::new(Type::Int, &[Value::Int(2)]).unwrap(),
            ))]),
        )
        .unwrap_err();
    }

    #[test]
    fn test_col_to_json() {
        {
            let c = Col::new(
                Arc::new(Field::new("foo".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(2), Value::Int(3), Value::Int(4)]),
            )
            .unwrap();
            assert_eq!(c.to_json().unwrap().to_string(), "[2,3,4]");
        }
        {
            let c = Col::new(
                Arc::new(Field::new(
                    "foo".to_string(),
                    Type::List(Box::new(Type::Float)),
                )),
                Arc::new(vec![
                    Value::List(Arc::new(
                        List::new(Type::Float, &[Value::Float(1.0)]).unwrap(),
                    )),
                    Value::List(Arc::new(
                        List::new(Type::Float, &[Value::Float(2.0), Value::Float(3.0)]).unwrap(),
                    )),
                ]),
            )
            .unwrap();
            assert_eq!(c.to_json().unwrap().to_string(), "[[1.0],[2.0,3.0]]");
        }
        {
            let c = Col::new(
                Arc::new(Field::new(
                    "foo".to_string(),
                    Type::Map(Box::new(Type::Float)),
                )),
                Arc::new(vec![
                    Value::Map(Arc::new(
                        Map::new(
                            Type::Float,
                            &[
                                ("a".to_string(), Value::Float(1.0)),
                                ("b".to_string(), Value::Float(10.0)),
                            ],
                        )
                        .unwrap(),
                    )),
                    Value::Map(Arc::new(
                        Map::new(
                            Type::Float,
                            &[
                                ("aa".to_string(), Value::Float(2.0)),
                                ("bb".to_string(), Value::Float(3.0)),
                            ],
                        )
                        .unwrap(),
                    )),
                ]),
            )
            .unwrap();
            assert_eq!(
                c.to_json().unwrap().to_string(),
                "[{\"a\":1.0,\"b\":10.0},{\"aa\":2.0,\"bb\":3.0}]"
            );
        }
    }

    fn create_avro_record(
        schema: &AvroSchema,
        a: i32,
        b: bool,
        c: Option<String>,
        t: i64,
    ) -> AvroValue {
        let mut writer = AvroWriter::new(&schema, Vec::new());
        let mut record = AvroRecord::new(&schema).unwrap();
        record.put("t", t);
        record.put("b", b);
        record.put("c", c);
        record.put("a", a);
        writer.append(record).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = AvroReader::with_schema(&schema, &encoded[..]).unwrap();
        reader.into_iter().next().unwrap().unwrap()
    }

    #[test]
    fn test_from_avro_parsed() {
        let raw_avro_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                { "name": "a", "type": "int" },
                { "name": "b", "type": "boolean" },
                { "name": "c", "type": ["null", "string"] },
                { "name": "t", "type": { "type": "long", "logicalType": "timestamp-micros" } }
            ]
        }
        "#;
        let avro_schema = AvroSchema::parse_str(raw_avro_schema).unwrap();
        let row_schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
                Field::new("c".to_string(), Type::String),
                Field::new("t".to_string(), Type::Timestamp),
            ])
            .unwrap(),
        );
        let v = create_avro_record(
            &avro_schema,
            1,
            true,
            Some("foo".to_owned()),
            1577836800000000,
        );
        let expected = Row::new(
            row_schema.clone(),
            Arc::new(vec![
                Value::Int(1),
                Value::Bool(true),
                Value::String(Arc::new(String::from("foo"))),
                // Wednesday, January 1, 2020 12:00:00 AM in GMT
                Value::Timestamp(UTCTimestamp::from(1577836800000000)),
            ]),
        )
        .unwrap();

        assert_eq!(
            Row::from_avro_parsed(row_schema.clone(), v.clone(), &avro_schema, None).unwrap(),
            Some(expected.clone()),
        );
        let raw_avro_schema_with_optional = r#"
        {
            "name": "optschema",
            "type": ["null", VALUE_SCHEMA]
        }
        "#
        .replace("VALUE_SCHEMA", raw_avro_schema);
        let schema = AvroSchema::parse_str(&raw_avro_schema_with_optional).unwrap();
        let v = AvroValue::Union(1, Box::new(v));
        assert_eq!(
            Row::from_avro_parsed(row_schema.clone(), v, &schema, None).unwrap(),
            Some(expected)
        );
        let v = AvroValue::Union(0, Box::new(AvroValue::Null));
        assert_eq!(
            Row::from_avro_parsed(row_schema.clone(), v, &schema, None).unwrap(),
            None
        );
    }

    #[test]
    fn test_from_json() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
                Field::new("c".to_string(), Type::Optional(Box::new(Type::Bool))),
                Field::new("t".to_string(), Type::Timestamp),
            ])
            .unwrap(),
        );
        // can parse a row from json, including nulls for optional fields
        let row = Row::new(
            schema.clone(),
            Arc::new(vec![
                Value::Int(1),
                Value::Bool(false),
                Value::None,
                // Wednesday, January 1, 2020 12:00:00 AM in GMT
                Value::Timestamp(UTCTimestamp::from(1577836800000000)),
            ]),
        )
        .unwrap();
        // can parse a row from json, including nulls for optional fields
        assert_eq!(
            row.clone(),
            Row::from_json(
                schema.clone(),
                r#" 
            { "a": 1, "b": false, "c": null, "t": "2020-01-01T00:00:00Z"}
            "#,
            )
            .unwrap()
        );
        // also works when optional fields are omitted
        assert_eq!(
            row.clone(),
            Row::from_json(
                schema.clone(),
                r#" 
            { "a": 1, "b": false, "t": "2020-01-01T00:00:00Z"}
            "#,
            )
            .unwrap()
        );
        // can parse timestamps in both iso and unix format
        assert_eq!(
            row.clone(),
            Row::from_json(
                schema.clone(),
                r#" 
            { "a": 1, "b": false, "c": null, "t": 1577836800000}
            "#,
            )
            .unwrap()
        );
        // also supports RFC2822 format
        assert_eq!(
            row.clone(),
            Row::from_json(
                schema.clone(),
                r#" 
            { "a": 1, "b": false, "c": null, "t": "Tue, 31 Dec 2019 19:00:00 EST"}
            "#,
            )
            .unwrap()
        );
        // We do support date types from str. Most of the Postgres supported formats are supported
        // https://tinyurl.com/pg-date
        assert_eq!(
            row.clone(),
            Row::from_json(
                schema.clone(),
                r#" { "a": 1, "b": false, "t": "2020-01-01"} "#,
            )
            .unwrap()
        );
        // or if a non-optional field is missing
        Row::from_json(schema.clone(), r#" { "a": 1, "b": false }"#).unwrap_err();
        // or if a field is of the wrong type
        Row::from_json(
            schema.clone(),
            r#" { "a": 1, "b": "foo", "t": "2020-01-01T00:00:00Z"} "#,
        )
        .unwrap_err();
        // or if a float is used instead of an int, even if its a whole number
        Row::from_json(
            schema.clone(),
            r#" { "a": 1.0, "b": false, "t": "2020-01-01T00:00:00Z"} "#,
        )
        .unwrap_err();
    }

    #[test]
    fn test_to_json() {
        let row = Row::new(
            Arc::new(
                Schema::new(vec![
                    Field::new("a".to_string(), Type::Int),
                    Field::new("b".to_string(), Type::Optional(Box::new(Type::Int))),
                    Field::new("c".to_string(), Type::Embedding(2)),
                    Field::new("d".to_string(), Type::Map(Box::new(Type::Float))),
                ])
                .unwrap(),
            ),
            Arc::new(vec![
                Value::Int(2),
                Value::Int(10),
                Value::Embedding(Arc::new(vec![1.0, 2.0])),
                Value::Map(Arc::new(
                    Map::new(Type::Float, &[("bar".to_string(), Value::Float(40.0))]).unwrap(),
                )),
            ]),
        )
        .unwrap();
        assert_eq!(
            row.to_json().unwrap().to_string(),
            "{\"a\":2,\"b\":10,\"c\":[1.0,2.0],\"d\":{\"bar\":40.0}}"
        );
    }

    #[test]
    fn from_primitives() {
        let c = Col::new(
            Arc::new(Field::new("a".to_string(), Type::Int)),
            Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
        )
        .unwrap();
        // can create a column from vector of primitive types
        let c1 = Col::from("a", Type::Int, vec![1, 2, 3]).unwrap();
        assert_eq!(c1, c);

        // or from arrays
        let c2 = Col::from("a", Type::Int, [1, 2, 3]).unwrap();
        assert_eq!(c2, c);

        // test this for other types
        let c_bool = Col::new(
            Arc::new(Field::new("a".to_string(), Type::Bool)),
            Arc::new(vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
            ]),
        )
        .unwrap();
        let c = Col::from("a", Type::Bool, [true, false, true]).unwrap();
        assert_eq!(c, c_bool);

        let c_float = Col::new(
            Arc::new(Field::new("a".to_string(), Type::Float)),
            Arc::new(vec![
                Value::Float(1.0),
                Value::Float(2.0),
                Value::Float(3.0),
            ]),
        )
        .unwrap();
        let c = Col::from("a", Type::Float, [1.0, 2.0, 3.0]).unwrap();
        assert_eq!(c, c_float);

        let c_string = Col::new(
            Arc::new(Field::new("a".to_string(), Type::String)),
            Arc::new(vec![
                Value::String(Arc::new("a".to_string())),
                Value::String(Arc::new("b".to_string())),
                Value::String(Arc::new("c".to_string())),
            ]),
        )
        .unwrap();
        let c = Col::from("a", Type::String, ["a", "b", "c"]).unwrap();
        assert_eq!(c, c_string);

        // also works for optional types
        let c1 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Int)),
            [Some(1), None, Some(3)],
        )
        .unwrap();
        let c2 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Int)),
            [Some(1), None, Some(3)],
        )
        .unwrap();
        assert_eq!(c1, c2);

        let c1 = Col::new(
            Arc::new(Field::new(
                "a".to_string(),
                Type::Optional(Box::new(Type::Float)),
            )),
            Arc::new(vec![Value::Float(1.0), Value::None, Value::Float(3.0)]),
        )
        .unwrap();
        let c2 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Float)),
            [Some(1.0), None, Some(3.0)],
        )
        .unwrap();
        assert_eq!(c1, c2);

        // same for bools
        let c1 = Col::new(
            Arc::new(Field::new(
                "a".to_string(),
                Type::Optional(Box::new(Type::Bool)),
            )),
            Arc::new(vec![Value::Bool(true), Value::None, Value::Bool(false)]),
        )
        .unwrap();
        let c2 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Bool)),
            [Some(true), None, Some(false)],
        )
        .unwrap();
        assert_eq!(c1, c2);

        // and same for strings
        let c1 = Col::new(
            Arc::new(Field::new(
                "a".to_string(),
                Type::Optional(Box::new(Type::String)),
            )),
            Arc::new(vec![
                Value::String(Arc::new("a".to_string())),
                Value::None,
                Value::String(Arc::new("c".to_string())),
            ]),
        )
        .unwrap();

        let c2 = Col::from(
            "a",
            Type::Optional(Box::new(Type::String)),
            [Some("a"), None, Some("c")],
        )
        .unwrap();
        assert_eq!(c1, c2);

        // and works for timestamps too - conversion can be from vec of ints
        let c1 = Col::from("a", Type::Timestamp, [1577836800, 1577836801, 1577836802]).unwrap();
        let c2 = Col::new(
            Arc::new(Field::new("a".to_string(), Type::Timestamp)),
            Arc::new(vec![
                Value::Timestamp(UTCTimestamp::from(1577836800)),
                Value::Timestamp(UTCTimestamp::from(1577836801)),
                Value::Timestamp(UTCTimestamp::from(1577836802)),
            ]),
        )
        .unwrap();
        assert_eq!(c1, c2);
        // and also for optional timestamps
        let c1 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Timestamp)),
            [Some(1577836800), None, Some(1577836802)],
        )
        .unwrap();
        let c2 = Col::new(
            Arc::new(Field::new(
                "a".to_string(),
                Type::Optional(Box::new(Type::Timestamp)),
            )),
            Arc::new(vec![
                Value::Timestamp(UTCTimestamp::from(1577836800)),
                Value::None,
                Value::Timestamp(UTCTimestamp::from(1577836802)),
            ]),
        )
        .unwrap();
        assert_eq!(c1, c2);
    }

    #[test]
    fn col_to_tsseries() {
        let c1 = Col::from("a'", Type::Int, [1, 2, 3]).unwrap();
        let c2 = Col::from("b", Type::Timestamp, [1, 2, 3]).unwrap();
        let expected = Arc::new(vec![
            UTCTimestamp::from(1),
            UTCTimestamp::from(2),
            UTCTimestamp::from(3),
        ]);
        // can not convert a column of ints to a timestamp series
        TryInto::<TSSeries>::try_into(c1).unwrap_err();

        // but can do it for a column of timestamps
        assert_eq!(TryInto::<TSSeries>::try_into(c2).unwrap(), expected);
    }

    #[test]
    fn test_null_count() {
        let c1 = Col::from("a", Type::Int, [1, 2, 3]).unwrap();
        assert_eq!(c1.null_count(), None);
        let c2 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Int)),
            [Some(1), None, Some(3)],
        )
        .unwrap();
        assert_eq!(c2.null_count(), Some(1));

        // nullable column with no nulls
        let c3 = Col::from(
            "a",
            Type::Optional(Box::new(Type::Int)),
            [Some(1), Some(2), Some(3)],
        )
        .unwrap();
        assert_eq!(c3.null_count(), Some(0));
    }

    #[test]
    fn test_hash_eq() {
        let schema1 = Schema::new(vec![
            Field::new("a".to_string(), Type::Int),
            Field::new("b".to_string(), Type::Float),
            Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
        ])
        .unwrap();
        let r1 = Row::new(
            Arc::new(schema1.clone()),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();

        // row with same values should have same hash
        let r2 = Row::new(
            Arc::new(schema1.clone()),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();
        assert_eq!(r1.hash(), r2.hash());
        assert_eq!(r1.clone(), r2.clone());

        // row with same values but different field order should have same hash
        let schema2 = Schema::new(vec![
            Field::new("b".to_string(), Type::Float),
            Field::new("a".to_string(), Type::Int),
            Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
        ])
        .unwrap();
        let r3 = Row::new(
            Arc::new(schema2),
            Arc::new(vec![Value::Float(2.0), Value::Int(1), Value::None]),
        )
        .unwrap();
        assert_eq!(r1.clone(), r3.clone());
        assert_eq!(r1.hash(), r3.hash());

        // hash should change if any of the field names change
        let schema3 = Schema::new(vec![
            Field::new("a".to_string(), Type::Int),
            Field::new("b".to_string(), Type::Float),
            Field::new("d".to_string(), Type::Optional(Box::new(Type::Int))),
        ])
        .unwrap();
        let r4 = Row::new(
            Arc::new(schema3),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();
        assert_ne!(r1.hash(), r4.hash());
        assert_ne!(r1.clone(), r4.clone());

        // hash also changes when any of the types changes (even if the values are the same)
        let schema4 = Schema::new(vec![
            Field::new("a".to_string(), Type::Int),
            Field::new("b".to_string(), Type::Float),
            Field::new("c".to_string(), Type::Optional(Box::new(Type::Float))),
        ])
        .unwrap();
        let r5 = Row::new(
            Arc::new(schema4),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();
        assert_eq!(r1.values(), r5.values());
        assert_ne!(r1.clone(), r4.clone());
        assert_ne!(r1.hash(), r5.hash());

        // hash should change if any of the values change
        let schema5 = Schema::new(vec![
            Field::new("a".to_string(), Type::Int),
            Field::new("b".to_string(), Type::Float),
            Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
        ])
        .unwrap();
        let r6 = Row::new(
            Arc::new(schema5),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::Int(3)]),
        )
        .unwrap();
        assert_ne!(r1.hash(), r6.hash());
        assert_ne!(r1.clone(), r4.clone());
    }

    #[test]
    fn test_to_from_bytes_for_row() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Float),
                Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
            ])
            .unwrap(),
        );
        let r1 = Row::new(
            schema.clone(),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();
        let r2 = Row::new(
            schema.clone(),
            Arc::new(vec![Value::Int(2), Value::Float(3.0), Value::Int(2)]),
        )
        .unwrap();
        let r3 = Row::new(
            schema.clone(),
            Arc::new(vec![Value::Int(3), Value::Float(4.0), Value::Int(3)]),
        )
        .unwrap();
        let bytes = Row::to_bytes(&vec![r1.clone(), r2.clone(), r3.clone()]).unwrap();
        assert_eq!(3, bytes.len());
        assert_eq!(r1, Row::from_bytes(schema.clone(), &bytes[0]).unwrap());
        assert_eq!(r2, Row::from_bytes(schema.clone(), &bytes[1]).unwrap());
        assert_eq!(r3, Row::from_bytes(schema.clone(), &bytes[2]).unwrap());

        // it is not possible to deserialize a row with a different schema
        let schema2 = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Float),
                Field::new("d".to_string(), Type::Optional(Box::new(Type::Int))),
            ])
            .unwrap(),
        );
        Row::from_bytes(schema2.clone(), &bytes[0]).unwrap_err();
    }

    #[test]
    fn test_col_to_from_bytes() {
        let cases = [
            Col::from("c1", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from(
                "c2",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), Some(2), Some(3)],
            )
            .unwrap(),
            Col::from(
                "c3",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
            Col::from("c4", Type::Int, Vec::<i64>::new()).unwrap(),
        ];
        for case in cases {
            let mut buf = vec![];
            case.encode(&mut buf).unwrap();
            let other =
                Col::decode(case.name(), case.dtype().clone(), &mut buf.as_slice()).unwrap();
            assert_eq!(case, other);
        }
    }

    #[test]
    fn test_row_project() {
        let r1 = Row::new(
            Arc::new(
                Schema::new(vec![
                    Field::new("a".to_string(), Type::Int),
                    Field::new("b".to_string(), Type::Float),
                    Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
                ])
                .unwrap(),
            ),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();

        // it's an error to project a field that doesn't exist
        r1.project(&["d"]).unwrap_err();

        // it's also okay to project using an empty list of fields
        let c: Vec<String> = vec![];
        r1.project(&c).unwrap();

        // but it's ok to project using a list of fields that are a subset of the original schema
        let r2 = r1.project(&["a", "c"]).unwrap();
        assert_eq!(
            r2,
            Row::new(
                Arc::new(
                    Schema::new(vec![
                        Field::new("a".to_string(), Type::Int),
                        Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
                    ])
                    .unwrap(),
                ),
                Arc::new(vec![Value::Int(1), Value::None]),
            )
            .unwrap()
        );
    }

    #[test]
    fn test_row_select() {
        let r1 = Row::new(
            Arc::new(
                Schema::new(vec![
                    Field::new("a".to_string(), Type::Int),
                    Field::new("b".to_string(), Type::Float),
                    Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
                ])
                .unwrap(),
            ),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();

        // it's an error to select a field that doesn't exist
        r1.select(&["d"]).unwrap_err();

        // it's also okay to select using an empty list of fields
        let c: Vec<String> = vec![];
        r1.select(&c).unwrap();

        // but it's ok to select using a list of fields that are a subset of the original schema
        let r2 = r1.select(&["a", "c"]).unwrap();
        assert_eq!(r2, vec![&Value::Int(1), &Value::None],);
    }
    #[test]
    fn test_row_set() {
        let r1 = Row::new(
            Arc::new(
                Schema::new(vec![
                    Field::new("a".to_string(), Type::Int),
                    Field::new("b".to_string(), Type::Float),
                    Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
                ])
                .unwrap(),
            ),
            Arc::new(vec![Value::Int(1), Value::Float(2.0), Value::None]),
        )
        .unwrap();
        // can not set a field that doesn't exist
        r1.clone().set("d", Value::Int(1)).unwrap_err();
        // can not set a field with the wrong type
        r1.clone().set("a", Value::Float(1.0)).unwrap_err();
        // but works for fields that exist and have the correct type
        let r2 = r1.clone().set("a", Value::Int(2)).unwrap();
        assert_eq!(
            r2,
            Row::new(
                Arc::new(
                    Schema::new(vec![
                        Field::new("a".to_string(), Type::Int),
                        Field::new("b".to_string(), Type::Float),
                        Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
                    ])
                    .unwrap(),
                ),
                Arc::new(vec![Value::Int(2), Value::Float(2.0), Value::None]),
            )
            .unwrap()
        );
        let r3 = r2.clone().set("c", Value::Int(3)).unwrap();
        assert_eq!(
            r3,
            Row::new(
                Arc::new(
                    Schema::new(vec![
                        Field::new("a".to_string(), Type::Int),
                        Field::new("b".to_string(), Type::Float),
                        Field::new("c".to_string(), Type::Optional(Box::new(Type::Int))),
                    ])
                    .unwrap(),
                ),
                Arc::new(vec![Value::Int(2), Value::Float(2.0), Value::Int(3)]),
            )
            .unwrap()
        );
    }

    #[test]
    fn test_row_hash() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("foo".to_string(), Type::Int),
                Field::new("car".to_string(), Type::Float),
                Field::new("baz".to_string(), Type::String),
            ])
            .unwrap(),
        );
        let row = Row::new(
            schema.clone(),
            Arc::new(vec![
                Value::Int(42),
                Value::Float(3.14),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();
        let found = row.hash();
        assert_eq!(found, 8267029860743657676);
    }

    #[test]
    fn test_row_encoding_immutable() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("foo".to_string(), Type::Int),
                Field::new("car".to_string(), Type::Float),
                Field::new("baz".to_string(), Type::String),
            ])
            .unwrap(),
        );

        let row = Row::new(
            schema.clone(),
            Arc::new(vec![
                Value::Int(42),
                Value::Float(3.14),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();

        let mut encoded = Vec::new();
        row.encode(&mut encoded).unwrap();

        // any change that would modify the encoded bytes should never be allowed
        // because these encoded bytes are stored in DB etc.
        let expected_bytes = vec![
            0x51, 0x28, 0x01, 0x03, 0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x1F, 0x85, 0xEB, 0x51,
            0xB8, 0x1E, 0x09, 0x40, 0x54,
        ];

        assert_eq!(
            encoded, expected_bytes,
            "Encoded bytes do not match expected bytes"
        );

        let decoded_row = Row::decode(schema.clone(), &mut encoded.as_slice()).unwrap();
        assert_eq!(row, decoded_row, "Decoded row does not match original row");
    }

    #[test]
    fn test_row_value_hash() {
        let schema1 = Arc::new(
            Schema::new(vec![
                Field::new("foo".to_string(), Type::Int),
                Field::new("car".to_string(), Type::Float),
                Field::new("baz".to_string(), Type::String),
            ])
            .unwrap(),
        );
        let schema2 = Arc::new(
            Schema::new(vec![
                Field::new("name1".to_string(), Type::Int),
                Field::new("name2".to_string(), Type::Float),
                Field::new("name3".to_string(), Type::String),
            ])
            .unwrap(),
        );

        let schema3 = Arc::new(
            Schema::new(vec![
                Field::new("name1".to_string(), Type::Optional(Box::new(Type::Int))),
                Field::new("name2".to_string(), Type::Float),
                Field::new("name3".to_string(), Type::String),
            ])
            .unwrap(),
        );

        let schema4 = Arc::new(
            Schema::new(vec![
                Field::new("name2".to_string(), Type::Float),
                Field::new("name1".to_string(), Type::Optional(Box::new(Type::Int))),
                Field::new("name3".to_string(), Type::String),
            ])
            .unwrap(),
        );

        let row1 = Row::new(
            schema1.clone(),
            Arc::new(vec![
                Value::Int(42),
                Value::Float(3.14),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();
        let row2 = Row::new(
            schema2.clone(),
            Arc::new(vec![
                Value::Int(42),
                Value::Float(3.14),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();
        let row3 = Row::new(
            schema3.clone(),
            Arc::new(vec![
                Value::Int(42),
                Value::Float(3.14),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();
        let row4 = Row::new(
            schema4.clone(),
            Arc::new(vec![
                Value::Float(3.14),
                Value::Int(42),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();
        let row5 = Row::new(
            schema1.clone(),
            Arc::new(vec![
                Value::Int(43),
                Value::Float(3.14),
                Value::String(Arc::new("hello".to_string())),
            ]),
        )
        .unwrap();

        assert_eq!(row1.value_hash().unwrap(), row2.value_hash().unwrap());
        assert_eq!(row1.value_hash().unwrap(), row3.value_hash().unwrap());
        assert_ne!(row1.value_hash().unwrap(), row4.value_hash().unwrap());
        assert_ne!(row1.value_hash().unwrap(), row5.value_hash().unwrap());
    }
}
