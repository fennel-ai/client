use super::{Type, Value};
use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose, Engine as _};
use num_bigint::ToBigInt;
use serde_json::Value as JsonValue;

impl Value {
    /// Converts internal data values to Debezium JSON format.
    ///
    /// This method takes an internal data value and converts it to a JSON value
    /// that is compatible with Debezium's expected formats for various data types.
    /// For example, an `Int` value is directly converted to a JSON number, while
    /// a `Timestamp` value is converted to a JSON number representing milliseconds.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    ///
    /// ```
    /// use fennel_data_lib::Value;
    /// 
    /// let value = Value::Int(42);
    /// let json_value = value.to_debezium_json().unwrap();
    /// assert_eq!(json_value, serde_json::json!(42));
    /// ```
    ///
    /// Converting a timestamp:
    ///
    /// ```
    /// use fennel_data_lib::UTCTimestamp;
    /// use fennel_data_lib::Value;
    /// 
    /// let timestamp = Value::Timestamp(UTCTimestamp::from(1609459200000));
    /// let json_value = timestamp.to_debezium_json().unwrap();
    /// assert_eq!(json_value, serde_json::json!(1609459200));
    /// ```
    /// https://debezium.io/documentation/reference/1.7/connectors/mysql.html#mysql-data-types
    pub fn to_debezium_json(&self) -> Result<JsonValue> {
        match self {
            Value::None => Ok(JsonValue::Null),
            Value::Int(v) => Ok(JsonValue::from(*v)),
            Value::Float(v) => Ok(JsonValue::from(*v)),
            Value::String(v) => Ok(JsonValue::from(v.as_ref().clone())),
            Value::Bytes(v) => {
                // Convert bytes to base64 encoded string
                Ok(JsonValue::String(
                    general_purpose::STANDARD.encode(v.as_ref()),
                ))
            }
            Value::Bool(v) => Ok(JsonValue::from(*v)),
            // Milliseconds based on https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/connect/data/Timestamp.html
            Value::Timestamp(v) => Ok(JsonValue::from(v.millis())),
            Value::Embedding(v) => Ok(JsonValue::Array(
                v.iter().map(|v| JsonValue::from(*v)).collect(),
            )),
            Value::List(v) => Ok(JsonValue::Array(
                v.iter().map(|v| v.to_json()).collect::<Vec<_>>(),
            )),
            Value::Map(v) => {
                let mut object_map = serde_json::Map::with_capacity(v.len());
                for (k, v) in v.iter() {
                    // assert that the same key is never updated again
                    if object_map.insert(k.clone(), v.to_json()).is_some() {
                        return Err(anyhow!("Map has duplicated key: {}", k));
                    }
                }
                Ok(serde_json::Value::Object(object_map))
            }
            Value::Struct(s) => {
                let mut object_map = serde_json::Map::with_capacity(s.num_fields());
                for (name, val) in s.data() {
                    // assert that the same key is never updated again
                    if object_map
                        .insert(name.to_string(), val.to_json())
                        .is_some()
                    {
                        return Err(anyhow!("Struct has duplicated key: {}", name));
                    }
                }
                Ok(serde_json::Value::Object(object_map))
            }
            // Bytes based on https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/connect/data/Decimal.html
            Value::Decimal(d) => {
                let mantissa = d.mantissa().to_bigint().ok_or(anyhow!(
                    "Failed to convert decimal value's mantissa : {:?} into BigInt",
                    d.mantissa()
                ))?;
                let bytes_string = String::from_utf8(mantissa.to_signed_bytes_be())?;
                Ok(JsonValue::from(bytes_string))
            }
            // Number od days since epoch based on https://kafka.apache.org/24/javadoc/index.html?org/apache/kafka/connect/data/Date.html
            Value::Date(d) => Ok(JsonValue::from(d.days())),
        }
    }
}



impl Type {
    /// Converts the internal data type to a pair of Debezium-compatible data types.
    ///
    /// This method returns a tuple containing the JSON type of the field and the
    /// Kafka Connect data type used to parse the data. The conversion is based on
    /// Debezium's documentation and supports a variety of data types.
    ///
    ///
    /// A tuple of two strings:
    /// - The first string represents the JSON type of the field.
    /// - The second string represents the Kafka Connect data type.t of the non native
    /// data type are converted to json
    /// Take reference from https://debezium.io/documentation/reference/1.7/connectors/mongodb.html
    pub fn to_debezium_data_type(&self) -> Result<(String, String)> {
        let dtype = match self {
            Type::Optional(o) => (**o).clone(),
            x => x.clone(),
        };
        let (json_type, kafka_connect_data_type) = match dtype {
            Type::Int => ("int64", ""),
            Type::Float => ("double", ""),
            Type::Bool => ("boolean", ""),
            Type::String => ("string", ""),
            Type::Timestamp => ("int64", "org.apache.kafka.connect.data.Timestamp"),
            Type::Embedding(_) => ("string", "io.debezium.data.Json"),
            Type::List(_) => ("string", "io.debezium.data.Json"),
            Type::Map(_) => ("string", "io.debezium.data.Json"),
            Type::Between(b) if *b.dtype() == Type::Int => ("int64", ""),
            Type::Between(b) if *b.dtype() == Type::Float => ("double", ""),
            Type::Regex(_) => ("string", ""),
            Type::OneOf(o) if *o.dtype() == Type::Int => ("int64", ""),
            Type::OneOf(o) if *o.dtype() == Type::String => ("string", ""),
            Type::Struct(_) => ("string", "io.debezium.data.Json"),
            Type::Decimal(_) => ("bytes", "org.apache.kafka.connect.data.Decimal"),
            Type::Date => ("int32", "org.apache.kafka.connect.data.Date"),
            _ => bail!("Failed to parse value type"),
        };
        Ok((json_type.to_string(), kafka_connect_data_type.to_string()))
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::*;
    use crate::types::{Between, CompiledRegex, OneOf, StructType};
    use crate::value::{Date, Struct};
    use crate::{Field, List, Map, Type, UTCTimestamp};
    use rust_decimal::Decimal;

    
    #[test]
    fn test_to_debezium_json() {
        let vals = vec![
            (Value::Int(1), "1"),
            (Value::Float(1.0), "1.0"),
            (Value::String(Arc::new("hello".to_string())), "\"hello\""),
            (Value::None, "null"),
            (Value::Bool(true), "true"),
            (Value::Bool(false), "false"),
            // this returns timestamp in rfc3339 format
            (
                // 1 second
                Value::Timestamp(UTCTimestamp::from(1000_000)),
                "1000",
            ),
            (
                Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])),
                "[1.0,2.0,3.0]",
            ),
            (
                Value::List(Arc::new(
                    List::new(
                        Type::Bool,
                        &vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                    )
                    .unwrap(),
                )),
                "[true,false,true]",
            ),
            (
                Value::Map(Arc::new(
                    Map::new(
                        Type::Int,
                        &vec![
                            ("a".to_string(), Value::Int(1)),
                            ("b".to_string(), Value::Int(2)),
                            ("c".to_string(), Value::Int(3)),
                        ],
                    )
                    .unwrap(),
                )),
                "{\"a\":1,\"b\":2,\"c\":3}",
            ),
            (
                Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("field1".into(), Value::Int(1)),
                        (
                            "field2".into(),
                            Value::String(Arc::new("hello".to_string())),
                        ),
                    ])
                    .unwrap(),
                )),
                "{\"field1\":1,\"field2\":\"hello\"}",
            ),
        ];
        for (v, s) in vals {
            assert_eq!(v.to_debezium_json().unwrap().to_string(), s);
        }

        // on a valid `Value`, there should be no error to transform to a JSON Value
    }

    #[test]
    fn test_to_debezium_type() {
        let vals = vec![
            (Type::Int, ("int64", "")),
            (Type::Float, ("double", "")),
            (Type::Bool, ("boolean", "")),
            (Type::String, ("string", "")),
            (
                Type::Timestamp,
                ("int64", "org.apache.kafka.connect.data.Timestamp"),
            ),
            (Type::Embedding(10), ("string", "io.debezium.data.Json")),
            (
                Type::List(Box::new(Type::Int)),
                ("string", "io.debezium.data.Json"),
            ),
            (
                Type::Map(Box::new(Type::String)),
                ("string", "io.debezium.data.Json"),
            ),
            (
                Type::Between(
                    Between::new(Type::Int, Value::Int(0), Value::Int(1), true, true).unwrap(),
                ),
                ("int64", ""),
            ),
            (
                Type::Between(
                    Between::new(
                        Type::Float,
                        Value::Float(0.0),
                        Value::Float(1.0),
                        true,
                        true,
                    )
                    .unwrap(),
                ),
                ("double", ""),
            ),
            (
                Type::Regex(CompiledRegex::new(".*".to_string()).unwrap()),
                ("string", ""),
            ),
            (
                Type::OneOf(
                    OneOf::new(
                        Type::String,
                        vec![
                            Value::String(Arc::new("1".to_string())),
                            Value::String(Arc::new("2".to_string())),
                        ],
                    )
                    .unwrap(),
                ),
                ("string", ""),
            ),
            (
                Type::OneOf(OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2)]).unwrap()),
                ("int64", ""),
            ),
            (
                Type::Struct(Box::new(
                    StructType::new(
                        "struct".to_string().into(),
                        vec![Field::new("1".to_string(), Type::Int)],
                    )
                    .unwrap(),
                )),
                ("string", "io.debezium.data.Json"),
            ),
            (
                Type::Optional(Box::new(Type::Struct(Box::new(
                    StructType::new(
                        "struct".to_string().into(),
                        vec![Field::new("1".to_string(), Type::Int)],
                    )
                    .unwrap(),
                )))),
                ("string", "io.debezium.data.Json"),
            ),
        ];
        for (t, (r1, r2)) in vals {
            assert_eq!(
                t.to_debezium_data_type().unwrap(),
                (r1.to_string(), r2.to_string())
            );
        }
    }

    #[test]
    fn test_decimal_type_to_debezium_json() {
        let value = Value::Decimal(Arc::new(Decimal::new(123, 2)));
        let json_value = value.to_debezium_json().unwrap();
        // Assuming the expected bytes string representation is known
        assert_eq!(json_value, "{");
    }

    #[test]
    fn test_date_type_to_debezium_json() {
        let value = Value::Date(Date::try_from("1970-01-02").unwrap());
        let json_value = value.to_debezium_json().unwrap();
        assert_eq!(json_value, 1);
    }


}
