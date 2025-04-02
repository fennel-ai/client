use std::collections::HashMap;
use std::sync::Arc;
use std::{io::Read, io::Write};

use anyhow::{anyhow, Result};
use byteorder::{ReadBytesExt, WriteBytesExt};
use integer_encoding::{VarIntReader, VarIntWriter};
use regex::Regex;
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smartstring::alias::String as SmartString;

use super::value::Value;
use crate::schema::Field;
use crate::schema_proto as fproto;
use crate::schema_proto::schema::RegexType;
use std::fmt::{Display, Formatter};

/// Type represents the type of any value in the system.
/// A type can be a primitive type or a complex type.
/// Note: creating a vector of types is allocation efficient because
/// most types don't have any associated footprint on heap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Type {
    Null, // type that only accepts null values
    Int,
    Float,
    String,
    Bytes,
    Bool,
    Timestamp,
    Date,
    Embedding(usize),
    Optional(Box<Type>),
    List(Box<Type>),
    // we only support string keys for now
    Map(Box<Type>),
    Between(Box<Between>),
    Regex(Box<CompiledRegex>),
    OneOf(Box<OneOf>),
    Struct(Box<StructType>),
    Decimal(Box<DecimalType>),
}

impl Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl AsRef<Type> for Type {
    fn as_ref(&self) -> &Type {
        self
    }
}

impl PartialEq for Type {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Type::Null, Type::Null) => true,
            (Type::Int, Type::Int) => true,
            (Type::Float, Type::Float) => true,
            (Type::String, Type::String) => true,
            (Type::Bytes, Type::Bytes) => true,
            (Type::Bool, Type::Bool) => true,
            (Type::Timestamp, Type::Timestamp) => true,
            (Type::Date, Type::Date) => true,
            (Type::Embedding(n), Type::Embedding(m)) => n == m,
            (Type::Optional(t1), Type::Optional(t2)) => t1 == t2,
            (Type::List(t1), Type::List(t2)) => t1 == t2,
            (Type::Map(t1), Type::Map(t2)) => t1 == t2,
            (Type::Between(b1), Type::Between(b2)) => b1 == b2,
            (Type::Regex(r1), Type::Regex(r2)) => r1.pattern() == r2.pattern(),
            (Type::OneOf(o1), Type::OneOf(o2)) => o1 == o2,
            (Type::Between(b1), Type::Int) => b1.dtype() == &Type::Int,
            (Type::Int, Type::Between(b2)) => b2.dtype() == &Type::Int,
            (Type::Between(b1), Type::Float) => b1.dtype() == &Type::Float,
            (Type::Float, Type::Between(b2)) => b2.dtype() == &Type::Float,
            (Type::OneOf(o1), Type::Int) => o1.dtype() == &Type::Int,
            (Type::Int, Type::OneOf(o2)) => o2.dtype() == &Type::Int,
            (Type::OneOf(o1), Type::String) => o1.dtype() == &Type::String,
            (Type::String, Type::OneOf(o2)) => o2.dtype() == &Type::String,
            (Type::Struct(s1), Type::Struct(s2)) => s1 == s2,
            (Type::Decimal(d1), Type::Decimal(d2)) => d1.scale() == d2.scale(),
            _ => false,
        }
    }
}

impl Eq for Type {}

#[derive(Debug, Clone)]
pub struct CompiledRegex {
    compiled: Regex,
    pattern: String,
}

/// Required for Serialization of CompiledRegex struct since enum Type above implements Serialize
impl Serialize for CompiledRegex {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.pattern)
    }
}

struct RegexVisitor;

/// Required for Deserialization of CompiledRegex struct since enum Type above implements Deserialize
impl<'de> Deserialize<'de> for CompiledRegex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(RegexVisitor)
    }
}

/// Required for Deserialization of CompiledRegex
impl<'de> Visitor<'de> for RegexVisitor {
    type Value = CompiledRegex;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a regex")
    }
    fn visit_str<E>(self, v: &str) -> std::result::Result<Self::Value, E>
    where
        E: Error,
    {
        CompiledRegex::new(v.to_string())
            .map(|re| *re)
            .map_err(|e| {
                Error::custom(format!(
                    "Deserialization of CompiledRegex struct has failed with error: {:?}",
                    e
                ))
            })
    }
}

impl CompiledRegex {
    pub fn new(pattern: String) -> Result<Box<Self>> {
        let full_match = format!("^{}$", pattern);
        let compiled = Regex::new(&full_match)
            .map_err(|e| anyhow!("Regex compilation failed with error : {:?}", e))?;
        Ok(Box::new(Self { compiled, pattern }))
    }
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
    pub fn is_match(&self, pattern: impl AsRef<str>) -> bool {
        self.compiled.is_match(pattern.as_ref())
    }
}

impl Display for CompiledRegex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.pattern())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructType {
    name: SmartString,
    fields: Vec<Field>,
}

impl StructType {
    pub fn new(name: SmartString, mut fields: Vec<Field>) -> Result<StructType> {
        // Sort the fields by first element
        fields.sort_by(|a, b| a.name().cmp(&b.name()));
        // Check for duplicate fields
        for i in 1..fields.len() {
            if fields[i - 1].name() == fields[i].name() {
                return Err(anyhow!("Duplicate field name: {}", fields[i].name()));
            }
        }
        Ok(Self { name, fields })
    }

    /// Returns the dtype of the field with the given name if it exists.
    pub fn dtype_of(&self, name: &str) -> Option<&Type> {
        self.fields
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.dtype())
    }

    pub fn name(&self) -> &SmartString {
        &self.name
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    pub fn field_names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name())
    }

    pub fn get_permissive_struct(&self) -> StructType {
        let sub_fields = self
            .fields
            .iter()
            .map(|f| Field::new(f.name().to_string(), f.dtype().permissive_type()))
            .collect::<Vec<_>>();
        // It is safe to unwrap here since we are converting from a valid struct type, and only
        // changing the types of the fields.
        StructType::new(self.name.clone(), sub_fields).unwrap()
    }

    pub fn get_field_name_to_field_mapping(&self) -> HashMap<String, Field> {
        let mut field_name_to_field_mapping = HashMap::with_capacity(self.fields.len());
        for field in self.fields.iter() {
            field_name_to_field_mapping.insert(field.name().to_string(), field.clone());
        }
        field_name_to_field_mapping
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Between {
    dtype: Type,
    min: Value,
    max: Value,
    strict_min: bool,
    strict_max: bool,
}

impl Between {
    pub fn new(
        dtype: Type,
        min: Value,
        max: Value,
        strict_min: bool,
        strict_max: bool,
    ) -> Result<Box<Self>> {
        if dtype != Type::Int && dtype != Type::Float {
            return Err(anyhow!("Between type can only be used with int or float"));
        }

        Ok(Box::new(Self {
            dtype,
            min,
            max,
            strict_min,
            strict_max,
        }))
    }

    pub fn dtype(&self) -> &Type {
        &self.dtype
    }

    pub fn min(&self) -> &Value {
        &self.min
    }

    pub fn max(&self) -> &Value {
        &self.max
    }

    pub fn strict_min(&self) -> bool {
        self.strict_min
    }

    pub fn strict_max(&self) -> bool {
        self.strict_max
    }
}

// TODO: Make OneOf generic to accept any type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OneOf {
    dtype: Type,
    values: Vec<Value>,
}

impl OneOf {
    pub fn new(dtype: Type, values: Vec<Value>) -> Result<Box<Self>> {
        if dtype != Type::Int && dtype != Type::String {
            return Err(anyhow!("OneOf type can only be used with int, or string"));
        }

        Ok(Box::new(Self { dtype, values }))
    }

    pub fn dtype(&self) -> &Type {
        &self.dtype
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.values
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecimalType {
    scale: u32,
}

impl DecimalType {
    pub fn new(scale: u32) -> Result<Box<Self>> {
        if scale == 0 {
            return Err(anyhow!(
                "Please use int type if using decimal with scale 0."
            ));
        }

        // This is because python decimal library has maximum threshold of 28 as precision.
        if scale > 28 {
            return Err(anyhow!("Currently only scale upto 28 is supported"));
        }

        Ok(Box::new(Self { scale }))
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }
}

impl Type {
    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Optional(_) | Type::Null)
    }

    /// Checks whether dtype is optional or not.
    pub fn is_optional(&self) -> bool {
        matches!(self, Type::Optional(_))
    }

    /// Makes a type optional if it is not already.
    pub fn optional(t: Type) -> Type {
        match t {
            Type::Optional(_) => t,
            _ => Type::Optional(Box::new(t)),
        }
    }

    /// Unwraps an optional type if it is optional, else returns the type itself.
    pub fn inner(&self) -> &Type {
        match self {
            Type::Optional(t) => t.inner(),
            _ => self,
        }
    }

    /// Write a type to the writer in efficient binary format.
    pub fn encode(&self, mut w: &mut dyn Write) -> Result<()> {
        match self {
            Type::Int => w.write_u8(0)?,
            Type::Float => w.write_u8(1)?,
            Type::String => w.write_u8(2)?,
            Type::Bool => w.write_u8(3)?,
            Type::Timestamp => w.write_u8(4)?,
            Type::Embedding(n) => {
                w.write_u8(5)?; // 5 is the type code for embedding
                w.write_varint(*n as u32)?;
            }
            Type::Optional(t) => {
                w.write_u8(6)?; // 6 is the type code for optional
                t.encode(w)?;
            }
            Type::List(t) => {
                w.write_u8(7)?; // 7 is the type code for list
                t.encode(w)?;
            }
            Type::Map(t) => {
                w.write_u8(8)?; // 8 is the type code for map
                t.encode(w)?;
            }
            Type::Between(b) => {
                if *b.dtype() == Type::Int {
                    // We are not encoding type separately but assigning different codes itself
                    // as it saves space. We use one byte instead of two.
                    w.write_u8(9)?; // 9 is the type code for between of ints
                    let min_thresh = b.min.as_int()?;
                    let max_thresh = b.max.as_int()?;
                    w.write_varint(min_thresh)?;
                    w.write_varint(max_thresh)?;
                    w.write_u8(b.strict_min as u8)?;
                    w.write_u8(b.strict_max as u8)?;
                } else if *b.dtype() == Type::Float {
                    w.write_u8(10)?; // 10 is the type code for between of floats
                    let min_thresh = b.min.as_float()?;
                    let max_thresh = b.max.as_float()?;
                    w.write_f64::<byteorder::LittleEndian>(min_thresh)?;
                    w.write_f64::<byteorder::LittleEndian>(max_thresh)?;
                    w.write_u8(b.strict_min as u8)?;
                    w.write_u8(b.strict_max as u8)?;
                }
            }
            Type::Regex(s) => {
                w.write_u8(11)?; // 11 is the type code for regex
                w.write_varint(s.pattern().len() as u32)?;
                w.write_all(s.pattern().as_bytes())?;
            }
            Type::OneOf(o) => {
                let v = o.values();
                w.write_u8(12)?; // 12 is the type code for oneof of ints
                o.dtype().encode(w)?;
                w.write_varint(v.len() as u32)?;
                if *o.dtype() == Type::Int {
                    for i in v {
                        let int_val = i.as_int()?;
                        w.write_varint(int_val)?;
                    }
                } else {
                    for i in v {
                        let s = i.as_str()?;
                        w.write_varint(s.len() as u32)?;
                        w.write_all(s.as_bytes())?;
                    }
                }
            }
            Type::Struct(s) => {
                w.write_u8(13)?; // 13 is the type code for struct
                w.write_varint(s.num_fields() as u32)?;
                w.write_varint(s.name().len() as u32)?;
                w.write_all(s.name().as_bytes())?;
                for f in s.fields().iter() {
                    w.write_varint(f.name().len() as u32)?;
                    w.write_all(f.name().as_bytes())?;
                    f.dtype().encode(w)?;
                }
            }
            Type::Decimal(d) => {
                w.write_u8(14)?; // 14 is the type code for decimal
                w.write_varint(d.scale())?;
            }
            Type::Date => w.write_u8(15)?, // 15 is the type code for decimal
            Type::Bytes => w.write_u8(16)?, // 16 is the type code for bytes
            Type::Null => w.write_u8(17)?, // 17 is the type code for null
        }
        Ok(())
    }

    pub fn is_hashable(&self) -> bool {
        match self {
            Type::Null
            | Type::Int
            | Type::Bool
            | Type::String
            | Type::Bytes
            | Type::Timestamp
            | Type::Regex(_)
            | Type::Decimal(_)
            | Type::Date => true,
            Type::Between(b) => b.dtype.is_hashable(),
            Type::OneOf(of) => of.dtype.is_hashable(),
            Type::List(l) => l.is_hashable(),
            Type::Map(m) => m.is_hashable(),
            Type::Optional(dtype) => dtype.is_hashable(),
            Type::Struct(s) => s.fields().iter().all(|f| f.dtype().is_hashable()),
            Type::Float | Type::Embedding(_) => false,
        }
    }

    /// Read a type from the reader written in efficient binary format.
    pub fn decode(mut r: &mut dyn Read) -> Result<Self> {
        match r.read_u8()? {
            0 => Ok(Type::Int),
            1 => Ok(Type::Float),
            2 => Ok(Type::String),
            3 => Ok(Type::Bool),
            4 => Ok(Type::Timestamp),
            5 => {
                let n = r.read_varint::<usize>()?;
                Ok(Type::Embedding(n))
            }
            6 => {
                let t = Type::decode(r)?;
                Ok(Type::Optional(Box::new(t)))
            }
            7 => {
                let t = Type::decode(r)?;
                Ok(Type::List(Box::new(t)))
            }
            8 => {
                let t = Type::decode(r)?;
                Ok(Type::Map(Box::new(t)))
            }
            9 => {
                let min = Value::Int(r.read_varint::<i64>()?);
                let max = Value::Int(r.read_varint::<i64>()?);
                let strict_min = r.read_u8()? != 0;
                let strict_max = r.read_u8()? != 0;
                Ok(Type::Between(Between::new(
                    Type::Int,
                    min,
                    max,
                    strict_min,
                    strict_max,
                )?))
            }
            10 => {
                let min = Value::Float(r.read_f64::<byteorder::LittleEndian>()?);
                let max = Value::Float(r.read_f64::<byteorder::LittleEndian>()?);
                let strict_min = r.read_u8()? != 0;
                let strict_max = r.read_u8()? != 0;
                Ok(Type::Between(Between::new(
                    Type::Float,
                    min,
                    max,
                    strict_min,
                    strict_max,
                )?))
            }
            11 => {
                let len = r.read_varint::<usize>()?;
                let mut buf = vec![0; len];
                r.read_exact(&mut buf)?;
                let s = String::from_utf8(buf)?;
                let re = CompiledRegex::new(s)?;
                Ok(Type::Regex(re))
            }
            12 => {
                let t = Type::decode(r)?;
                let len = r.read_varint::<usize>()?;
                let mut v = Vec::with_capacity(len);
                if t == Type::Int {
                    for _ in 0..len {
                        let s = r.read_varint::<i64>()?;
                        v.push(Value::Int(s));
                    }
                    Ok(Type::OneOf(OneOf::new(Type::Int, v)?))
                } else {
                    for _ in 0..len {
                        let len = r.read_varint::<usize>()?;
                        let mut buf = vec![0; len];
                        r.read_exact(&mut buf)?;
                        let s = String::from_utf8(buf)?;
                        v.push(Value::String(Arc::new(s)));
                    }
                    Ok(Type::OneOf(OneOf::new(Type::String, v)?))
                }
            }
            14 => {
                let scale = r.read_varint::<u32>()?;
                Ok(Type::Decimal(DecimalType::new(scale)?))
            }
            15 => Ok(Type::Date),
            16 => Ok(Type::Bytes),
            17 => Ok(Type::Null),
            n => Err(anyhow!("invalid type: {}", n)),
        }
    }

    pub fn from_proto(_proto: &fproto::schema::DataType) -> Result<Self> {
        panic!("to be deprecated")
    }

    /// https://json-schema.org/understanding-json-schema/reference/type.html
    /// It also matches the serde JsonValue.
    pub fn to_json_schema_type(&self) -> &'static str {
        match self {
            Type::Null => "null",
            Type::Int => "integer",
            Type::Float => "number",
            Type::String => "string",
            Type::Bool => "boolean",
            Type::Timestamp => "string",
            Type::Embedding(_) => "array",
            Type::Optional(inner_type) => inner_type.to_json_schema_type(),
            Type::List(_) => "array",
            Type::Map(_) => "object",
            Type::Between(b) => {
                return b.dtype.to_json_schema_type();
            }
            Type::Regex(_) => "string",
            Type::OneOf(o) => {
                return o.dtype().to_json_schema_type();
            }
            Type::Struct(_) => "object",
            Type::Decimal(_) => "number",
            Type::Date => "string",
            // Json schema does not have bytes type and it must be a base64 encoded string.
            Type::Bytes => "string",
        }
    }

    // Sub type represents a more permissive type that can be used to parse columns.
    pub fn permissive_type(&self) -> Self {
        Type::Optional(Box::from(match self {
            Type::Null
            | Type::Int
            | Type::Float
            | Type::String
            | Type::Bytes
            | Type::Bool
            | Type::Timestamp
            | Type::Decimal(_)
            | Type::Date => self.clone(),
            Type::Between(b) => b.dtype().clone(),
            Type::OneOf(o) => o.dtype().clone(),
            Type::Regex(_) => Type::String,
            Type::List(inner_type) => Type::List(inner_type.permissive_type().into()),
            Type::Map(inner_type) => Type::Map(inner_type.permissive_type().into()),
            Type::Embedding(_) => Type::List(Box::new(Type::Float)),
            Type::Optional(_) => self.clone(), // This makes it an Optional<Optional<T>> which is fine.
            Type::Struct(s) => Type::Struct(Box::from(s.get_permissive_struct())),
        }))
    }
}

type ProtoType = fproto::schema::DataType;
type ProtoStructType = fproto::schema::StructType;

impl TryFrom<ProtoStructType> for StructType {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoStructType) -> Result<Self> {
        let fields = proto
            .fields
            .iter()
            .map(|f| {
                if f.dtype.is_none() {
                    return Err(anyhow!("no type specified for field {}", f.name));
                }
                let field_type = Type::try_from(f.dtype.as_ref().unwrap().clone())?;
                Ok(Field::new(f.name.to_owned(), field_type))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(StructType::new(proto.name.to_owned().into(), fields)?)
    }
}

impl From<&StructType> for ProtoStructType {
    fn from(value: &StructType) -> Self {
        fproto::schema::StructType {
            name: value.name().to_string(),
            fields: value
                .fields()
                .iter()
                .map(|f| fproto::schema::Field {
                    name: f.name().to_string(),
                    dtype: Some(ProtoType::from(f.dtype())),
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<ProtoType> for Type {
    type Error = anyhow::Error;

    fn try_from(proto: ProtoType) -> Result<Self> {
        use fproto::schema::data_type::Dtype;
        if proto.dtype.is_none() {
            return Err(anyhow!("no type specified").into());
        }
        let dtype = proto.dtype.unwrap();
        match dtype {
            Dtype::NullType(_) => Ok(Type::Null),
            Dtype::IntType(_) => Ok(Type::Int),
            Dtype::DoubleType(_) => Ok(Type::Float),
            Dtype::StringType(_) => Ok(Type::String),
            Dtype::BytesType(_) => Ok(Type::Bytes),
            Dtype::BoolType(_) => Ok(Type::Bool),
            Dtype::TimestampType(_) => Ok(Type::Timestamp),
            Dtype::DateType(_) => Ok(Type::Date),
            Dtype::ArrayType(array_type) => match array_type.of.as_ref() {
                Some(of) => {
                    let of = Type::try_from(of.as_ref().clone())?;
                    Ok(Type::List(Box::new(of)))
                }
                None => Err(anyhow!("no type specified").into()),
            },
            Dtype::MapType(map_type) => match (map_type.key.as_ref(), map_type.value.as_ref()) {
                (Some(key), Some(value)) => {
                    let key = Type::try_from(key.as_ref().clone())?;
                    if key != Type::String {
                        return Err(anyhow!("map keys must be strings").into());
                    }
                    let value = Type::try_from(value.as_ref().clone())?;
                    Ok(Type::Map(Box::new(value)))
                }
                _ => Err(anyhow!("no key or value type specified").into()),
            },
            Dtype::EmbeddingType(embedding_type) => match embedding_type.embedding_size {
                0 => Err(anyhow!("embedding dimension must be > 0").into()),
                n => Ok(Type::Embedding(n as usize)),
            },
            Dtype::BetweenType(between_type) => {
                match (&between_type.dtype, &between_type.min, &between_type.max) {
                    (Some(dtype), Some(min), Some(max)) => {
                        let min = min.as_ref().clone().try_into()?;
                        let max = max.as_ref().clone().try_into()?;
                        Ok(Type::Between(Between::new(
                            dtype.as_ref().clone().try_into()?,
                            min,
                            max,
                            between_type.strict_min,
                            between_type.strict_max,
                        )?))
                    }
                    _ => Err(anyhow!("one of between type, min, max not specified").into()),
                }
            }
            Dtype::RegexType(re) => {
                let s = re.pattern.to_owned();
                let re = CompiledRegex::new(s)?;
                Ok(Type::Regex(re))
            }
            Dtype::OneOfType(oneof) => match oneof.of.as_ref() {
                Some(dtype) => {
                    let dtype = dtype.as_ref().clone().try_into()?;
                    let values = oneof
                        .options
                        .iter()
                        .map(|v| v.to_owned().try_into())
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Type::OneOf(OneOf::new(dtype, values)?))
                }
                None => Err(anyhow!("no type specified for oneof").into()),
            },
            Dtype::StructType(struct_type) => Ok(Type::Struct(Box::new(struct_type.try_into()?))),
            Dtype::OptionalType(optional) => match optional.of.as_ref() {
                Some(dtype) => {
                    let dtype = dtype.as_ref().clone().try_into()?;
                    Ok(Type::Optional(Box::new(dtype)))
                }
                None => Err(anyhow!("no type specified for optional").into()),
            },
            Dtype::DecimalType(decimal_type) => {
                Ok(Type::Decimal(DecimalType::new(decimal_type.scale as u32)?))
            }
        }
    }
}

impl From<&Type> for ProtoType {
    fn from(value: &Type) -> Self {
        match value {
            Type::Null => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::NullType(
                    fproto::schema::NullType {},
                )),
            },
            Type::Int => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::IntType(
                    fproto::schema::IntType {},
                )),
            },
            Type::Float => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::DoubleType(
                    fproto::schema::DoubleType {},
                )),
            },
            Type::String => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::StringType(
                    fproto::schema::StringType {},
                )),
            },
            Type::Bytes => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::BytesType(
                    fproto::schema::BytesType {},
                )),
            },
            Type::Bool => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::BoolType(
                    fproto::schema::BoolType {},
                )),
            },
            Type::Timestamp => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::TimestampType(
                    fproto::schema::TimestampType {},
                )),
            },
            Type::Date => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::DateType(
                    fproto::schema::DateType {},
                )),
            },
            Type::Optional(inner_type) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::OptionalType(Box::new(
                    fproto::schema::OptionalType {
                        of: Some(Box::new(ProtoType::from(inner_type.as_ref()))),
                    },
                ))),
            },
            Type::Embedding(size) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::EmbeddingType(
                    fproto::schema::EmbeddingType {
                        embedding_size: *size as i32,
                    },
                )),
            },
            Type::List(inner_type) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::ArrayType(Box::new(
                    fproto::schema::ArrayType {
                        of: Some(Box::new(ProtoType::from(inner_type.as_ref()))),
                    },
                ))),
            },
            Type::Map(value_type) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::MapType(Box::new(
                    fproto::schema::MapType {
                        key: Some(Box::new(ProtoType::from(&Type::String))),
                        value: Some(Box::new(ProtoType::from(value_type.as_ref()))),
                    },
                ))),
            },
            Type::Between(b) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::BetweenType(Box::new(
                    fproto::schema::Between {
                        dtype: Some(Box::new(ProtoType::from(b.dtype()))),
                        min: Some(Box::new((&b.min).try_into().unwrap())),
                        max: Some(Box::new((&b.max).try_into().unwrap())),
                        strict_max: b.strict_max,
                        strict_min: b.strict_min,
                    },
                ))),
            },
            Type::OneOf(o) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::OneOfType(Box::new(
                    fproto::schema::OneOf {
                        of: Some(Box::new(ProtoType::from(o.dtype()))),
                        options: o.values().iter().map(|v| v.into()).collect::<Vec<_>>(),
                    },
                ))),
            },
            Type::Regex(r) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::RegexType(RegexType {
                    pattern: r.as_ref().pattern().to_owned(),
                })),
            },
            Type::Struct(s) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::StructType(
                    s.as_ref().into(),
                )),
            },
            Type::Decimal(d) => fproto::schema::DataType {
                dtype: Some(fproto::schema::data_type::Dtype::DecimalType(
                    fproto::schema::DecimalType {
                        scale: d.scale() as i32,
                    },
                )),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::schema_proto::schema::{data_type::Dtype, ArrayType, IntType, MapType};

    use super::*;

    #[test]
    fn test_encode_decode() {
        for t in types() {
            let mut buf = vec![];
            t.encode(&mut buf).unwrap();
            let mut r = buf.as_slice();
            assert_eq!(t, Type::decode(&mut r).unwrap());
        }
    }

    #[test]
    fn test_permissive_type() {
        // Test for primitive types
        assert_eq!(
            Type::Int.permissive_type(),
            Type::Optional(Box::new(Type::Int))
        );
        assert_eq!(
            Type::Float.permissive_type(),
            Type::Optional(Box::new(Type::Float))
        );
        assert_eq!(
            Type::String.permissive_type(),
            Type::Optional(Box::new(Type::String))
        );
        assert_eq!(
            Type::Bool.permissive_type(),
            Type::Optional(Box::new(Type::Bool))
        );
        assert_eq!(
            Type::Timestamp.permissive_type(),
            Type::Optional(Box::new(Type::Timestamp))
        );
        assert_eq!(
            Type::Date.permissive_type(),
            Type::Optional(Box::new(Type::Date))
        );

        // Test for Embedding
        let embedding_size = 10;
        let embedding_type = Type::Embedding(embedding_size);
        assert_eq!(
            embedding_type.permissive_type(),
            Type::Optional(Box::new(Type::List(Box::new(Type::Float))))
        );

        // Test for Optional
        let optional_type = Type::Optional(Box::new(Type::Int));
        assert_eq!(
            optional_type.permissive_type(),
            Type::Optional(Box::new(optional_type.clone()))
        );

        // Test for List
        let list_type = Type::List(Box::new(Type::Int));
        assert_eq!(
            list_type.permissive_type(),
            Type::Optional(Box::new(Type::List(Box::new(Type::Optional(Box::new(
                Type::Int
            ))))))
        );

        // Test for Map
        let map_type = Type::Map(Box::new(Type::Float));
        assert_eq!(
            map_type.permissive_type(),
            Type::Optional(Box::new(Type::Map(Box::new(Type::Optional(Box::new(
                Type::Float
            ))))))
        );

        // Test for Between
        let between = Between::new(
            Type::Float,
            Value::Float(1.0),
            Value::Float(100.0),
            true,
            true,
        )
        .unwrap();
        let between_type = Type::Between(between);
        assert_eq!(
            between_type.permissive_type(),
            Type::Optional(Box::from(Type::Float))
        );

        // Test for Regex
        let s = ".*".to_string();
        let regex_type = Type::Regex(CompiledRegex::new(s).unwrap());
        assert_eq!(
            regex_type.permissive_type(),
            Type::Optional(Box::new(Type::String))
        );

        // Test for OneOf
        let one_of =
            OneOf::new(Type::Int, vec![Value::Int(0), Value::Int(1), Value::Int(2)]).unwrap();
        let one_of_type = Type::OneOf(one_of);
        assert_eq!(
            one_of_type.permissive_type(),
            Type::Optional(Box::new(Type::Int))
        );

        // Test for Struct
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
        let permisive_nested_struct_type = Type::Struct(Box::new(
            StructType::new(
                "nested_struct".into(),
                vec![
                    Field::new("nested_field1", Type::Optional(Box::new(Type::Int))),
                    Field::new("nested_field2", Type::Optional(Box::new(Type::String))),
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

        let permissive_struct_type = Type::Optional(Box::new(Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Optional(Box::new(Type::Int))),
                    Field::new(
                        "field2",
                        Type::Optional(Box::new(Type::Struct(Box::new(
                            StructType::new(
                                "inner_struct".into(),
                                vec![
                                    Field::new(
                                        "inner_field1",
                                        Type::Optional(Box::new(Type::Optional(Box::new(
                                            nested_struct_type.clone(),
                                        )))),
                                    ),
                                    Field::new(
                                        "inner_field2",
                                        Type::Optional(Box::new(Type::List(Box::new(
                                            Type::Optional(Box::new(
                                                permisive_nested_struct_type.clone(),
                                            )),
                                        )))),
                                    ),
                                ],
                            )
                            .unwrap(),
                        )))),
                    ),
                ],
            )
            .unwrap(),
        ))));
        // Replace `struct_type.get_permissive_struct()` with appropriate logic based on actual implementation
        assert_eq!(struct_type.permissive_type(), permissive_struct_type);

        // Test for Decimal Type
        assert_eq!(
            Type::Decimal(DecimalType::new(2).unwrap()).permissive_type(),
            Type::Optional(Box::new(Type::Decimal(DecimalType::new(2).unwrap())))
        );
    }

    fn types() -> Vec<Type> {
        vec![
            Type::Int,
            Type::Float,
            Type::String,
            Type::Bytes,
            Type::Bool,
            Type::Timestamp,
            Type::Embedding(10),
            Type::Optional(Box::new(Type::Int)),
            Type::List(Box::new(Type::Int)),
            Type::Map(Box::new(Type::Int)),
            Type::Between(
                Between::new(Type::Int, Value::Int(0), Value::Int(10), false, false).unwrap(),
            ),
            Type::Between(
                Between::new(
                    Type::Float,
                    Value::Float(0.0),
                    Value::Float(10.0),
                    false,
                    false,
                )
                .unwrap(),
            ),
            Type::OneOf(
                OneOf::new(Type::Int, vec![Value::Int(0), Value::Int(1), Value::Int(2)]).unwrap(),
            ),
            Type::OneOf(
                OneOf::new(
                    Type::String,
                    vec![
                        Value::String(Arc::new("0".to_string())),
                        Value::String(Arc::new("1".to_string())),
                        Value::String(Arc::new("2".to_string())),
                    ],
                )
                .unwrap(),
            ),
            {
                let s = ".*".to_string();
                Type::Regex(CompiledRegex::new(s).unwrap())
            },
            Type::Decimal(DecimalType::new(2).unwrap()),
            Type::Date,
        ]
    }

    #[test]
    fn test_encode_decode_additional_types() {
        for t in additional_types() {
            let mut buf = vec![];
            t.encode(&mut buf).unwrap();
            let mut r = buf.as_slice();
            assert_eq!(t, Type::decode(&mut r).unwrap());
        }
    }

    fn additional_types() -> Vec<Type> {
        vec![
            Type::Between(
                Between::new(
                    Type::Float,
                    Value::Float(0.0),
                    Value::Float(1.0),
                    false,
                    false,
                )
                .unwrap(),
            ),
            Type::Between(
                Between::new(Type::Int, Value::Int(3), Value::Int(11), false, true).unwrap(),
            ),
            Type::Between(
                Between::new(
                    Type::Float,
                    Value::Float(-3.412123),
                    Value::Float(32432.324234235234),
                    true,
                    false,
                )
                .unwrap(),
            ),
            Type::Between(
                Between::new(
                    Type::Int,
                    Value::Int(-323),
                    Value::Int(1343243241),
                    true,
                    true,
                )
                .unwrap(),
            ),
            {
                let s = ".*".to_string();
                Type::Regex(CompiledRegex::new(s).unwrap())
            },
            {
                let s = "[a-zA-z]+[0-9]".to_string();
                Type::Regex(CompiledRegex::new(s).unwrap())
            },
            Type::OneOf(
                OneOf::new(
                    Type::String,
                    vec![
                        Value::String(Arc::new("a".to_string())),
                        Value::String(Arc::new("b".to_string())),
                        Value::String(Arc::new("c".to_string())),
                    ],
                )
                .unwrap(),
            ),
            Type::OneOf(
                OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap(),
            ),
        ]
    }

    #[test]
    fn test_type_equality() {
        let t1 = Type::Int;
        let t2 = Type::Int;
        assert_eq!(t1, t2);

        // Check equality between oneof and string
        let t1 = Type::OneOf(
            OneOf::new(Type::String, vec![Value::String(Arc::new("a".to_string()))]).unwrap(),
        );
        let t2 = Type::String;
        assert_eq!(t1, t2);

        // Check equality between oneof and int
        let t1 = Type::OneOf(OneOf::new(Type::Int, vec![Value::Int(1)]).unwrap());
        let t2 = Type::Int;
        assert_eq!(t1, t2);

        // Check equality between between and int
        let t1 = Type::Between(
            Between::new(Type::Int, Value::Int(0), Value::Int(10), false, false).unwrap(),
        );
        let t2 = Type::Int;
        assert_eq!(t1, t2);

        // Check equality between between and float
        let t1 = Type::Between(
            Between::new(
                Type::Float,
                Value::Float(0.0),
                Value::Float(10.0),
                false,
                false,
            )
            .unwrap(),
        );
        let t2 = Type::Float;
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_to_json_schema_type() {
        assert_eq!(
            Type::Optional(Box::new(Type::Embedding(10))).to_json_schema_type(),
            "array"
        );
        assert_eq!(
            Type::Optional(Box::new(Type::Timestamp)).to_json_schema_type(),
            "string"
        );
        assert_eq!(
            Type::Optional(Box::new(Type::Int)).to_json_schema_type(),
            "integer"
        );
        assert_eq!(
            Type::Optional(Box::new(Type::Bytes)).to_json_schema_type(),
            "string"
        );
        assert_eq!(
            Type::Between(
                Between::new(
                    Type::Float,
                    Value::Float(0.0),
                    Value::Float(1.0),
                    false,
                    false,
                )
                .unwrap()
            )
            .to_json_schema_type(),
            "number"
        );
        assert_eq!(
            Type::Between(
                Between::new(Type::Int, Value::Int(3), Value::Int(11), false, true).unwrap()
            )
            .to_json_schema_type(),
            "integer"
        );
        assert_eq!(
            Type::Regex(CompiledRegex::new(".*".to_string()).unwrap()).to_json_schema_type(),
            "string"
        );
        assert_eq!(
            Type::OneOf(
                OneOf::new(
                    Type::String,
                    vec![
                        Value::String(Arc::new("a".to_string())),
                        Value::String(Arc::new("b".to_string())),
                        Value::String(Arc::new("c".to_string())),
                    ],
                )
                .unwrap()
            )
            .to_json_schema_type(),
            "string"
        );
        assert_eq!(
            Type::OneOf(
                OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap()
            )
            .to_json_schema_type(),
            "integer"
        );
        assert_eq!(
            Type::Optional(Box::new(Type::Decimal(DecimalType::new(10).unwrap())))
                .to_json_schema_type(),
            "number"
        );
        assert_eq!(
            Type::Optional(Box::new(Type::Date)).to_json_schema_type(),
            "string"
        );
    }

    #[test]
    fn test_is_hashable_type() {
        assert_eq!(Type::Int.is_hashable(), true);
        assert_eq!(Type::Bool.is_hashable(), true);
        assert_eq!(Type::String.is_hashable(), true);
        assert_eq!(Type::Bytes.is_hashable(), true);
        assert_eq!(Type::Timestamp.is_hashable(), true);
        assert_eq!(Type::Date.is_hashable(), true);
        assert_eq!(
            Type::Regex(CompiledRegex::new("test".to_string()).unwrap()).is_hashable(),
            true
        );

        assert_eq!(
            Type::Between(Box::new(Between {
                dtype: Type::Int,
                min: Value::Int(0),
                max: Value::Int(10),
                strict_max: false,
                strict_min: false,
            }))
            .is_hashable(),
            true
        );
        assert_eq!(
            Type::OneOf(Box::new(OneOf {
                dtype: Type::Bool,
                values: vec![Value::Bool(true), Value::Bool(false)],
            }))
            .is_hashable(),
            true
        );
        assert_eq!(Type::List(Box::new(Type::String)).is_hashable(), true);
        assert_eq!(Type::Map(Box::new(Type::Int)).is_hashable(), true);
        assert_eq!(Type::Optional(Box::new(Type::Bool)).is_hashable(), true);

        assert_eq!(Type::Float.is_hashable(), false);
        assert_eq!(Type::Embedding(10).is_hashable(), false);
        assert_eq!(
            Type::Struct(Box::new(
                StructType::new("test_struct".into(), vec![]).unwrap()
            ))
            .is_hashable(),
            true
        );
        assert_eq!(
            Type::Struct(Box::new(
                StructType::new(
                    "test_struct".into(),
                    vec![
                        Field::new("test", Type::Int),
                        Field::new("test2", Type::String),
                        Field::new("test3", Type::List(Box::new(Type::Int))),
                    ],
                )
                .unwrap()
            ))
            .is_hashable(),
            true
        );
        assert_eq!(
            Type::Struct(Box::new(
                StructType::new(
                    "test_struct".into(),
                    vec![
                        Field::new("test", Type::Int),
                        Field::new("test2", Type::Map(Box::new(Type::Float))),
                        Field::new("test3", Type::List(Box::new(Type::Int))),
                    ],
                )
                .unwrap()
            ))
            .is_hashable(),
            false
        );
        assert_eq!(
            Type::Decimal(DecimalType::new(2).unwrap()).is_hashable(),
            true
        );
    }

    #[test]
    fn test_type_struct_ordering() {
        let struct_type_1 = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field1", Type::Int),
                    Field::new("field2", Type::String),
                    Field::new("field3", Type::Bool),
                    Field::new("field4", Type::Float),
                ],
            )
            .unwrap(),
        ));

        let struct_type_2 = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("field4", Type::Float),
                    Field::new("field2", Type::String),
                    Field::new("field1", Type::Int),
                    Field::new("field3", Type::Bool),
                ],
            )
            .unwrap(),
        ));

        assert_eq!(struct_type_1, struct_type_2);

        // Assert the encodings are the same.
        let mut buf = Vec::new();
        struct_type_1.encode(&mut buf).unwrap();
        let mut buf2 = Vec::new();
        struct_type_2.encode(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_to_from_proto() {
        for t in types() {
            let proto: ProtoType = (&t).into();
            assert_eq!(t, Type::try_from(proto).unwrap());
        }
        // TODO: add invalid proto types
        let invalids: Vec<ProtoType> = vec![
            ProtoType { dtype: None },
            ProtoType {
                // dtype is required
                dtype: Some(Dtype::ArrayType(Box::new(ArrayType { of: None }))),
            },
            ProtoType {
                // key and value are required
                dtype: Some(Dtype::MapType(Box::new(MapType {
                    key: None,
                    value: None,
                }))),
            },
            ProtoType {
                // keys are required
                dtype: Some(Dtype::MapType(Box::new(MapType {
                    key: None,
                    value: Some(Box::new(ProtoType {
                        dtype: Some(Dtype::IntType(IntType {})),
                    })),
                }))),
            },
            ProtoType {
                // keys must be strings
                dtype: Some(Dtype::MapType(Box::new(MapType {
                    key: Some(Box::new(ProtoType {
                        dtype: Some(Dtype::IntType(IntType {})),
                    })),
                    value: Some(Box::new(ProtoType {
                        dtype: Some(Dtype::IntType(IntType {})),
                    })),
                }))),
            },
            ProtoType {
                // Decimal with 0 precision should fail
                dtype: Some(Dtype::DecimalType(fproto::schema::DecimalType { scale: 0 })),
            },
            ProtoType {
                // Decimal with 29 precision should fail
                dtype: Some(Dtype::DecimalType(fproto::schema::DecimalType {
                    scale: 29,
                })),
            },
        ];
        for pt in invalids {
            assert!(Type::try_from(pt).is_err());
        }
    }

    #[test]
    fn test_serialize_deserialize_compiledregex() {
        let regex = "[a-zA-z]+[0-9]".to_string();
        let regex_type = Type::Regex(CompiledRegex::new(regex.clone()).unwrap());

        let json_str = serde_json::to_string(&regex_type).unwrap();
        let expected_json_str = serde_json::json!({"Regex": regex}).to_string();
        assert_eq!(json_str, expected_json_str);

        let compiled_regex: Type = serde_json::from_str(&json_str).unwrap();
        assert_eq!(compiled_regex, regex_type);
    }

    #[test]
    fn test_invalid_compiledregex() {
        let regex = "[.*".to_string();
        CompiledRegex::new(regex.clone()).unwrap_err();
    }
}
