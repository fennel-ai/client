use std::fmt::Display;
use std::{
    collections::{HashMap, HashSet},
    io::{Read, Write},
};

use super::types::Type;
use crate::schema_proto as fproto;
use anyhow::{anyhow, Context, Result};
use integer_encoding::{VarIntReader, VarIntWriter};
use itertools::Itertools;
use prost::Message;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smartstring::alias::String as SmartString;
use std::hash::Hash;
use std::hash::Hasher;
use xxhash_rust::xxh3::xxh3_64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    name: SmartString,
    dtype: Type,
}

impl Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "field_name: {} field_dtype: {}", self.name, self.dtype)
    }
}
impl AsRef<Field> for Field {
    fn as_ref(&self) -> &Field {
        self
    }
}

impl AsRef<str> for Field {
    fn as_ref(&self) -> &str {
        self.name.as_str()
    }
}

impl Field {
    pub fn new(name: impl Into<SmartString>, dtype: Type) -> Self {
        Field {
            name: name.into(),
            dtype,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline(always)]
    pub fn dtype(&self) -> &Type {
        &self.dtype
    }

    pub fn is_nullable(&self) -> bool {
        self.dtype.is_nullable()
    }

    pub fn drop_nullable(&self) -> Result<Self> {
        if let Type::Optional(inner) = self.dtype() {
            let new_field = Field::new(self.name(), inner.as_ref().clone());
            Ok(new_field)
        } else {
            Err(anyhow!(format!(
                "Expected type Optional type found {:?}",
                self.dtype
            )))
        }
    }
}

/// Schema represents a non-empty ordered collection of fields.
/// The only way to create a schema is to use the `new` method which
/// will check for duplicate field names - hence Schema can be assumed
/// to have unique field names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    fields: SmallVec<[Field; 8]>,
    #[serde(skip)]
    sorted_idxs: SmallVec<[usize; 8]>,
    #[serde(skip)]
    hash: u64,
}

impl Schema {
    /// Create a new schema from a list of fields.
    /// Returns an error if there are duplicate field names
    /// NOTE: it's okay to create a schema with no fields
    /// this is useful for creating a DS Schema with no keys
    pub fn new(fields: Vec<Field>) -> Result<Self> {
        let mut names = HashSet::with_capacity(fields.len());
        for f in &fields {
            if names.contains(f.name()) {
                return Err(anyhow!(
                    "duplicate field name {}, given fields {:?}",
                    f.name(),
                    fields
                ));
            }
            names.insert(f.name());
        }
        let sorted_idxs = fields
            .iter()
            .cloned()
            .enumerate()
            .sorted_by(|(_, a), (_, b)| a.name().cmp(b.name()))
            .map(|(idx, _)| idx)
            .collect_vec();
        let hash = Self::hash_fields(sorted_idxs.iter().map(|&idx| &fields[idx]));
        let sorted_idxs = SmallVec::from(sorted_idxs);
        let fields = SmallVec::from(fields);
        Ok(Schema {
            fields,
            hash,
            sorted_idxs,
        })
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn sorted_idxs(&self) -> &[usize] {
        &self.sorted_idxs
    }

    /// Rename fields in the schema.
    /// Returns an error if the from and to slices are not the same length.
    /// Also returns an error if after renaming, there are duplicate field names.
    pub fn rename(&self, from: &[&str], to: &[&str]) -> Result<Self> {
        if from.len() != to.len() {
            return Err(anyhow!("from and to slices are not the same length"));
        }
        let mut fields = Vec::with_capacity(self.fields.len());
        for field in self.fields.iter() {
            let new_name = match from.iter().position(|&name| name == field.name()) {
                Some(idx) => to[idx],
                None => field.name(),
            };
            fields.push(Field::new(new_name.to_string(), field.dtype().clone()));
        }
        Ok(Schema::new(fields)?)
    }

    /// Returns the fields in a sorted order along with the index of the field in the schema.
    pub fn sorted_fields(&self) -> impl Iterator<Item = (usize, &Field)> {
        self.sorted_idxs.iter().map(|&idx| (idx, &self.fields[idx]))
    }

    pub fn dropnull(&self, names: &[&str]) -> Result<Self> {
        for name in names {
            if !self.names().contains(name) {
                return Err(anyhow!("{} not a field in Schema: {:?}", name, self));
            }
        }
        let new_fields = self
            .fields()
            .iter()
            .map(|f| {
                if names.contains(&f.name()) {
                    f.drop_nullable()
                } else {
                    Ok(f.clone())
                }
            })
            .collect::<Result<_>>()?;
        Self::new(new_fields)
    }

    /// Return the names of the fields in the schema.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.fields.iter().map(|f| f.name())
    }

    /// Find the index of a field by name in the schema if it exists.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        let result = self
            .sorted_idxs
            .binary_search_by_key(&name, |&idx| self.fields[idx].name());
        match result {
            Ok(idx) => Some(self.sorted_idxs[idx]),
            Err(_) => None,
        }
    }

    /// Get the type of a field by name in the schema if it exists.
    pub fn dtype_of(&self, field: &str) -> Option<&Type> {
        self.fields
            .iter()
            .find(|f| f.name() == field)
            .map(|f| f.dtype())
    }

    /// Find a field by name in the schema.
    pub fn find(&self, field: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name() == field)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn contains(&self, field: &Field) -> bool {
        self.fields.contains(field)
    }

    /// Project a schema to a subset of fields (or returns error if some fields are not present)
    /// The order of the fields is preserved.
    pub fn project(&self, fields: &[impl AsRef<str>]) -> Result<Self> {
        let mut new_fields = vec![];
        for f in fields {
            let field = self
                .find(f.as_ref())
                .ok_or_else(|| anyhow!("field {} not found", f.as_ref()))?;
            new_fields.push(field.clone());
        }
        Ok(Schema::new(new_fields)?)
    }

    /// Drop given fields from the schema (or returns error if some fields are not present).
    /// The order of the fields is preserved.
    pub fn strip(&self, dropped: &[&str]) -> Result<Self> {
        let mut new_fields = vec![];
        for f in self.fields() {
            if !dropped.contains(&f.name()) {
                new_fields.push(f.clone());
            }
        }
        Ok(Schema::new(new_fields)?)
    }

    /// Project a schema to a subset of fields (or returns error if some fields are not present)
    /// The order of the fields is preserved. Also returns error if types
    /// of selected fields don't match the types of the fields in the schema.
    pub fn project_fields(&self, fields: &[Field]) -> Result<Self> {
        let mut new_fields = vec![];
        for f in fields {
            let field = self
                .fields()
                .iter()
                .find(|x| x.name() == f.name() && x.dtype() == f.dtype())
                .ok_or_else(|| anyhow!("field {} not found", f.name()))?;
            new_fields.push(field.clone());
        }
        Ok(Schema::new(new_fields)?)
    }

    /// Append a field to the schema - no field with the same name must be present.
    pub fn append(&self, other: Field) -> Result<Self> {
        let mut new_fields = self.fields.clone();
        if self.find(other.name()).is_some() {
            return Err(anyhow!(
                "can not append field {} to schema: field already present",
                other.name()
            ));
        }
        new_fields.push(other);
        Ok(Schema::new(new_fields.into_vec())?)
    }

    /// Append a field to the schema - with field with same name exists then it
    /// replaces the existing field.
    pub fn upsert(&self, other: Field) -> Result<Self> {
        let mut new_fields = Vec::with_capacity(self.fields.len() + 1);
        let other_field_name = other.name();
        for field in self.fields.iter() {
            if field.name() != other_field_name {
                new_fields.push(field.clone())
            }
        }
        new_fields.push(other);
        Ok(Schema::new(new_fields)?)
    }

    /// Stitch two schemas together. The schemas must not have any fields in common.
    /// The fields of the other schema are appended to the fields of the current schema.
    pub fn stitch(&self, other: &Self) -> Result<Self> {
        let mut new_fields = self.fields.clone();
        for f in other.fields() {
            match self.find(f.name()) {
                Some(_) => {
                    return Err(anyhow!(
                        "can not stitch schemas: field {} present in both schemas",
                        f.name()
                    ));
                }
                None => new_fields.push(f.clone()),
            }
        }
        Ok(Schema::new(new_fields.into_vec())?)
    }

    pub fn hash(&self) -> u64 {
        self.hash
    }

    /// Hash the schema - the hash is guaranteed to be stable across
    /// runs/versions so can be stored in a database. The hash doesn't depend
    /// on the order of the fields.
    fn hash_fields(fields: impl Iterator<Item = impl AsRef<Field>>) -> u64 {
        let mut buf = vec![];
        for f in fields {
            let f = f.as_ref();
            buf.write_varint(f.name().len() as u64).unwrap();
            buf.extend_from_slice(f.name().as_bytes());
            f.dtype().encode(&mut buf).unwrap(); // unwrap is safe because writing to a vec can't fail
        }
        xxh3_64(&buf)
    }

    pub fn to_json_schema_string(&self) -> String {
        let mut parts = vec![];
        let sorted_fields = self.fields.iter().sorted_by(|a, b| a.name().cmp(b.name()));
        for f in sorted_fields {
            parts.push(format!(
                "\"{}\": \"{}\"",
                f.name,
                f.dtype.to_json_schema_type()
            ));
        }
        let mut ret = String::new();
        ret.push_str("{");
        ret.push_str(&parts.join(", "));
        ret.push_str("}");
        ret
    }

    pub fn encode(&self, mut writer: &mut dyn Write) -> Result<()> {
        let proto: ProtoSchema = self.into();
        let v = proto.encode_to_vec();
        writer.write_varint(v.len() as u32)?;
        writer.write_all(&v)?;
        Ok(())
    }

    pub fn decode(mut reader: &mut dyn Read) -> Result<Self> {
        let len: u32 = reader.read_varint()?;
        let mut buf = vec![0; len as usize];
        reader.read_exact(&mut buf)?;
        let proto: ProtoSchema = prost::Message::decode(buf.as_ref())?;
        proto.try_into()
    }

    pub fn permissive_schema(&self) -> Self {
        // It is safe to unwrap here because we know that the fields are valid.
        Schema::new(
            self.fields
                .iter()
                .map(|f| Field::new(f.name().to_string(), f.dtype().permissive_type()))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    pub fn get_fields_map(&self) -> HashMap<String, Type> {
        self.fields()
            .iter()
            .map(|f| (f.name().to_string(), f.dtype().clone()))
            .collect()
    }

    pub fn union(&self, other: &Self) -> Result<Self> {
        let mut fields = self.fields.to_vec();
        // Only add fields that are not already present
        for f in other.fields() {
            if !fields.contains(f) {
                fields.push(f.clone());
            }
        }
        Ok(Schema::new(fields)?)
    }
}

impl Eq for Schema {}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        if self.fields.len() != other.fields.len() {
            return false;
        }

        self.sorted_idxs
            .iter()
            .zip(other.sorted_idxs.iter())
            .all(|(&idx1, &idx2)| self.fields[idx1] == other.fields[idx2])
    }
}

/// DSSchema represents the schema of a dataset
/// It has a set of non-optional key fields, a timestamp field,
/// and a set of (possibly optional) value fields.
/// The only way to create a DSSchema is to use the `new` method which
/// will do all the necessary checks - hence DSSchema can be assumed
/// to be valid.
#[derive(Clone, Serialize, Deserialize, Eq)]
pub struct DSSchema {
    keys: Schema,
    ts: SmartString,
    vals: Schema,
    row_schema: Schema,
    erase_keys: Vec<SmartString>,
}
impl std::fmt::Debug for DSSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DSSchema {{")?;
        for k in self.keynames() {
            write!(f, "(key) {:?}: {:?}, ", k, self.keys.dtype_of(k).unwrap())?;
        }
        write!(f, "(timestamp) {:?}, ", self.tsname())?;
        for v in self.valnames() {
            write!(f, "(value) {:?}: {:?}, ", v, self.vals.dtype_of(v).unwrap())?;
        }
        write!(f, "}}")
    }
}

impl Hash for DSSchema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let keys_hash = self.keys.hash();
        keys_hash.hash(state);
        self.ts.hash(state);
        let vals_hash = self.vals.hash();
        vals_hash.hash(state);
    }
}

impl PartialEq for DSSchema {
    fn eq(&self, other: &Self) -> bool {
        self.keys == other.keys && self.ts == other.ts && self.vals == other.vals
    }
}
impl AsRef<DSSchema> for DSSchema {
    fn as_ref(&self) -> &Self {
        self
    }
}
impl AsRef<Schema> for Schema {
    fn as_ref(&self) -> &Schema {
        self
    }
}

impl DSSchema {
    pub fn new(keys: Vec<Field>, ts: impl Into<SmartString>, vals: Vec<Field>) -> Result<Self> {
        Self::new_with_erase_keys(keys, ts, vals, vec![])
    }

    pub fn keys(&self) -> &Schema {
        &self.keys
    }

    pub fn ts(&self) -> &SmartString {
        &self.ts
    }

    pub fn vals(&self) -> &Schema {
        &self.vals
    }

    pub fn new_with_erase_keys(
        keys: Vec<Field>,
        ts: impl Into<SmartString>,
        vals: Vec<Field>,
        erase_keys: Vec<String>,
    ) -> Result<Self> {
        let mut all = HashSet::new();
        let ts = ts.into();
        all.insert(ts.as_str());
        for f in &keys {
            if f.dtype().is_nullable() {
                return Err(anyhow!("key field {} cannot be optional", f.name()));
            }
            if all.contains(f.name()) {
                return Err(anyhow!("duplicate key field {}", f.name()));
            }
            all.insert(f.name());
        }

        for f in &vals {
            if all.contains(f.name()) {
                return Err(anyhow!("duplicate field {}", f.name()));
            }
            all.insert(f.name());
        }

        // we know that the schema is valid, so unwrap is safe
        let row_schema = Self::row_schema(&keys, &vals, ts.as_str()).unwrap();

        let key_schema = Schema::new(keys).context("Failed to create key schema")?;

        let erase_keys: Vec<SmartString> = erase_keys
            .into_iter()
            .map(|erase_key| SmartString::from(erase_key))
            .collect_vec();

        for erase_key_name in erase_keys.iter() {
            if key_schema.find(erase_key_name.as_str()).is_none() {
                return Err(anyhow!("erase key not found in key {}", &erase_key_name));
            }
        }

        Ok(DSSchema {
            keys: key_schema,
            ts,
            vals: Schema::new(vals)?,
            row_schema,
            erase_keys,
        })
    }

    pub fn keys_schema(&self) -> &Schema {
        &self.keys
    }

    pub fn values_schema(&self) -> &Schema {
        &self.vals
    }

    fn row_schema(key_fields: &[Field], val_fields: &[Field], tsname: &str) -> Result<Schema> {
        let mut row_fields = Vec::with_capacity(key_fields.len() + val_fields.len() + 1);
        for f in key_fields {
            row_fields.push(f.clone());
        }
        row_fields.push(Field::new(tsname, Type::Timestamp));
        for f in val_fields {
            row_fields.push(f.clone());
        }
        Schema::new(row_fields)
    }

    /// Returns a new DSSchema with all value fields made optional if they are not already
    pub fn with_optional_values(&self) -> Self {
        let mut val_fields = vec![];
        for f in self.valfields() {
            let field = match f.is_nullable() {
                true => f.clone(),
                false => Field::new(f.name().to_string(), Type::optional(f.dtype().clone())),
            };
            val_fields.push(field);
        }
        let row_schema =
            Self::row_schema(self.keys.fields(), &val_fields, self.ts.as_str()).unwrap();
        DSSchema {
            keys: self.keys.clone(),
            ts: self.ts.clone(),
            vals: Schema::new(val_fields).unwrap(), // can unwrap because we know the fields are valid
            row_schema,
            erase_keys: self.erase_keys.clone(),
        }
    }

    /// Returns true if the schema functionally matches the DSSchema
    /// (i.e. it has the same fields with the same types)
    pub fn matches(&self, schema: &Schema) -> bool {
        let other = schema.fields();
        if other.len() != self.keys.len() + self.vals.len() + 1 {
            return false;
        }
        for f in other {
            if self.keys.contains(f) || self.vals.contains(f) {
                continue;
            }
            if f.name() != self.ts || f.dtype() != &Type::Timestamp {
                return false;
            }
        }
        true
    }

    /// Returns true if the schema of the keys matches the given schema
    pub fn keys_match(&self, key_schema: &Schema) -> bool {
        return self.keys.eq(key_schema);
    }

    /// Returns true if the schema of the values matches the given schema
    pub fn values_match(&self, value_schema: &Schema) -> bool {
        return self.vals.eq(&value_schema);
    }

    /// Returns a (possibly empty) list of fields that are keys
    pub fn keyfields(&self) -> &[Field] {
        &self.keys.fields()
    }

    pub fn erase_keys(&self) -> &[SmartString] {
        &self.erase_keys
    }

    /// Returns a (possibly empty) list of fields that are erase keys
    pub fn erase_keyfields(&self) -> anyhow::Result<Schema> {
        Ok(Schema::new(
            self.keys.project(&self.erase_keys)?.fields.to_vec(),
        )?)
    }

    /// Returns a (possibly empty) list of fields that are values
    pub fn valfields(&self) -> &[Field] {
        &self.vals.fields()
    }

    /// Returns an iterator over the names of the key fields
    pub fn keynames(&self) -> impl Iterator<Item = &str> {
        self.keys.fields().iter().map(|f| f.name())
    }

    /// Returns an iterator over the names of the key fields
    pub fn non_erase_keynames(&self) -> Vec<&str> {
        let erase_keys = self
            .erase_keys()
            .iter()
            .map(|s| s.as_str())
            .collect::<HashSet<&str>>();
        self.keys
            .fields()
            .iter()
            .map(|f| f.name())
            .filter(|key| !erase_keys.contains(key))
            .collect_vec()
    }

    /// Returns an iterator over the names of the value fields
    pub fn valnames(&self) -> impl Iterator<Item = &str> {
        self.vals.fields().iter().map(|f| f.name())
    }

    /// Returns the name of the timestamp field
    pub fn tsname(&self) -> &str {
        &self.ts
    }

    /// Get the row schema for this DSSchema
    pub fn to_schema(&self) -> Schema {
        // TODO: This can be made more efficient by storing row_schema as an Arc<Schema> and
        // returning an Arc reference from here instead of cloning.
        self.row_schema.clone()
    }

    pub fn encode(&self, mut buf: &mut dyn Write) -> Result<()> {
        let proto: ProtoDSSchema = self.into();
        let encoded = proto.encode_to_vec();
        buf.write_varint(encoded.len() as u32)?; // write length of encoded proto (varint
        buf.write_all(&encoded)?;
        Ok(())
    }

    pub fn decode(mut buf: &mut dyn Read) -> Result<Self> {
        let len: u32 = buf.read_varint()?;
        let mut encoded = vec![0; len as usize];
        buf.read_exact(&mut encoded)?;
        let proto: ProtoDSSchema = ProtoDSSchema::decode(&encoded[..])?;
        proto.try_into()
    }

    pub fn contains_erase_key(&self) -> bool {
        self.erase_keys.len() > 0
    }
}

type ProtoField = fproto::schema::Field;
type ProtoSchema = fproto::schema::Schema;
type ProtoDSSchema = fproto::schema::DsSchema;

impl TryFrom<ProtoField> for Field {
    type Error = anyhow::Error;
    fn try_from(proto: ProtoField) -> Result<Self> {
        match proto {
            ProtoField {
                name,
                dtype: Some(dt),
            } => {
                let dtype = Type::try_from(dt)?;
                Ok(Field::new(name, dtype))
            }
            ProtoField { name, dtype: _ } => Err(anyhow!("missing dtype for field {name}")),
        }
    }
}

impl From<&Field> for ProtoField {
    fn from(field: &Field) -> Self {
        ProtoField {
            name: field.name().to_string(),
            dtype: Some(field.dtype().into()),
        }
    }
}

impl TryFrom<ProtoSchema> for Schema {
    type Error = anyhow::Error;
    fn try_from(proto: ProtoSchema) -> Result<Self> {
        let fields = proto
            .fields
            .into_iter()
            .map(|f| Field::try_from(f))
            .collect::<Result<Vec<_>>>()?;
        Schema::new(fields)
    }
}

impl From<&Schema> for ProtoSchema {
    fn from(schema: &Schema) -> Self {
        ProtoSchema {
            fields: schema.fields().iter().map(|f| f.into()).collect(),
        }
    }
}

impl TryFrom<ProtoDSSchema> for DSSchema {
    type Error = anyhow::Error;
    fn try_from(proto: ProtoDSSchema) -> Result<Self> {
        let ProtoDSSchema {
            keys,
            values,
            timestamp,
            erase_keys,
        } = proto;
        match (keys, values) {
            (Some(keys), Some(vals)) => {
                let key_schema: Schema = keys.try_into()?;
                let val_schema: Schema = vals.try_into()?;
                DSSchema::new_with_erase_keys(
                    key_schema.fields.into_vec(),
                    timestamp,
                    val_schema.fields.into_vec(),
                    erase_keys,
                )
            }
            _ => Err(anyhow!("missing keys or vals")),
        }
    }
}

impl From<&DSSchema> for ProtoDSSchema {
    fn from(dsschema: &DSSchema) -> Self {
        let keys = Some((&dsschema.keys).into());
        let erase_keys = dsschema
            .erase_keys
            .iter()
            .map(|key| key.to_string())
            .collect_vec();
        ProtoDSSchema {
            keys,
            values: Some((&dsschema.vals).into()),
            timestamp: dsschema.ts.as_str().into(),
            erase_keys,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::{Buf, BufMut, BytesMut};

    use crate::schema_proto::schema::OptionalType;
    use crate::testutils::DSSchemaBuilder;
    use crate::types::Type::*;
    use crate::types::{Between, CompiledRegex, OneOf};
    use crate::value::Value;

    use super::*;

    // checks in O(N^2) if two slices have contain the same elements ignoring order
    pub fn same_elements<T: PartialEq>(a: &[T], b: &[T]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for elem in a {
            let (mut count1, mut count2) = (0, 0);
            for other in a {
                if elem == other {
                    count1 += 1;
                }
            }
            for other in b {
                if elem == other {
                    count2 += 1;
                }
            }
            if count1 != count2 {
                return false;
            }
        }

        true
    }

    #[test]
    fn test_indexof_append_project() {
        let schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
        ])
        .unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.index_of("a").unwrap(), 0);
        assert_eq!(schema.index_of("b").unwrap(), 1);
        assert!(schema.index_of("c").is_none());
        same_elements(&vec!["a", "b"], &schema.names().collect_vec());

        let schema = schema.append(Field::new("c".to_string(), Int)).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.index_of("a").unwrap(), 0);
        assert_eq!(schema.index_of("b").unwrap(), 1);
        assert_eq!(schema.index_of("c").unwrap(), 2);
        same_elements(&vec!["a", "b", "c"], &schema.names().collect_vec());

        let schema = schema.project(&["a", "c"]).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.index_of("a").unwrap(), 0);
        assert!(schema.index_of("b").is_none());
        assert_eq!(schema.index_of("c").unwrap(), 1);
        same_elements(&vec!["a", "c"], &schema.names().collect_vec());

        // project using fields
        {
            let schema = schema
                .project_fields(&[
                    Field::new("a".to_string(), Int),
                    Field::new("c".to_string(), Int),
                ])
                .unwrap();
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.index_of("a").unwrap(), 0);
            assert!(schema.index_of("b").is_none());
            assert_eq!(schema.index_of("c").unwrap(), 1);
            same_elements(&vec!["a", "c"], &schema.names().collect_vec());
        }
    }

    #[test]
    fn test_schema_encode_decode() {
        let schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
        ])
        .unwrap();
        let mut buf = BytesMut::new();
        let mut writer = buf.writer();
        schema.encode(&mut writer).unwrap();
        buf = writer.into_inner();
        let encoded = buf.split().freeze();
        let schema2 = Schema::decode(&mut encoded.reader()).unwrap();
        assert_eq!(schema, schema2);

        // More complex schema
        let schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Optional(Box::new(Int))),
            Field::new("c".to_string(), List(Box::new(Int))),
            Field::new("d".to_string(), List(Box::new(Optional(Box::new(Int))))),
            Field::new("e".to_string(), Map(Box::new(Int))),
            Field::new("f".to_string(), Map(Box::new(Optional(Box::new(Int))))),
            Field::new("h".to_string(), Map(Box::new(List(Box::new(Int))))),
            Field::new(
                "i".to_string(),
                Map(Box::new(List(Box::new(Optional(Box::new(Int)))))),
            ),
        ]);
        let schema = schema.unwrap();
        let mut writer = buf.writer();
        schema.encode(&mut writer).unwrap();
        let encoded = writer.into_inner().split().freeze();
        let schema2 = Schema::decode(&mut encoded.reader()).unwrap();
        assert_eq!(schema, schema2);
    }

    #[test]
    fn test_find() {
        let schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
        ])
        .unwrap();
        assert_eq!(schema.find("a").unwrap(), &Field::new("a".to_string(), Int));
        assert_eq!(schema.find("b").unwrap(), &Field::new("b".to_string(), Int));
        assert!(schema.find("c").is_none());
    }

    #[test]
    fn test_field() {
        let f = Field::new("a".to_string(), Int);
        assert!(!f.is_nullable());
        assert_eq!(f.name(), "a");
        assert_eq!(f.dtype(), &Int);

        let f = Field::new("a".to_string(), Optional(Box::new(Int)));
        assert!(f.is_nullable());
        assert_eq!(f.name(), "a");
        assert_eq!(f.dtype(), &Optional(Box::new(Int)));
    }

    #[test]
    fn test_new() {
        Schema::new(vec![Field::new("a".to_string(), Int)]).unwrap();
        Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
        ])
        .unwrap();

        // can not have duplicate field names
        Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
            Field::new("a".to_string(), Float),
        ])
        .unwrap_err();

        Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
            Field::new("a".to_string(), Int),
        ])
        .unwrap_err();
        // can have empty schema
        Schema::new(vec![]).unwrap();
    }

    #[test]
    fn test_dsschema_basic() {
        let dsschema = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("c".to_string(), Int)],
        )
        .unwrap();
        assert_eq!(dsschema.keynames().collect::<Vec<_>>(), vec!["a", "b"]);
        assert_eq!(dsschema.valnames().collect::<Vec<_>>(), vec!["c"]);
        assert_eq!(dsschema.tsname(), "ts");
        let schema = dsschema.to_schema();
        assert_eq!(
            schema,
            Schema::new(vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
                Field::new("ts".to_string(), Timestamp),
                Field::new("c".to_string(), Int),
            ])
            .unwrap(),
        );
        assert!(dsschema.matches(&schema));

        assert_eq!(
            dsschema.keyfields(),
            &[
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool)
            ]
        );
        assert_eq!(dsschema.valfields(), &[Field::new("c".to_string(), Int)]);
    }

    #[test]
    fn test_dsschema_invalid() {
        let res = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("a".to_string(), Int)],
        );
        assert!(res.is_err());
        let res = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("ts".to_string(), Int)],
        );
        assert!(res.is_err());
        let res = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("a".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("b".to_string(), Int)],
        );
        assert!(res.is_err());
    }

    #[test]
    fn test_dsschema_non_erase_keyname() {
        let res = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("c".to_string(), Int)],
        );
        assert_eq!(res.unwrap().non_erase_keynames(), vec!["a", "b"]);
        let res = DSSchema::new_with_erase_keys(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("c".to_string(), Int)],
            vec!["a".to_string()],
        );
        assert_eq!(res.unwrap().non_erase_keynames(), vec!["b"]);
        let res = DSSchema::new_with_erase_keys(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![Field::new("c".to_string(), Int)],
            vec!["a".to_string(), "b".to_string()],
        );
        let empty_vec: Vec<&str> = vec![];
        assert_eq!(res.unwrap().non_erase_keynames(), empty_vec);
    }

    #[test]
    fn test_dsschema_equals() {
        let ds1 = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![
                Field::new("c".to_string(), Int),
                Field::new("d".to_string(), Float),
            ],
        )
        .unwrap();

        // same as ds1 but with different order of fields
        let ds2 = DSSchema::new(
            vec![
                Field::new("b".to_string(), Bool),
                Field::new("a".to_string(), Int),
            ],
            "ts".to_string(),
            vec![
                Field::new("d".to_string(), Float),
                Field::new("c".to_string(), Int),
            ],
        )
        .unwrap();
        assert_eq!(ds1.clone(), ds2.clone());

        // doesn't work when any of the fields are different in type
        let ds3 = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![
                Field::new("c".to_string(), Int),
                Field::new("d".to_string(), Int),
            ],
        )
        .unwrap();
        assert_ne!(ds1.clone(), ds3.clone());

        // doesn't work when any of the names are different
        let ds4 = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Bool),
            ],
            "ts".to_string(),
            vec![
                Field::new("c".to_string(), Int),
                Field::new("e".to_string(), Float),
            ],
        )
        .unwrap();
        assert_ne!(ds1.clone(), ds4.clone());

        // also doesn't work when any fields are missing
        let ds5 = DSSchema::new(
            vec![Field::new("a".to_string(), Int)],
            "ts".to_string(),
            vec![
                Field::new("c".to_string(), Int),
                Field::new("d".to_string(), Float),
            ],
        )
        .unwrap();
        assert_ne!(ds1.clone(), ds5.clone());

        let ds6 = DSSchema::new(
            vec![
                Field::new(
                    "a".to_string(),
                    Type::Between(
                        Between::new(Int, Value::Int(0), Value::Int(10), false, false).unwrap(),
                    ),
                ),
                Field::new(
                    "b".to_string(),
                    Type::OneOf(OneOf::new(Int, vec![Value::Int(1)]).unwrap()),
                ),
            ],
            "ts".to_string(),
            vec![
                Field::new(
                    "d".to_string(),
                    Type::OneOf(
                        OneOf::new(String, vec![Value::String(Arc::new("foo".to_string()))])
                            .unwrap(),
                    ),
                ),
                Field::new(
                    "e".to_string(),
                    Type::Regex(CompiledRegex::new("foo".to_string()).unwrap()),
                ),
                Field::new(
                    "f".to_string(),
                    Type::Between(
                        Between::new(Float, Value::Float(0.0), Value::Float(10.0), false, false)
                            .unwrap(),
                    ),
                ),
            ],
        )
        .unwrap();

        let ds47 = DSSchema::new(
            vec![
                Field::new("a".to_string(), Int),
                Field::new("b".to_string(), Int),
            ],
            "ts".to_string(),
            vec![
                Field::new("c".to_string(), String),
                Field::new("e".to_string(), String),
                Field::new("f".to_string(), Float),
            ],
        )
        .unwrap();
        assert_ne!(ds6.clone(), ds47.clone());
    }

    #[test]
    fn test_schema_keys_vals_match() {
        let schema = DSSchema::new(
            vec![
                Field::new("foo".to_string(), String),
                Field::new("foo2".to_string(), Int),
            ],
            "ts".to_string(),
            vec![
                Field::new("bar".to_string(), String),
                Field::new("bar2".to_string(), Float),
            ],
        )
        .expect("failed to create test schema");

        let schema2 = DSSchema::new(
            vec![
                Field::new("foo".to_string(), String),
                Field::new("foo2".to_string(), Int),
            ],
            "ts".to_string(),
            vec![Field::new("bar3".to_string(), Timestamp)],
        )
        .expect("failed to create test schema");

        let schema3 = DSSchema::new(
            vec![Field::new("key".to_string(), String)],
            "ts".to_string(),
            vec![
                Field::new("bar".to_string(), String),
                Field::new("bar2".to_string(), Float),
            ],
        )
        .expect("failed to create test schema");

        assert!(schema.keys_match(&schema2.keys));
        assert!(!schema.keys_match(&schema3.keys));

        assert!(!schema.values_match(&schema2.vals));
        assert!(schema.values_match(&schema3.vals));
    }

    #[test]
    fn test_project_field() {
        let schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
            Field::new("c".to_string(), Optional(Box::new(Float))),
        ])
        .unwrap();
        // error to project field if both name/type don't match
        let res = schema.project_fields(&[Field::new("a".to_string(), Float)]);
        assert!(res.is_err());
        // error if even one field doesn't match
        let res = schema.project_fields(&[
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Float),
        ]);
        assert!(res.is_err());
        // but works if all fields match
        let res = schema.project_fields(&[
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
        ]);
        assert!(res.is_ok());
    }

    #[test]
    fn test_hash() {
        let s1 = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
            Field::new("c".to_string(), Optional(Box::new(Float))),
        ])
        .unwrap();
        assert_eq!(s1.hash(), 8730445126568971767); // schema hash should also not change in absolute

        // same as s1 but with different order of fields
        let s2 = Schema::new(vec![
            Field::new("c".to_string(), Optional(Box::new(Float))),
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
        ])
        .unwrap();
        assert_eq!(s1.hash(), s2.hash());

        // doesn't work when any of the fields are different in type
        let s3 = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
            Field::new("c".to_string(), Optional(Box::new(Int))),
        ])
        .unwrap();
        assert_ne!(s1.hash(), s3.hash());

        // doesn't work when any of the names are different
        let s4 = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
            Field::new("d".to_string(), Optional(Box::new(Float))),
        ])
        .unwrap();
        assert_ne!(s1.hash(), s4.hash());
    }

    #[test]
    fn test_to_json_schema_string() -> Result<()> {
        let s = Schema::new(vec![
            Field::new("a".to_owned(), Int),
            Field::new("b".to_owned(), Float),
            Field::new("c".to_owned(), String),
            Field::new("d".to_owned(), Bool),
            Field::new("e".to_owned(), Timestamp),
            Field::new("f".to_owned(), Optional(Box::new(Float))),
            Field::new("g".to_owned(), List(Box::new(Int))),
            Field::new("h".to_owned(), Embedding(10)),
            Field::new("i".to_owned(), Map(Box::new(Float))),
        ])
        .unwrap();
        let schema_str = s.to_json_schema_string();

        #[derive(Deserialize)]
        struct JsonSchema(HashMap<std::string::String, std::string::String>);

        let json_schema: JsonSchema = serde_json::from_str(&schema_str)?;
        let expected: HashMap<&str, &str> = HashMap::from([
            ("a", "integer"),
            ("b", "number"),
            ("c", "string"),
            ("d", "boolean"),
            ("e", "string"),
            ("f", "number"),
            ("g", "array"),
            ("h", "array"),
            ("i", "object"),
        ]);
        for (k, v) in expected {
            assert_eq!(json_schema.0.get(k).map(|x| x.as_str()), Some(v));
        }

        Ok(())
    }

    #[test]
    fn test_field_schema_to_from_proto() {
        let fields = vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Float),
            Field::new("c".to_string(), String),
            Field::new("d".to_string(), Bool),
            Field::new("e".to_string(), Timestamp),
            Field::new("f".to_string(), Optional(Box::new(Float))),
            Field::new("g".to_string(), List(Box::new(Int))),
            Field::new("h".to_string(), Embedding(10)),
            Field::new("i".to_string(), Map(Box::new(Float))),
        ];
        for field in &fields {
            let proto: ProtoField = field.into();
            let field2 = Field::try_from(proto).unwrap();
            assert_eq!(field, &field2);
        }
        let schemas = vec![
            Schema::new(fields.clone()).unwrap(),
            Schema::new(vec![Field::new("a".to_string(), Int)]).unwrap(),
            Schema::new(vec![]).unwrap(),
        ];
        for schema in &schemas {
            let proto: ProtoSchema = schema.into();
            let schema2 = Schema::try_from(proto).unwrap();
            assert_eq!(schema, &schema2);
        }
        use crate::schema_proto::schema::{data_type::Dtype, DataType as ProtoType, IntType};
        let invalid_schema = ProtoSchema {
            fields: vec![
                ProtoField {
                    name: "a".to_string(),
                    dtype: Some(ProtoType {
                        dtype: Some(Dtype::IntType(IntType {})),
                    }),
                },
                ProtoField {
                    name: "a".to_string(),
                    dtype: Some(ProtoType {
                        dtype: Some(Dtype::IntType(IntType {})),
                    }),
                },
            ],
        };
        Schema::try_from(invalid_schema.clone()).unwrap_err();
    }

    #[test]
    fn test_dsschema_encode_decode() {
        let cases = [
            DSSchemaBuilder::new().build(),
            DSSchemaBuilder::new().build(),
        ];
        for case in &cases {
            let mut buf = vec![];
            case.encode(&mut buf).unwrap();
            let mut reader: &[u8] = &buf;
            let decoded = DSSchema::decode(&mut reader).unwrap();
            assert_eq!(case, &decoded);
        }
        // verify that it also works when we encode multiple dsschemas together
        let mut buf = vec![];
        for case in &cases {
            case.encode(&mut buf).unwrap();
        }
        let buf2 = buf.clone();
        let mut reader: &[u8] = &buf2;
        for (i, case) in cases.iter().enumerate() {
            let decoded = DSSchema::decode(&mut reader)
                .expect(&format!("failed to decode dsschema at index {}", i));
            assert_eq!(case, &decoded);
        }
    }

    #[test]
    fn test_dsschema_to_from_proto() {
        use crate::schema_proto::schema::{data_type::Dtype, DataType as ProtoType, IntType};
        let dsschemas = vec![
            DSSchema::new(
                vec![Field::new("a".to_string(), Int)],
                "ts".to_string(),
                vec![
                    Field::new("c".to_string(), Int),
                    Field::new("d".to_string(), Optional(Box::new(Float))),
                ],
            )
            .unwrap(),
            DSSchema::new(
                vec![Field::new("a".to_string(), Int)],
                "ts".to_string(),
                vec![Field::new("c".to_string(), Int)],
            )
            .unwrap(),
            DSSchema::new(vec![], "ts".to_string(), vec![]).unwrap(),
            DSSchema::new(
                vec![Field::new("a".to_string(), Int)],
                "ts".to_string(),
                vec![Field::new("c".to_string(), Int)],
            )
            .unwrap(),
            DSSchema::new_with_erase_keys(
                vec![Field::new("a".to_string(), Int)],
                "ts".to_string(),
                vec![Field::new("c".to_string(), Int)],
                vec!["a".to_string()],
            )
            .unwrap(),
        ];
        for dsschema in &dsschemas {
            let proto: ProtoDSSchema = dsschema.into();
            let dsschema2 = DSSchema::try_from(proto).unwrap();
            assert_eq!(dsschema, &dsschema2);
        }

        let invalid_dsschemas = vec![
            // None is not allowed in either keys or values
            ProtoDSSchema {
                keys: None,
                values: None,
                timestamp: "ts".to_string(),
                erase_keys: vec![],
            },
            ProtoDSSchema {
                keys: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "a".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::IntType(IntType {})),
                        }),
                    }],
                }),
                values: None,
                timestamp: "ts".to_string(),
                erase_keys: vec![],
            },
            ProtoDSSchema {
                keys: None,
                values: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "a".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::IntType(IntType {})),
                        }),
                    }],
                }),
                timestamp: "ts".to_string(),
                erase_keys: vec![],
            },
            // key has an optional type
            ProtoDSSchema {
                keys: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "a".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::OptionalType(Box::new(OptionalType {
                                of: Some(Box::new(ProtoType {
                                    dtype: Some(Dtype::IntType(IntType {})),
                                })),
                            }))),
                        }),
                    }],
                }),
                values: None,
                timestamp: "ts".to_string(),
                erase_keys: vec![],
            },
            // key and value have a field with the same name
            ProtoDSSchema {
                keys: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "a".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::OptionalType(Box::new(OptionalType {
                                of: Some(Box::new(ProtoType {
                                    dtype: Some(Dtype::IntType(IntType {})),
                                })),
                            }))),
                        }),
                    }],
                }),
                values: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "a".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::OptionalType(Box::new(OptionalType {
                                of: Some(Box::new(ProtoType {
                                    dtype: Some(Dtype::IntType(IntType {})),
                                })),
                            }))),
                        }),
                    }],
                }),
                timestamp: "ts".to_string(),
                erase_keys: vec![],
            },
            // Erase keys not in key fields
            ProtoDSSchema {
                keys: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "a".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::IntType(IntType {})),
                        }),
                    }],
                }),
                values: Some(ProtoSchema {
                    fields: vec![ProtoField {
                        name: "b".to_string(),
                        dtype: Some(ProtoType {
                            dtype: Some(Dtype::IntType(IntType {})),
                        }),
                    }],
                }),
                timestamp: "ts".to_string(),
                erase_keys: vec!["b".to_string()],
            },
        ];
        for invalid_dsschema in invalid_dsschemas {
            assert!(
                DSSchema::try_from(invalid_dsschema.clone()).is_err(),
                "{:?}",
                invalid_dsschema
            );
        }
    }

    #[test]
    fn test_dropnull() {
        let cases = vec![
            (
                Schema::new(vec![Field::new(
                    "nulla",
                    Type::Optional(Box::new(Type::Int)),
                )])
                .unwrap(),
                vec!["nulla"],
                Schema::new(vec![Field::new("nulla", Type::Int)]).unwrap(),
                true,
            ),
            (
                Schema::new(vec![
                    Field::new("nulla", Type::Optional(Box::new(Type::Int))),
                    Field::new("b", Type::String),
                ])
                .unwrap(),
                vec!["nulla"],
                Schema::new(vec![
                    Field::new("nulla", Type::Int),
                    Field::new("b", Type::String),
                ])
                .unwrap(),
                true,
            ),
            (
                Schema::new(vec![
                    Field::new("nulla", Type::Optional(Box::new(Type::Int))),
                    Field::new("b", Type::String),
                ])
                .unwrap(),
                vec!["nulla", "b"],
                Schema::new(vec![
                    Field::new("nulla", Type::Int),
                    Field::new("b", Type::String),
                ])
                .unwrap(),
                false,
            ),
            (
                Schema::new(vec![
                    Field::new("nulla", Type::Optional(Box::new(Type::Int))),
                    Field::new("b", Type::String),
                ])
                .unwrap(),
                vec!["c"],
                Schema::new(vec![
                    Field::new("nulla", Type::Int),
                    Field::new("b", Type::String),
                ])
                .unwrap(),
                false,
            ),
        ];

        for (input, names, expected, pass) in cases {
            let output = input.dropnull(&names);
            if pass {
                assert!(output.unwrap().eq(&expected))
            } else {
                assert!(output.is_err())
            }
        }
    }

    #[test]
    fn test_hash_is_immutable() {
        let schema = Schema::new(vec![
            Field::new("kite", Type::Int),
            Field::new("flies", Type::Bool),
            Field::new("in", Type::Timestamp),
            Field::new("the", Type::Int),
            Field::new("morning", Type::Float),
        ])
        .unwrap();
        assert_eq!(schema.hash(), 15710506858579836465);
    }

    #[test]
    fn test_upsert_schema() {
        let schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Bool),
            Field::new("c".to_string(), Optional(Box::new(Float))),
        ])
        .unwrap();

        // Appending a field which is not already contained
        {
            let field = Field::new("d".to_string(), Int);
            let schema = schema.upsert(field.clone()).unwrap();
            assert_eq!(schema.find(field.name()).unwrap(), &field);
        }

        // Appending a field which is already present should return the field with new dtype
        {
            let field = Field::new("d".to_string(), Float);
            let schema = schema.upsert(field.clone()).unwrap();
            assert_eq!(schema.find(field.name()).unwrap(), &field);
        }
    }

    #[test]
    fn test_union_schemas() {
        let schema1 = Schema::new(vec![Field::new("a".to_string(), Int)]).unwrap();
        let schema2 = Schema::new(vec![Field::new("b".to_string(), Int)]).unwrap();
        let schema = schema1.union(&schema2).unwrap();
        let expected = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
        ])
        .unwrap();
        assert_eq!(schema, expected);

        // Identical schemas should be idempotent
        let schema = schema.union(&schema).unwrap();
        assert_eq!(schema, expected);

        // Union with an empty schema should be idempotent
        let empty_schema = Schema::new(vec![]).unwrap();
        let schema = schema.union(&empty_schema).unwrap();
        assert_eq!(schema, expected);

        // Union with a superset should be idempotent
        let superset_schema = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
            Field::new("c".to_string(), Int),
        ])
        .unwrap();
        let schema = schema.union(&superset_schema).unwrap();
        assert_eq!(schema, superset_schema);

        // Union is commutative
        let schema = superset_schema.union(&schema).unwrap();
        assert_eq!(schema, superset_schema);

        // Some common fields should be merged
        let schema3 = Schema::new(vec![
            Field::new("c".to_string(), Int),
            Field::new("d".to_string(), Float),
        ])
        .unwrap();
        let schema = superset_schema.union(&schema3).unwrap();
        let expected = Schema::new(vec![
            Field::new("a".to_string(), Int),
            Field::new("b".to_string(), Int),
            Field::new("c".to_string(), Int),
            Field::new("d".to_string(), Float),
        ])
        .unwrap();
        assert_eq!(schema, expected);

        // Same name, different types should error
        let schema4 = Schema::new(vec![Field::new("a".to_string(), Float)]).unwrap();
        assert!(schema.union(&schema4).is_err());
    }
}
