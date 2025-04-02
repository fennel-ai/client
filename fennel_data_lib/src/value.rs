use bytes::Bytes;
use core::panic;
use prost_types::Timestamp;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Read, Write};
use std::ops::Deref;
use std::sync::Arc;

use crate::schema_proto as fproto;
use anyhow::{anyhow, bail, Context, Result};
use apache_avro::schema::NamesRef;
use apache_avro::types::Value as AvroValue;
use apache_avro::Schema as AvroSchema;
use base64::{engine::general_purpose, Engine as _};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use chrono::{DateTime, NaiveDateTime, TimeZone};
use chrono_tz::Tz;
use integer_encoding::{VarIntReader, VarIntWriter};
use itertools::{multizip, Itertools};
use num_bigint::BigInt;
use num_traits::cast::ToPrimitive;
use protofish::context::{Context as ProtoContext, ValueType};
use protofish::context::{MessageField, Multiplicity, ValueType as ProtobufValueType};
use protofish::decode::{MessageValue, PackedArray, UnknownValue, Value as ProtobufValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use smartstring::alias::String as SmartString;
use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use xxhash_rust::xxh3::xxh3_64;

use crate::types::Type;

type Duration = std::time::Duration;

/// Value represents any data in the system.
///
/// All the types that don't fit in 8 bytes are stored as Arc pointers - this
/// has two benefits:
/// 1. The size of the value struct is 9 bytes and hence it is efficient to
///    create vectors of values.
/// 2. Value is efficiently cloneable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    None,
    Int(i64),
    Float(f64),
    Bool(bool),
    String(Arc<String>),
    Bytes(Bytes),
    Timestamp(UTCTimestamp),
    Embedding(Arc<Vec<f64>>),
    List(Arc<List>),
    Map(Arc<Map>),
    Struct(Arc<Struct>),
    Decimal(Arc<Decimal>),
    Date(Date),
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::None => write!(f, "None"),
            Value::Int(i) => write!(f, "{}", i),
            Value::Float(ff) => write!(f, "{}", ff),
            Value::Bool(b) => write!(f, "{}", b),
            Value::String(s) => write!(f, "\"{}\"", s),
            Value::Timestamp(t) => write!(f, "{}", t.micros),
            Value::Embedding(e) => write!(f, "{:?}", e),
            Value::List(l) => write!(f, "[{}]", l.data.iter().map(|v| format!("{}", v)).join(",")),
            Value::Map(m) => write!(
                f,
                "{{{}}}",
                m.data
                    .iter()
                    .map(|(k, v)| format!("\"{}\":{}", k, v))
                    .join(",")
            ),
            Value::Struct(s) => write!(
                f,
                "{{{}}}",
                s.data
                    .iter()
                    .map(|(k, v)| format!("\"{}\":{}", k, v))
                    .join(",")
            ),
            Value::Decimal(d) => write!(f, "{:?}", d.as_ref()),
            Value::Date(d) => write!(f, "{:?}", d),
            Value::Bytes(b) => write!(f, "{:?}", b),
        }
    }
}

impl AsRef<Value> for Value {
    fn as_ref(&self) -> &Value {
        self
    }
}

// We manually overwrite the default implementation of PartialEq so that 2 Values Float(f64::NAN)
// are equal.
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => fp_approx_equal(*a, *b, 6),
            (Value::None, Value::None) => true,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Embedding(a), Value::Embedding(b)) => a == b,
            (Value::List(a), Value::List(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            (Value::Struct(a), Value::Struct(b)) => a == b,
            (Value::Decimal(a), Value::Decimal(b)) => Arc::ptr_eq(a, b) || a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

// checks if two floating points are approximately equal,
// i.e. a and b are both NaN or both within 10^-epsilon of each other
pub fn fp_approx_equal(a: f64, b: f64, epsilon: i32) -> bool {
    if a.is_nan() && b.is_nan() {
        return true;
    }
    // infinities are also equal
    if a.is_infinite() && b.is_infinite() {
        return true;
    }
    return (a - b).abs() < 10f64.powi(-epsilon);
}

/// Timestamp represents a timestamp in the system .
/// and is stored as a number of microseconds since the epoch in UTC.
/// It's the responsibility of the user to convert the timestamp
/// from the local timezone to UTC.
#[derive(Debug, PartialEq, Clone, PartialOrd, Copy, Serialize, Deserialize, Ord, Eq, Hash)]
pub struct UTCTimestamp {
    micros: i64,
}

impl From<prost_types::Timestamp> for UTCTimestamp {
    fn from(ts: prost_types::Timestamp) -> Self {
        let micros = ts.seconds * 1_000_000 + ts.nanos as i64 / 1_000;
        UTCTimestamp::from_micros(micros)
    }
}

impl TryFrom<UTCTimestamp> for chrono::DateTime<chrono::Utc> {
    type Error = anyhow::Error;

    fn try_from(value: UTCTimestamp) -> Result<Self, Self::Error> {
        chrono::DateTime::from_timestamp(
            value.micros() / 1_000_000,
            ((value.micros() % 1_000_000) * 1_000) as u32,
        )
        .ok_or(anyhow!("Failed to convert Timestamp to DateTime"))
    }
}

impl TryFrom<UTCTimestamp> for chrono::NaiveDateTime {
    type Error = anyhow::Error;

    fn try_from(value: UTCTimestamp) -> Result<Self, Self::Error> {
        let dt: chrono::DateTime<chrono::Utc> = value.try_into()?;
        Ok(dt.naive_utc())
    }
}

impl From<chrono::DateTime<chrono::Utc>> for UTCTimestamp {
    fn from(value: chrono::DateTime<chrono::Utc>) -> Self {
        UTCTimestamp::from_micros(
            value.timestamp() * 1_000_000 + i64::from(value.timestamp_subsec_micros()),
        )
    }
}

impl From<UTCTimestamp> for prost_types::Timestamp {
    fn from(ts: UTCTimestamp) -> Self {
        Self {
            seconds: ts.micros() / 1_000_000,
            nanos: ((ts.micros() % 1_000_000) * 1_000) as i32,
        }
    }
}

impl TryFrom<&Timestamp> for UTCTimestamp {
    type Error = anyhow::Error;

    fn try_from(value: &Timestamp) -> std::result::Result<Self, Self::Error> {
        Ok(UTCTimestamp {
            micros: value.seconds * 1_000_000 + value.nanos as i64 / 1_000,
        })
    }
}

impl UTCTimestamp {
    pub fn now() -> Result<Self> {
        Ok(UTCTimestamp::from(
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64,
        ))
    }

    pub fn from(micros: i64) -> Self {
        UTCTimestamp { micros }
    }

    pub fn from_str(ts: &str, format: &str) -> Result<UTCTimestamp> {
        let dt = NaiveDateTime::parse_from_str(ts, format)
            .map_err(|e| anyhow!("Failed to parse commit timestamp: {}", e))?;
        let dt = dt.and_utc();
        let micros = dt.timestamp() * 1_000_000 + dt.timestamp_subsec_micros() as i64;
        Ok(UTCTimestamp { micros })
    }

    pub fn from_secs(secs: i64) -> Self {
        UTCTimestamp {
            micros: secs * 1_000_000,
        }
    }

    pub fn from_millis(millis: i64) -> Self {
        UTCTimestamp {
            micros: millis * 1_000,
        }
    }
    pub fn from_micros(micros: i64) -> Self {
        UTCTimestamp { micros }
    }

    pub fn micros(&self) -> i64 {
        self.micros
    }

    pub fn millis(&self) -> i64 {
        self.micros / 1_000
    }

    pub fn seconds(&self) -> u32 {
        (self.micros / 1_000_000) as u32
    }

    pub fn add(&self, d: Duration) -> Self {
        Self::from_micros(self.micros() + d.as_micros() as i64)
    }

    /// sub returns a new timestamp with the given duration subtracted.
    ///
    /// If given duration is greater than the current timestamp, it returns ZERO.
    pub fn sub(&self, d: Duration) -> Self {
        if self.micros() < d.as_micros() as i64 {
            Self::from_micros(0)
        } else {
            Self::from_micros(self.micros() - d.as_micros() as i64)
        }
    }

    /// Returns the timestamp in the given timezone.
    /// This function goes through a few standard datetime formats
    /// and tries to parse the timestamp. If it fails, it returns
    /// the error.
    pub fn from_proto_parsed(v: ProtobufValue) -> Result<Self> {
        let converted_val = Value::from_protobuf_val_to_parent_type(v);
        match converted_val {
            ProtobufValue::Int64(t) => {
                if let Ok(ret) = Self::try_from_secs(t) {
                    return Ok(ret);
                }
                if let Ok(ret) = Self::try_from_millis(t) {
                    return Ok(ret);
                }
                if let Ok(ret) = Self::try_from_micros(t) {
                    return Ok(ret);
                }
                return Self::try_from_nanos(t);
            }
            ProtobufValue::String(s) => Self::try_from(s.as_str()),
            converted_val => Err(anyhow!("invalid timestamp {:?}", converted_val)),
        }
    }

    /// Returns the timestamp in the given timezone.
    /// This function goes through a few standard datetime formats
    /// and tries to parse the timestamp. If it fails, it returns
    /// the error.
    pub fn from_json_parsed(v: &JsonValue) -> Result<Self> {
        match v {
            JsonValue::Number(n) => {
                let n = n.as_i64();
                if n.is_none() {
                    return Err(anyhow!("timestamp is a float {:?}", n));
                }
                let t = n.unwrap();
                if let Ok(ret) = Self::try_from_secs(t) {
                    return Ok(ret);
                }
                if let Ok(ret) = Self::try_from_millis(t) {
                    return Ok(ret);
                }
                if let Ok(ret) = Self::try_from_micros(t) {
                    return Ok(ret);
                }
                return Self::try_from_nanos(t);
            }
            JsonValue::String(s) => Self::try_from(s.as_str()),
            _ => Err(anyhow!("invalid timestamp {:?}", v)),
        }
    }

    pub fn to_rfc3339(&self) -> String {
        let dt = chrono::Utc.timestamp_nanos(self.micros * 1_000);
        // format the timestamp as micros since we use micros as SoT and set `use_z` to
        // set the timezone as UTC
        //
        // this is useful for python `datetime` conversions
        dt.to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
    }

    fn to_json(&self) -> JsonValue {
        JsonValue::String(self.to_rfc3339())
    }

    pub fn try_from_secs(t: i64) -> Result<UTCTimestamp> {
        let now = chrono::Utc::now().timestamp();
        if t > now + 60 * 60 * 24 * 365 * 100 {
            return Err(anyhow!("timestamp {} is too far in the future", t));
        }
        Ok(UTCTimestamp {
            micros: t * 1_000_000,
        })
    }

    pub fn try_from_micros(t: i64) -> Result<UTCTimestamp> {
        let now = chrono::Utc::now().timestamp_micros();
        if t > now + 60 * 60 * 24 * 365 * 100 * 1_000_000 {
            return Err(anyhow!("timestamp {} is too far in the future", t));
        }
        Ok(UTCTimestamp { micros: t })
    }

    pub fn try_from_nanos(t: i64) -> Result<UTCTimestamp> {
        Ok(UTCTimestamp { micros: t / 1_000 })
    }

    pub fn try_from_millis(t: i64) -> Result<UTCTimestamp> {
        let now = chrono::Utc::now().timestamp_millis();
        if t > now + 60 * 60 * 24 * 365 * 100 * 1_000 {
            return Err(anyhow!("timestamp {} is too far in the future", t));
        }
        Ok(UTCTimestamp { micros: t * 1_000 })
    }

    fn try_from_date_str(s: &str) -> Result<Self> {
        let midnight = format!("{} 00:00:00+0000", s);
        let ts_fmt = "%T %z";

        let try_fmt = |date_fmt: &str| {
            chrono::DateTime::parse_from_str(&midnight, format!("{} {}", date_fmt, ts_fmt).as_str())
                .map(|dt| UTCTimestamp::from(dt.timestamp_micros()))
        };

        // try to parse using various date formats
        // NOTE: mm/dd/yyyy, dd/mm/yyyy are not supported due to ambiguity
        // TODO: we could add locale based support (%x) or follow the postgres model of having a
        //  configurable "mode" (YMD, DMY, MDY) for a true date type
        // These formats cover several of the formats supported by postgres:
        // https://tinyurl.com/pg-date
        try_fmt("%F") // ISO8601 format (YYYY-MM-DD)
            .or_else(|_| try_fmt("%B %d, %Y")) // January 08, 1999
            .or_else(|_| try_fmt("%b %d, %Y")) //  Jan 8, 1999
            .or_else(|_| try_fmt("%Y-%b-%d")) // 1999-Jan-08
            .or_else(|_| try_fmt("%b-%d-%Y")) // Jan-08-1999
            .or_else(|_| try_fmt("%d-%b-%Y")) // 08-Jan-1999
            .or_else(|_| try_fmt("%Y/%m/%d")) // 1999/01/08
            .or_else(|_| try_fmt("%Y%m%d")) // 19990108
            .or_else(|_| try_fmt("%Y.%j")) // 1999.008 - day of the year
            .map_err(|e| anyhow!("unable to parse date {:?}: {:?}", s, e))
    }
}

impl std::ops::Add<Duration> for UTCTimestamp {
    type Output = UTCTimestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        UTCTimestamp::from_micros(self.micros() + rhs.as_micros() as i64)
    }
}
impl std::ops::Sub<Duration> for UTCTimestamp {
    type Output = UTCTimestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        UTCTimestamp::from_micros(self.micros() - rhs.as_micros() as i64)
    }
}

impl TryFrom<&str> for UTCTimestamp {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        fn parse_from_rfc_3339_with_timezone_str(s: &str) -> anyhow::Result<UTCTimestamp> {
            let parts: Vec<&str> = s.rsplitn(2, ' ').collect();
            let timezone: Tz = parts[0].parse()?;
            let naive_datetime = NaiveDateTime::parse_from_str(parts[1], "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|e| anyhow!("Error parsing datetime: {}", e))?;
            let timestamp_micros = timezone
                .from_local_datetime(&naive_datetime)
                .single()
                .ok_or(anyhow!(
                    "Unable to add timezone: {} to datetime: {:?}",
                    timezone,
                    naive_datetime
                ))?
                .timestamp_micros();
            Ok(UTCTimestamp::from(timestamp_micros))
        }

        // Check for a date-only string
        if let Ok(ts) = UTCTimestamp::try_from_date_str(&s) {
            return Ok(ts);
        }

        // If the string is a number, parse it as time since epoch.
        if let Ok(ts) = s.parse::<i64>() {
            return Self::from_json_parsed(&JsonValue::Number(ts.into()));
        }
        // try to parse the timestamp in RFC3339 format
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
            return Ok(UTCTimestamp::from(dt.timestamp_micros()));
        }
        // try to parse the timestamp in RFC2822 format
        if let Ok(dt) = chrono::DateTime::parse_from_rfc2822(&s) {
            return Ok(UTCTimestamp::from(dt.timestamp_micros()));
        }
        // ISO8601 variant where a space is used instead of 'T'
        if let Ok(ndt) = chrono::DateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f%z") {
            return Ok(UTCTimestamp::from(ndt.timestamp_micros()));
        }

        // ISO8601 variant where a space is used instead of 'T' and timezone added with space
        if let Ok(timestamp) = parse_from_rfc_3339_with_timezone_str(&s) {
            return Ok(timestamp);
        }

        // JSON exported from Postgres TIMESTAMP (without TZ) columns have this format
        // NOTE: We use chrono::NativeDateTime instead of chrono::DateTime because the
        // latter assumes the timestamp is in UTC and will apply a timezone offset.
        if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%S%.f") {
            return Ok(UTCTimestamp::from(ndt.and_utc().timestamp_micros()));
        }
        // HACK for oslash: parse the timestamp for a specific format they use
        if let Ok(ndt) = NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S%.f") {
            let dt = DateTime::<chrono::Utc>::from_naive_utc_and_offset(ndt, chrono::Utc);
            return Ok(UTCTimestamp::from(dt.timestamp_micros()));
        }

        // if s ends with +00 or -00, then add 00 and try to parse it as a timestamp
        if s.ends_with("+00") || s.ends_with("-00") {
            let processed_s = format!("{}00", s);
            if let Ok(dt) =
                chrono::DateTime::parse_from_str(&processed_s, "%Y-%m-%d %H:%M:%S%.3f%z")
            {
                return Ok(UTCTimestamp::from(dt.to_utc().timestamp_micros()));
            }
        }
        Err(anyhow!("invalid timestamp {:?}", s))
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
pub struct List {
    dtype: Type,
    data: Vec<Value>,
}

impl List {
    pub fn new(dtype: Type, data: &[Value]) -> Result<Self> {
        if data.is_empty() {
            return Ok(List {
                dtype,
                data: Vec::new(),
            });
        }
        for (i, v) in data.iter().enumerate() {
            if !v.matches(&dtype) {
                return Err(anyhow!(
                    "expected list of type {:?} but element in position {} is {:?}, which is not compatible",
                    dtype,
                    i,
                    v
                ));
            }
        }
        Ok(List {
            dtype,
            data: data.to_vec(),
        })
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn dtype(&self) -> &Type {
        &self.dtype
    }

    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.data.iter()
    }

    pub fn values(&self) -> &Vec<Value> {
        &self.data
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct Map {
    dtype: Type,
    // we don't have to do any get/set operations on this map
    // so we can use a vector instead of a hashmap
    data: Vec<(String, Value)>,
}

impl Map {
    pub fn new(dtype: Type, data: &[(String, Value)]) -> Result<Self> {
        for (k, v) in data.iter() {
            if !v.matches(&dtype) {
                return Err(anyhow!(
                    "expected map of type {:?} but value for key {} is {:?}, which is not compatible",
                    dtype,
                    k,
                    v
                ));
            }
        }
        Ok(Map {
            dtype,
            data: data.to_vec(),
        })
    }

    pub fn dtype(&self) -> &Type {
        &self.dtype
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.data.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl Iterator<Item = &Value> {
        self.data.iter().map(|(_, v)| v)
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &(String, Value)> {
        self.data.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Struct {
    data: Vec<(SmartString, Value)>,
}

impl Struct {
    pub fn new(mut data: Vec<(SmartString, Value)>) -> Result<Self> {
        // Sort data by fields
        data.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        // Check for duplicate fields
        for i in 1..data.len() {
            if data[i - 1].0 == data[i].0 {
                return Err(anyhow!("duplicate field name {} in struct", data[i].0));
            }
        }
        Ok(Struct {
            data: data.to_vec(),
        })
    }

    pub fn num_fields(&self) -> usize {
        self.data.len()
    }

    pub fn data(&self) -> &Vec<(SmartString, Value)> {
        &self.data
    }

    pub fn field_names(&self) -> impl Iterator<Item = &SmartString> {
        self.data.iter().map(|(k, _)| k)
    }

    pub fn get(&self, field: &str) -> Option<&Value> {
        for (k, v) in self.data.iter() {
            if k == field {
                return Some(v);
            }
        }
        None
    }
    pub fn fields(&self) -> impl Iterator<Item = &(SmartString, Value)> {
        self.data.iter()
    }
}

/// Date represents a date in the system.
/// And is stored as number of days since UNIX epoch 1970-01-01. There's no concept of timezone to date,
/// that's why we won't be doing conversion from datetime to date ourselves, user can still do this
/// using transform functions. However, now method for date will be timezone aware -> system time will be
/// used to calculate that. Idea behind not doing conversion is suppose there's timestamp 2024-01-02 04:30 IST,
/// now the date will be 2024-01-01 in UTC but 2024-01-02 in IST, which date to store to avoid the confusion
/// we are not supporting the conversion
#[derive(Debug, PartialEq, Clone, PartialOrd, Copy, Serialize, Deserialize, Ord, Eq, Hash)]
pub struct Date {
    days: i64,
}

impl Date {
    pub fn now() -> Result<Self> {
        let system_time = SystemTime::now();
        let date_time: chrono::DateTime<chrono::Local> = system_time.into();
        let naive_date_time = date_time.naive_local();
        let naive_date = chrono::NaiveDate::from(naive_date_time);
        Self::from_naive_date(naive_date)
    }

    /// Days since epoch
    pub fn from(days: i64) -> Self {
        Date { days }
    }

    /// Milli Seconds since epoch
    pub fn from_milli_secs(milli_secs: i64) -> Self {
        Date {
            days: milli_secs / 24 / 60 / 60 / 1000,
        }
    }

    /// Returns the date.
    /// This function goes through a few standard date formats
    /// and tries to parse the date. If it fails, it returns
    /// the error.
    pub fn from_proto_parsed(v: ProtobufValue) -> Result<Self> {
        let converted_val = Value::from_protobuf_val_to_parent_type(v);
        match converted_val {
            ProtobufValue::Int64(val) => {
                return Ok(Self::from(val));
            }
            ProtobufValue::String(s) => Self::try_from(s.as_str()),
            converted_val => Err(anyhow!("invalid date {:?}", converted_val)),
        }
    }

    /// Returns the date.
    /// This function goes through a few standard date formats
    /// and tries to parse the date. If it fails, it returns
    /// the error.
    pub fn from_json_parsed(v: &JsonValue) -> Result<Self> {
        match v {
            JsonValue::Number(n) => {
                let n = n.as_i64();
                if n.is_none() {
                    return Err(anyhow!("date is a float {:?}", n));
                }
                let t = n.unwrap();

                if let Ok(ret) = Self::try_from_days(t) {
                    return Ok(ret);
                }
                if let Ok(ret) = Self::try_from_secs(t) {
                    return Ok(ret);
                }
                if let Ok(ret) = Self::try_from_millis(t) {
                    return Ok(ret);
                }
                return Self::try_from_micros(t);
            }
            JsonValue::String(s) => Self::try_from(s.as_str()),
            _ => Err(anyhow!("invalid date {:?}", v)),
        }
    }

    fn to_naive_date(&self) -> chrono::NaiveDate {
        let unix_naive_date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = self.days();

        if days < 0 {
            (&unix_naive_date)
                .checked_sub_days(chrono::Days::new((days * -1) as u64))
                .unwrap()
        } else {
            (&unix_naive_date)
                .checked_add_days(chrono::Days::new(days as u64))
                .unwrap()
        }
    }

    /// String in format YYYY-DD-MM
    fn to_json(&self) -> JsonValue {
        JsonValue::String(self.to_naive_date().to_string())
    }

    pub fn days(&self) -> i64 {
        self.days
    }

    pub fn add(&self, days: i64) -> Self {
        Self::from(self.days() + days)
    }

    /// sub returns a new date with the given days subtracted.
    ///
    /// If given days is greater than the current date, it returns ZERO.
    pub fn sub(&self, days: i64) -> Self {
        if self.days() < days {
            Self::from(0)
        } else {
            Self::from(self.days() - days)
        }
    }

    pub fn from_naive_date(naive_date: chrono::NaiveDate) -> Result<Self> {
        let unix_naive_date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
            .ok_or_else(|| anyhow!("Failed to get unix date 1970-01-01 in naive date"))?;
        let days = naive_date.signed_duration_since(unix_naive_date).num_days();
        Ok(Date { days })
    }

    pub fn try_from_micros(d: i64) -> Result<Date> {
        let now = chrono::Utc::now().timestamp_micros();
        if d > now + 1_000_000 * 60 * 60 * 24 * 365 * 100 {
            bail!("date timestamp {} is too far in the future", d);
        }
        Ok(Date {
            days: d / 24 / 60 / 60 / 1_000_000,
        })
    }

    pub fn try_from_millis(d: i64) -> Result<Date> {
        let now = chrono::Utc::now().timestamp_millis();
        if d > now + 1_000 * 60 * 60 * 24 * 365 * 100 {
            bail!("date timestamp {} is too far in the future", d);
        }
        Ok(Date {
            days: d / 24 / 60 / 60 / 1_000,
        })
    }

    pub fn try_from_secs(d: i64) -> Result<Date> {
        let now = chrono::Utc::now().timestamp();
        if d > now + 60 * 60 * 24 * 365 * 100 {
            bail!("date timestamp {} is too far in the future", d);
        }
        Ok(Date {
            days: d / 24 / 60 / 60,
        })
    }

    pub fn try_from_days(d: i64) -> Result<Date> {
        let now = chrono::Utc::now().timestamp() / 60 / 60 / 24;
        if d > now + 365 * 100 {
            bail!("date timestamp {} is too far in the future", d);
        }
        Ok(Date { days: d })
    }
}

impl TryFrom<&str> for Date {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        // try to parse the date in  19990108 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y%m%d") {
            return Date::from_naive_date(dt);
        }

        // If the string is a number, parse it as days since epoch.
        if let Ok(ts) = s.parse::<i64>() {
            return Self::from_json_parsed(&JsonValue::Number(ts.into()));
        }

        // try to parse the date in January 08, 1999 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%B %d, %Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in Jan 8, 1999 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%b %d, %Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in Jan-08-1999 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%b-%d-%Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in Feb172009 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%b%d%Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 08 January, 1999 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%d %B, %Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 08 Jan, 1999 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%d %b, %Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 08-Jan-1999 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%d-%b-%Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 17Feb2009 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%d%b%Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 2009, February 17 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y, %B %d") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 2009, Feb 17 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y, %b %d") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 1999-Jan-08 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y-%b-%d") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 1999Jan08 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y%b%d") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in  1999/01/08 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y/%m/%d") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in  1999-01-08 format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 1999.008 - day of the year format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y.%j") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 48/2009 - day of the year format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%j/%Y") {
            return Date::from_naive_date(dt);
        }

        // try to parse the date in 1999/48 - day of the year format
        if let Ok(dt) = chrono::NaiveDate::parse_from_str(&s, "%Y/%j") {
            return Date::from_naive_date(dt);
        }

        Err(anyhow!("invalid date {:?}", s))
    }
}

const CODEC: u8 = 1;

impl Value {
    /// Returns true if the value matches the type.
    pub fn matches(&self, dtype: &Type) -> bool {
        match (self, dtype) {
            (Value::None, Type::Null) => true,
            (Value::None, Type::Optional(_)) => true,
            (v, Type::Optional(t)) => v.matches(t),
            (Value::Int(_), Type::Int) => true,
            (Value::Float(_), Type::Float) => true,
            (Value::String(_), Type::String) => true,
            (Value::Bool(_), Type::Bool) => true,
            (Value::Timestamp(_), Type::Timestamp) => true,
            (Value::Bytes(_), Type::Bytes) => true,
            (Value::Date(_), Type::Date) => true,
            (Value::Embedding(v), Type::Embedding(dim)) => v.len() == *dim,
            (Value::List(l), Type::Embedding(dim)) => {
                if l.data.len() != *dim {
                    return false;
                }
                if l.dtype == Type::Float {
                    return true;
                }

                l.values().iter().all(|v| v.matches(&Type::Float))
            }
            (Value::List(l), Type::List(t)) => {
                if l.dtype == *t.as_ref() {
                    return true;
                }
                l.values().iter().all(|v| v.matches(t.as_ref()))
            }
            (Value::Map(m), Type::Map(t)) => {
                if m.dtype == *t.as_ref() {
                    return true;
                }
                m.data.iter().all(|(_k, v)| v.matches(t.as_ref()))
            }
            (Value::Float(f), Type::Between(b)) => {
                if *b.dtype() != Type::Float {
                    return false;
                }
                let min_thresh = b.min().as_float();
                if min_thresh.is_err() {
                    return false;
                }
                let min_thresh = min_thresh.unwrap();
                let max_thresh = b.max().as_float();
                if max_thresh.is_err() {
                    return false;
                }
                let max_thresh = max_thresh.unwrap();
                if *f < min_thresh
                    || *f > max_thresh
                    || ((*f - min_thresh).abs() < f64::EPSILON && b.strict_min())
                    || ((*f - max_thresh).abs() < f64::EPSILON && b.strict_max())
                {
                    return false;
                }
                return true;
            }
            (Value::Int(i), Type::Between(b)) => {
                if *b.dtype() != Type::Int {
                    return false;
                }
                let min_thresh = b.min().as_int();
                if min_thresh.is_err() {
                    return false;
                }
                let min_thresh = min_thresh.unwrap();
                let max_thresh = b.max().as_int();
                if max_thresh.is_err() {
                    return false;
                }
                let max_thresh = max_thresh.unwrap();
                if i < &min_thresh
                    || i > &max_thresh
                    || (i == &min_thresh && b.strict_min())
                    || (i == &max_thresh && b.strict_max())
                {
                    return false;
                }
                return true;
            }
            (Value::Int(int), Type::OneOf(o)) => {
                if *o.dtype() != Type::Int {
                    return false;
                }
                let options = o.values();
                for i in 0..options.len() {
                    let int_val = options[i].as_int();
                    if int_val.is_ok() && int_val.unwrap() == *int {
                        return true;
                    }
                }
                return false;
            }
            (Value::String(s), Type::OneOf(o)) => {
                if *o.dtype() != Type::String {
                    return false;
                }
                let options = o.values();
                for i in 0..options.len() {
                    let str_val = options[i].as_str();
                    if str_val.is_ok() && str_val.unwrap() == s.as_str() {
                        return true;
                    }
                }
                return false;
            }
            (Value::String(s), Type::Regex(r)) => r.is_match(s.as_ref()),
            (Value::Struct(s), Type::Struct(t)) => {
                // Iterate parallely over the fields and the values of the current row, since both
                // are sorted by name.
                let mut value_struct_index = 0;
                let mut type_index = 0;
                while type_index < t.fields().len() {
                    let type_field = t.fields().get(type_index).unwrap();
                    if value_struct_index >= s.data().len() {
                        if type_field.dtype().is_nullable() {
                            type_index += 1;
                            continue;
                        }
                        return false;
                    }
                    let value_field = s.data().get(value_struct_index).unwrap();
                    if type_field.name() == value_field.0 {
                        if !value_field.1.matches(type_field.dtype()) {
                            return false;
                        }
                        type_index += 1;
                        value_struct_index += 1;
                    } else if type_field.name() < &value_field.0 as &str {
                        // Skipping a field in the type that is not present in the value.
                        if !type_field.dtype().is_nullable() {
                            return false;
                        }
                        type_index += 1;
                    } else {
                        // Value field has a field that is not present in the type.
                        // We now allow this, since we allow the user to add new fields to the value but not to the type.
                        value_struct_index += 1;
                    }
                }
                return true;
            }
            (Value::Decimal(v), Type::Decimal(t)) => v.scale() == t.scale(),
            _ => false,
        }
    }

    // Similar to matches, but returns a Result
    pub fn try_matches(&self, dtype: &Type) -> Result<()> {
        if self.matches(dtype) {
            Ok(())
        } else {
            Err(anyhow!(
                "value {:?} does not match between type {:?}",
                self,
                dtype,
            ))
        }
    }

    /// Converts primitive type values into the largest value under same type
    /// Integer types are converted to Int64 and Float types are converted into Double
    fn from_protobuf_val_to_parent_type(val: ProtobufValue) -> ProtobufValue {
        match val {
            ProtobufValue::Int32(val)
            | ProtobufValue::SInt32(val)
            | ProtobufValue::SFixed32(val) => ProtobufValue::Int64(val as i64),
            ProtobufValue::SInt64(val) | ProtobufValue::SFixed64(val) => ProtobufValue::Int64(val),
            ProtobufValue::UInt32(val) | ProtobufValue::Fixed32(val) => {
                ProtobufValue::Int64(val as i64)
            }
            ProtobufValue::UInt64(val) | ProtobufValue::Fixed64(val) => {
                ProtobufValue::Int64(val as i64)
            }
            ProtobufValue::Float(val) => ProtobufValue::Double(val as f64),
            ProtobufValue::Packed(PackedArray::Int32(data))
            | ProtobufValue::Packed(PackedArray::SInt32(data))
            | ProtobufValue::Packed(PackedArray::SFixed32(data)) => {
                let data = data.into_iter().map(|d| d as i64).collect_vec();
                ProtobufValue::Packed(PackedArray::Int64(data))
            }
            ProtobufValue::Packed(PackedArray::SInt64(data))
            | ProtobufValue::Packed(PackedArray::SFixed64(data)) => {
                let data = data.into_iter().map(|d| d).collect_vec();
                ProtobufValue::Packed(PackedArray::Int64(data))
            }
            ProtobufValue::Packed(PackedArray::UInt32(data))
            | ProtobufValue::Packed(PackedArray::Fixed32(data)) => {
                let data = data.into_iter().map(|d| d as i64).collect_vec();
                ProtobufValue::Packed(PackedArray::Int64(data))
            }
            ProtobufValue::Packed(PackedArray::UInt64(data))
            | ProtobufValue::Packed(PackedArray::Fixed64(data)) => {
                let data = data.into_iter().map(|d| d as i64).collect_vec();
                ProtobufValue::Packed(PackedArray::Int64(data))
            }
            ProtobufValue::Packed(PackedArray::Float(data)) => {
                let data = data.into_iter().map(|d| d as f64).collect_vec();
                ProtobufValue::Packed(PackedArray::Double(data))
            }
            _ => val,
        }
    }

    /// Parses a json string into a value of the given type
    pub fn from_json(dtype: &Type, s: &str) -> Result<Self> {
        Self::from_json_parsed(dtype, &serde_json::from_str(s)?)
            .map_err(|e| anyhow!("Error parsing json str {s} as type {dtype:?}: {e:?}"))
    }

    /// Parses a json value into a value of the given type - except json has
    /// already been parsed into a serde_json::Value. This is useful for cases
    /// when parsing many fields out of JSON out of which one or more are Values
    pub fn from_json_parsed(dtype: &Type, parsed: &JsonValue) -> Result<Self> {
        match (dtype, parsed) {
            (Type::Null, JsonValue::Null) => Ok(Value::None),
            (Type::Int, JsonValue::Number(n)) => match n.as_i64() {
                Some(v) => Ok(Value::Int(v)),
                None => Err(anyhow!("expected int, but got float {:?}", n)),
            },
            (Type::Float, JsonValue::Number(n)) => match n.as_f64() {
                Some(v) => Ok(Value::Float(v)),
                None => Err(anyhow!("expected float, but got int {:?}", n)),
            },
            (Type::Float, JsonValue::String(s)) if s.to_lowercase().eq("nan") => {
                Ok(Value::Float(f64::NAN))
            }
            (Type::Float, JsonValue::String(s))
                if s.to_lowercase().eq("+inf")
                    || s.to_lowercase().eq("+infinity")
                    || s.to_lowercase().eq("inf")
                    || s.to_lowercase().eq("infinity") =>
            {
                Ok(Value::Float(f64::INFINITY))
            }
            (Type::Float, JsonValue::String(s))
                if s.to_lowercase().eq("-inf") || s.to_lowercase().eq("-infinity") =>
            {
                Ok(Value::Float(f64::NEG_INFINITY))
            }
            (Type::Decimal(d), JsonValue::Number(n)) => match n.as_f64() {
                Some(n) => Ok(Value::Decimal(Arc::new(
                    Decimal::try_from(n)?.trunc_with_scale(d.scale()),
                ))),
                None => Err(anyhow!("expected decimal, but got int {:?}", n)),
            },
            (Type::Bytes, JsonValue::String(s)) => {
                // Base64 decode the string
                let decoded = general_purpose::STANDARD.decode(s.as_bytes())?;
                Ok(Value::Bytes(Bytes::from(decoded)))
            }
            (Type::Bool, JsonValue::Bool(b)) => Ok(Value::Bool(*b)),
            (Type::Timestamp, v) => Ok(Value::Timestamp(UTCTimestamp::from_json_parsed(v)?)),
            (Type::Date, v) => Ok(Value::Date(Date::from_json_parsed(v)?)),
            (Type::Optional(_), JsonValue::Null) => Ok(Value::None),
            // Optional(String) types require special handling since we need to deal with the empty
            // string case. Our convention is that an empty string remains as if the type is Optional(String).
            // However, for all other optional types, we treat the empty string as None.
            (Type::Optional(t), JsonValue::String(s)) => match t.as_ref() {
                Type::String => Ok(Value::String(Arc::new(s.clone()))),
                _ => match s.as_str() {
                    "" => Ok(Value::None),
                    _v => Value::from_json_parsed(t, parsed),
                },
            },
            (Type::Optional(t), v) => Value::from_json_parsed(t.as_ref(), v),
            (Type::Embedding(n), JsonValue::Array(elements)) => {
                if n != &elements.len() {
                    return Err(anyhow!(
                        "expected embedding of length {}, but got {}",
                        n,
                        elements.len()
                    ));
                }
                let v: Vec<f64> = elements
                    .into_iter()
                    .map(|v| match v {
                        JsonValue::Number(n) => match n.as_f64() {
                            Some(v) => Ok(v),
                            None => Err(anyhow!("expected floats for embedding, but got {:?}", n)),
                        },
                        _ => Err(anyhow!("expected floats for embedding, but got {:?}", v)),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Value::Embedding(Arc::new(v)))
            }
            (Type::List(t), JsonValue::Array(elems)) => {
                let v: Vec<Value> = elems
                    .into_iter()
                    .map(|v| {
                        Value::from_json_parsed(t.as_ref(), v).with_context(|| {
                            anyhow!("Failed to parse list element of type {}, value {}", t, v)
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let list = List::new(t.as_ref().clone(), &v)?;
                Ok(Value::List(Arc::new(list)))
            }
            (Type::Map(t), JsonValue::Object(fields)) => {
                let v = fields
                    .into_iter()
                    .map(|(k, v)| {
                        Ok((
                            k.clone(),
                            Value::from_json_parsed(t.as_ref(), v).with_context(|| {
                                anyhow!("Failed to parse field {} of type {}", k, t)
                            })?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let map = Map::new(t.as_ref().clone(), v.as_slice())?;
                Ok(Value::Map(Arc::new(map)))
            }
            (Type::Between(b), JsonValue::Number(n)) => {
                if *b.dtype() == Type::Int {
                    let val = match n.as_i64() {
                        Some(v) => v,
                        None => return Err(anyhow!("expected int, but got float {:?}", n)),
                    };
                    let v = Value::Int(val);
                    v.try_matches(dtype).map(|_| v)
                } else {
                    let val = match n.as_f64() {
                        Some(v) => v,
                        None => return Err(anyhow!("expected float, but got int {:?}", n)),
                    };
                    let v = Value::Float(val);
                    v.try_matches(dtype).map(|_| v)
                }
            }
            (Type::OneOf(of), v) => {
                let options = of.values();
                if *of.dtype() == Type::Int {
                    let int = match v {
                        JsonValue::Number(n) => match n.as_i64() {
                            Some(v) => v,
                            None => return Err(anyhow!("expected int, but got float {:?}", n)),
                        },
                        _ => return Err(anyhow!("expected int, but got {:?}", v)),
                    };
                    if Value::Int(int).matches(dtype) {
                        return Ok(Value::Int(int));
                    }
                    Err(anyhow!("expected int in {:?}, but got {}", options, int))
                } else {
                    let str = match v {
                        JsonValue::String(s) => s,
                        _ => return Err(anyhow!("expected string, but got {:?}", v)),
                    };
                    if Value::String(Arc::new(str.clone())).matches(dtype) {
                        return Ok(Value::String(Arc::new(str.clone())));
                    }
                    Err(anyhow!("expected string in {:?}, but got {}", options, str))
                }
            }
            (Type::Struct(s), JsonValue::Object(fields)) => {
                let mut values: Vec<(SmartString, Value)> = Vec::with_capacity(s.fields().len());
                // For every field defined in the struct type, check if the field is present in the json. If it is,
                // parse the value and add it to the struct.
                for field in s.fields() {
                    let ftype = field.dtype();
                    let name = field.name();
                    let value = match fields.get(name) {
                        Some(v) => Value::from_json_parsed(ftype, v).with_context(|| {
                            format!(
                                "Failed to parse field {} in struct {:?}, value {:?}",
                                name,
                                s.name(),
                                v
                            )
                        })?,
                        None => {
                            if !ftype.is_nullable() {
                                return Err(anyhow!(
                                    "field {} not found in struct {:?}, defined fields {:?}",
                                    name,
                                    s.name(),
                                    s.fields()
                                ));
                            }
                            Value::None
                        }
                    };
                    values.push((name.into(), value));
                }
                let struct_ = Struct::new(values)?;
                Ok(Value::Struct(Arc::new(struct_)))
            }
            // Note: we keep this as the last entry to give other match arms priority.
            (_, JsonValue::String(s)) => match dtype {
                Type::String => Ok(Value::String(Arc::new(s.clone()))),
                Type::Regex(r) => {
                    if Value::String(Arc::new(s.clone())).matches(dtype) {
                        return Ok(Value::String(Arc::new(s.clone())));
                    }
                    Err(anyhow!(
                        "expected regex string {} to match, but got {}",
                        r,
                        s
                    ))
                }
                _ => Value::from_json(dtype, s),
            },
            // If its a string and json object, stringify the json object
            (Type::String, JsonValue::Object(_fields)) => {
                let s = serde_json::to_string(parsed)?;
                Ok(Value::String(Arc::new(s)))
            }
            _ => Err(anyhow!("expected {:?}, but got {:?}", dtype, parsed)),
        }
    }

    /// Parse google.protobuf.Timestamp type decoded as protofish::decode::Value to Fennel's timestamp value
    fn from_google_protobuf_timestamp_parsed(
        schema_field: &str,
        dtype: &Type,
        context: &ProtoContext,
        parsed: Option<Vec<ProtobufValue>>,
        multiplicity: &Multiplicity,
    ) -> Result<Self> {
        match dtype {
            Type::List(t) => match multiplicity {
                Multiplicity::Repeated => match parsed {
                    None => Ok(Value::List(Arc::new(
                        List::new(t.as_ref().clone(), &[]).unwrap(),
                    ))),
                    Some(parsed) => {
                        let mut values = Vec::with_capacity(parsed.len());
                        for proto_value in parsed {
                            let value = Self::from_google_protobuf_timestamp_parsed(
                                schema_field,
                                t.as_ref(),
                                context,
                                Some(vec![proto_value]),
                                &Multiplicity::Single,
                            )?;
                            values.push(value);
                        }
                        Ok(Value::List(Arc::new(
                            List::new(t.as_ref().clone(), &values).unwrap(),
                        )))
                    }
                },
                _ => {
                    return Err(anyhow!(
                        "Expected Repeated multiplicity for field: {:?} of type List[google.protobuf.Timestamp], found: {:?}",
                        schema_field,
                        multiplicity
                    ));
                }
            },
            Type::Optional(t) => {
                match t.as_ref() {
                    Type::Timestamp => {
                        // TODO(Harsha): Porter is observing issues due to which we removed multiplicity check here
                        match parsed {
                            None => Ok(Value::None),
                            Some(parsed) => Self::from_google_protobuf_timestamp_parsed(
                                schema_field,
                                t.as_ref(),
                                context,
                                Some(parsed),
                                &Multiplicity::Single,
                            ),
                        }
                    }
                    dtype => {
                        return Err(anyhow!(
                            "Expected Optional[Timestamp] field: {} for google.protobuf.Timestamp… \
                            type but found: Optional[{:?}]",
                            schema_field,
                            dtype
                        ))
                    }
                }
            }
            Type::Timestamp => {
                // Multiple entries not possible
                if parsed.is_none() {
                    return Err(anyhow!(
                        "Only single entry can be parsed for google.protobuf.Timestamp type \
                        corresponding to field:{} but found {:?}",
                        schema_field,
                        parsed
                    ));
                }
                let mut parsed = parsed.unwrap();
                if parsed.len() > 1 {
                    return Err(anyhow!(
                        "Only single entry can be parsed for google.protobuf.Timestamp type \
                        corresponding to field:{} but found {:?}",
                        schema_field,
                        parsed
                    ));
                }
                let parsed = parsed.pop().unwrap();
                if let ProtobufValue::Message(msg_val) = parsed {
                    let msg_info = context.resolve_message(msg_val.msg_ref);
                    if msg_val.fields.len() > 2 {
                        return Err(anyhow!(
                            "Expected less than or equal to 2 fields corresponding to \
                            google.protobuf.Timestamp type for field: {} but found {}",
                            schema_field,
                            msg_val.fields.len()
                        ));
                    }
                    let mut proto_timestamp = Timestamp {
                        seconds: 0,
                        nanos: 0,
                    };
                    for field in msg_val.fields {
                        let msg_field = msg_info.get_field(field.number).ok_or_else(|| {
                            anyhow!(
                                "Couldn't find field corresponding to number:{} in message info {:?}",
                                field.number,
                                msg_info
                            )
                        })?;
                        if msg_field.name == "seconds" {
                            match field.value {
                                ProtobufValue::Int64(seconds) => {
                                    proto_timestamp.seconds = seconds;
                                }
                                val => {
                                    return Err(anyhow!(
                                        "Expected Int64 for seconds inside field {:?} but found {:?}",
                                        schema_field,
                                        val
                                    ));
                                }
                            }
                        } else {
                            match field.value {
                                ProtobufValue::Int32(nanos) => {
                                    proto_timestamp.nanos = nanos;
                                }
                                val => {
                                    return Err(anyhow!(
                                        "Expected Int64 for nanos inside field {:?} but found {:?}",
                                        schema_field,
                                        val
                                    ));
                                }
                            }
                        }
                    }
                    let utc_timestamp: UTCTimestamp = proto_timestamp.try_into()?;
                    Ok(Value::Timestamp(utc_timestamp))
                } else {
                    Err(anyhow!(
                        "Expected message type for google.protobuf.Timestamp field: {} but found {:?}",
                        schema_field,
                        parsed
                    ))
                }
            }
            _ => Err(anyhow!(
                "Unsupported conversion of google.protobuf.Timestamp field to type:{:?}",
                dtype
            )),
        }
    }

    /// Parse google.protobuf.Value type decoded as protofish::decode::Value to Fennel's value
    fn from_google_protobuf_value_parsed(
        schema_field: &str,
        dtype: &Type,
        context: &ProtoContext,
        parsed: ProtobufValue,
    ) -> Result<Self> {
        match parsed {
            ProtobufValue::Message(mut msg_val) => {
                let msg_info = context.resolve_message(msg_val.msg_ref);
                if msg_info.full_name != "google.protobuf.Value" {
                    return Err(anyhow!(
                        "Expected google.protobuf.Value corresponding to field:{} but found: {}",
                        schema_field,
                        msg_info.full_name
                    ));
                }
                if msg_val.fields.len() != 1 {
                    return Err(anyhow!(
                        "Expected only 1 field inside message value corresponding to field: {} but found: {}",
                        schema_field,
                        msg_val.fields.len()
                    ));
                }
                let msg_val = msg_val.fields.pop().unwrap();
                let msg_field = msg_info.get_field(msg_val.number).ok_or_else(|| {
                    anyhow!(
                        "Protobuf message doesn't contain field corresponding to number: {:?}",
                        msg_val.number
                    )
                })?;
                let field_values = match msg_val.value {
                    ProtobufValue::Message(inner_msg_val) => {
                        match dtype {
                            Type::List(_) => {
                                // Peel the inner type since we will still have one more Message type for List
                                let inner_msg_values = inner_msg_val
                                    .fields
                                    .into_iter()
                                    .map(|f| f.value)
                                    .collect_vec();
                                inner_msg_values
                            }
                            _ => {
                                vec![ProtobufValue::Message(inner_msg_val)]
                            }
                        }
                    }
                    field_value => vec![field_value],
                };
                Self::from_proto_parsed(
                    schema_field,
                    dtype,
                    context,
                    Some(field_values),
                    msg_field,
                )
            }
            _ => {
                return Err(anyhow!(
                    "Expected message type for field:{} with google.protobuf.Value type but found: {:?}",
                    schema_field,
                    parsed
                ))
            }
        }
    }

    /// Parse google.protobuf.Struct type decoded as protofish::decode::Value to Fennel's Map Value
    fn from_google_protobuf_map_parsed_to_fennel_map(
        msg_val: MessageValue,
        dtype: &Type,
        context: &ProtoContext,
    ) -> Result<Vec<(String, Value)>> {
        let mut values: Vec<(String, Value)> = Vec::with_capacity(msg_val.fields.len());
        for field in msg_val.fields {
            let field_num = field.number;
            let proto_value = field.value;
            match proto_value {
                ProtobufValue::Message(mut msg_val) => {
                    // There should be only 2 entries corresponding to key, value in map
                    if msg_val.fields.len() != 2 {
                        return Err(anyhow!(
                            "Expected 2 entries inside google.protobuf.Struct field value:{:?} for field number: {}",
                            msg_val.fields,
                            field_num
                        ));
                    }
                    let field_value = msg_val.fields.pop().unwrap().value;
                    let name_field = msg_val.fields.pop().unwrap().value;
                    match (name_field, field_value) {
                        (ProtobufValue::String(field_name), ProtobufValue::Message(msg_val)) => {
                            let val = Self::from_google_protobuf_value_parsed(
                                &field_name,
                                dtype,
                                context,
                                ProtobufValue::Message(msg_val),
                            )?;
                            values.push((field_name, val));
                        }
                        (name_field, field_value) => {
                            return Err(anyhow!(
                                "Expected key to be string and value to be message inside \
                                google.protobuf.struct but found key: {:?}, value:{:?}",
                                name_field,
                                field_value
                            ))
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "Expected message value for google.protobuf.Struct field but found {:?}",
                        proto_value
                    ));
                }
            }
        }
        Ok(values)
    }

    /// Parse google.protobuf.Struct type decoded as protofish::decode::Value to Fennel's Struct Value
    fn from_google_protobuf_map_parsed_to_fennel_struct(
        msg_val: MessageValue,
        field_to_type_map: HashMap<String, Type>,
        context: &ProtoContext,
    ) -> Result<Vec<(SmartString, Value)>> {
        let mut values: Vec<(SmartString, Value)> = Vec::with_capacity(msg_val.fields.len());
        for field in msg_val.fields {
            let field_num = field.number;
            let proto_value = field.value;
            match proto_value {
                ProtobufValue::Message(mut msg_val) => {
                    // There should be only 2 entries corresponding to key, value in map
                    if msg_val.fields.len() != 2 {
                        return Err(anyhow!(
                            "Expected 2 entries inside field value:{:?} for field number: {}",
                            msg_val.fields,
                            field_num
                        ));
                    }
                    let field_value = msg_val.fields.pop().unwrap().value;
                    let name_field = msg_val.fields.pop().unwrap().value;
                    match (name_field, field_value.clone()) {
                        (ProtobufValue::String(field_name), ProtobufValue::Message(_)) => {
                            let field_type = field_to_type_map.get(&field_name);
                            if field_type.is_none() {
                                continue;
                            }
                            let val = Self::from_google_protobuf_value_parsed(
                                &field_name,
                                field_type.unwrap(),
                                context,
                                field_value,
                            )?;
                            values.push((field_name.into(), val));
                        }
                        (name_field, field_value) => {
                            return Err(anyhow!(
                                "Expected key to be string and value to be message inside \
                                google.protobuf.struct but found key: {:?}, value:{:?}",
                                name_field,
                                field_value
                            ))
                        }
                    }
                }
                _ => {
                    return Err(anyhow!(
                        "Expected message value for google.protobuf.Struct field but found: {:?}",
                        proto_value
                    ));
                }
            }
        }
        Ok(values)
    }

    /// Parse google.protobuf.Struct type decoded as protofish::decode::Value to Fennel's Value
    fn from_google_protobuf_struct_parsed(
        schema_field: &str,
        dtype: &Type,
        context: &ProtoContext,
        parsed: Option<Vec<ProtobufValue>>,
        multiplicity: &Multiplicity,
    ) -> Result<Self> {
        match dtype {
            Type::Optional(t) => {
                match t.as_ref() {
                    Type::Struct(_) => {
                        // TODO(Harsha): Porter is observing issues due to which we removed multiplicity check here
                        match parsed {
                            None => Ok(Value::None),
                            Some(parsed) => Self::from_google_protobuf_struct_parsed(
                                schema_field,
                                t.as_ref(),
                                context,
                                Some(parsed),
                                &Multiplicity::Single,
                            ),
                        }
                    }
                    Type::Map(_) => {
                        // TODO(Harsha): Porter is observing issues due to which we removed multiplicity check here
                        match parsed {
                            None => Ok(Value::None),
                            Some(parsed) => Self::from_google_protobuf_struct_parsed(
                                schema_field,
                                t.as_ref(),
                                context,
                                Some(parsed),
                                &Multiplicity::Single,
                            ),
                        }
                    }
                    dtype => {
                        return Err(anyhow!(
                            "Expected Optional[Struct] or Optional[Map] field:{} for \
                            google.protobuf.Struct type but found: Optional[{:?}]",
                            schema_field,
                            dtype
                        ))
                    }
                }
            }
            Type::List(t) => match multiplicity {
                Multiplicity::Repeated => match parsed {
                    None => Ok(Value::List(Arc::new(
                        List::new(t.as_ref().clone(), &[]).unwrap(),
                    ))),
                    Some(parsed) => {
                        let mut values = Vec::with_capacity(parsed.len());
                        for proto_value in parsed {
                            let value = Self::from_google_protobuf_struct_parsed(
                                schema_field,
                                t.as_ref(),
                                context,
                                Some(vec![proto_value]),
                                &Multiplicity::Single,
                            )?;
                            values.push(value);
                        }
                        Ok(Value::List(Arc::new(
                            List::new(t.as_ref().clone(), &values).unwrap(),
                        )))
                    }
                },
                _ => {
                    return Err(anyhow!(
                        "Expected Repeated multiplicity for field: {:?} of type List[google.protobuf.Struct], found: {:?}",
                        schema_field,
                        multiplicity
                    ));
                }
            },
            Type::Map(t) => {
                // Multiple entries not possible
                if parsed.is_none() {
                    return Err(anyhow!(
                        "Only single entry can be parsed for google.protobuf.Struct corresponding to field: {}, but found: {:?}",
                        schema_field,
                        parsed
                    ));
                }
                let mut parsed = parsed.unwrap();
                if parsed.len() > 1 {
                    return Err(anyhow!(
                        "Only single entry can be parsed for google.protobuf.Struct corresponding to field: {}, but found: {:?}",
                        schema_field,
                        parsed
                    ));
                }
                // We can safely unwrap given there is only 1 entry
                let parsed = parsed.pop().unwrap().clone();
                if let ProtobufValue::Message(msg_val) = parsed {
                    let values = Self::from_google_protobuf_map_parsed_to_fennel_map(
                        *msg_val,
                        t.as_ref(),
                        context,
                    )?;
                    Ok(Value::Map(Arc::new(
                        Map::new(t.as_ref().clone(), values.as_slice()).unwrap(),
                    )))
                } else {
                    return Err(anyhow!("Not implemented"));
                }
            }
            Type::Struct(st) => {
                // Multiple entries not possible
                if parsed.is_none() {
                    return Err(anyhow!(
                        "Only single entry can be parsed for google.protobuf.Struct corresponding to field: {}, but found: {:?}",
                        schema_field,
                        parsed
                    ));
                }
                let mut parsed = parsed.unwrap();
                if parsed.len() > 1 {
                    return Err(anyhow!(
                        "Only single entry can be parsed for google.protobuf.Struct corresponding to field: {}, but found: {:?}",
                        schema_field,
                        parsed
                    ));
                }
                // We can safely unwrap given there is only 1 entry
                let parsed = parsed.pop().unwrap();
                if let ProtobufValue::Message(msg_val) = parsed {
                    let field_to_type_map: HashMap<String, Type> = st
                        .fields()
                        .iter()
                        .map(|f| (f.name().to_string(), f.dtype().clone()))
                        .collect();
                    let values = Self::from_google_protobuf_map_parsed_to_fennel_struct(
                        *msg_val,
                        field_to_type_map,
                        context,
                    )?;
                    let struct_value = Value::Struct(Arc::new(Struct::new(values).unwrap()));
                    struct_value.try_matches(dtype)?;
                    Ok(struct_value)
                } else {
                    return Err(anyhow!("Not implemented"));
                }
            }
            dtype => {
                return Err(anyhow!(
                    "Expected map/struct type to be parsed for google.protobuf.Struct but \
                     found type:{:?}",
                    dtype,
                ));
            }
        }
    }

    /// Parses a proto value into a value of the given type - except proto has
    /// already been parsed into a protofish::decode::Value
    pub fn from_proto_parsed(
        schema_field: &str,
        dtype: &Type,
        context: &ProtoContext,
        parsed: Option<Vec<ProtobufValue>>,
        msg_field: &MessageField,
    ) -> Result<Self> {
        // TODO(Harsha): Pass msg_field appropriately to child methods

        // Handle google.protobuf types separately
        if let ProtobufValueType::Message(msg_ref) = msg_field.field_type {
            let msg_info = context.resolve_message(msg_ref);
            if msg_info.full_name == "google.protobuf.Struct" {
                return Self::from_google_protobuf_struct_parsed(
                    schema_field,
                    dtype,
                    context,
                    parsed,
                    &msg_field.multiplicity,
                );
            } else if msg_info.full_name == "google.protobuf.Value" {
                // TODO: Remove references and use value directly
                if let Some(ref inner_parsed) = parsed {
                    if inner_parsed.len() == 1 {
                        // Since there is single entry, we can directly parse it
                        let proto_value = inner_parsed.clone().pop().unwrap();
                        return Self::from_google_protobuf_value_parsed(
                            schema_field,
                            dtype,
                            context,
                            proto_value,
                        );
                    }
                }
            } else if msg_info.full_name == "google.protobuf.Timestamp" {
                return Self::from_google_protobuf_timestamp_parsed(
                    schema_field,
                    dtype,
                    context,
                    parsed,
                    &msg_field.multiplicity,
                );
            }
        }

        let parsed = parsed.map(|values| {
            values
                .into_iter()
                .map(|val| Value::from_protobuf_val_to_parent_type(val))
                .collect_vec()
        });

        match (dtype, parsed) {
            (Type::Optional(t), None) => {
                // For fields with non-optional multiplicity if we get None, it means they have set it to default value
                match msg_field.multiplicity {
                    Multiplicity::Optional => Ok(Value::None),
                    _ => Self::from_proto_parsed(
                        schema_field,
                        t.as_ref(),
                        context,
                        /*parsed=*/ None,
                        msg_field,
                    ),
                }
            }
            (Type::Optional(t), Some(parsed)) => {
                Self::from_proto_parsed(schema_field, t.as_ref(), context, Some(parsed), msg_field)
            }
            // Set default values for types in case of None since protobuf does the same
            (Type::Int, None) => Ok(Value::Int(0)),
            (Type::Float, None) => Ok(Value::Float(0f64)),
            (Type::String, None) => {
                if let ProtobufValueType::Enum(enum_ref) = &msg_field.field_type {
                    let enum_info = context.resolve_enum(enum_ref.clone());
                    let enum_value = enum_info.get_field_by_value(0).ok_or_else(|| {
                        anyhow!(
                            "Expected enum field corresponding to number:0 for schema field: {}",
                            schema_field
                        )
                    })?;
                    Ok(Value::String(Arc::new(enum_value.name.clone())))
                } else {
                    Ok(Value::String(Arc::new("".to_string())))
                }
            }
            (Type::Bytes, None) => Ok(Value::Bytes(Bytes::from("".to_string()))),
            (Type::Bool, None) => Ok(Value::Bool(false)),
            (Type::Decimal(t), None) => Ok(Value::Decimal(Arc::new(Decimal::new(0, t.scale())))),
            // Set default value of Enum field as 0 when it is not set
            (Type::OneOf(of), None) if *of.dtype() == Type::Int => {
                let v = Value::Int(0);
                v.try_matches(dtype).map(|_| v)
            }
            // Handle default enum value for String
            (Type::OneOf(of), None) if *of.dtype() == Type::String => match &msg_field.field_type {
                ValueType::Enum(enum_ref) => {
                    let enum_info = context.resolve_enum(enum_ref.clone());
                    let default_enum = enum_info.get_field_by_value(0);
                    match default_enum {
                        None => Err(anyhow!(
                            "Expected default value inside Enum field: {}",
                            schema_field,
                        )),
                        Some(enum_field) => {
                            let v = Value::String(Arc::new(enum_field.name.clone()));
                            v.try_matches(dtype).map(|_| v)
                        }
                    }
                }
                _ => Err(anyhow!(
                    "Expected Enum value type for field:{} but found: {:?}",
                    schema_field,
                    msg_field.field_type
                )),
            },
            (Type::Between(t), None) if *t.dtype() == Type::Int => Ok(Value::Int(0)),
            (Type::Between(t), None) if *t.dtype() == Type::String => {
                Ok(Value::String(Arc::new("".to_string())))
            }
            (Type::List(t), None) => Ok(Value::List(Arc::new(
                List::new(t.as_ref().clone(), &[]).unwrap(),
            ))),
            (Type::Embedding(_), None) => Ok(Value::Embedding(Arc::new(vec![]))),
            (Type::Map(t), None) => Ok(Value::Map(Arc::new(
                Map::new(t.as_ref().clone(), &[]).unwrap(),
            ))),
            (Type::Struct(t), None) => {
                let mut values: Vec<(SmartString, Value)> = Vec::with_capacity(t.fields().len());
                if let ProtobufValueType::Message(msg_ref) = &msg_field.field_type {
                    let msg_info = context.resolve_message(msg_ref.clone());
                    for field in t.fields() {
                        let child_msg_field = msg_info.get_field_by_name(field.name());
                        let val = match child_msg_field {
                            // If field is found, use its multiplicity
                            Some(child_msg_field) => Self::from_proto_parsed(
                                field.name(),
                                field.dtype(),
                                context,
                                None,
                                child_msg_field,
                            )?,
                            // If field is not found in schema, it means the field should be Optional
                            // and set value as None
                            None => match field.is_nullable() {
                                true => Value::None,
                                false => return Err(anyhow!(
                                    "Expected non-nullable field: {:?} inside struct field: {:?} with value None",
                                    field.name(),
                                    schema_field
                                )),
                            },
                        };
                        values.push((field.name().to_string().into(), val));
                    }
                    Ok(Value::Struct(Arc::new(Struct::new(values).unwrap())))
                } else {
                    return Err(anyhow!(
                        "Expected Message type field for struct field:{} but found: {:?}",
                        schema_field,
                        msg_field.field_type
                    ));
                }
            }
            (Type::Regex(_), None) => Ok(Value::String(Arc::new("".to_string()))),
            (dtype, None) => Err(anyhow!(
                "Expected optional type for field:{} in schema but found {}",
                schema_field,
                dtype
            )),
            (Type::List(t), Some(proto_values)) => {
                // Handling empty values when they are not set
                if proto_values.len() == 0 {
                    return Ok(Value::List(Arc::new(
                        List::new(t.as_ref().clone(), &[]).unwrap(),
                    )));
                }
                if proto_values.len() > 1 {
                    let mut values = Vec::with_capacity(proto_values.len());
                    for proto_value in proto_values {
                        let val = Self::from_proto_parsed(
                            schema_field,
                            t.as_ref(),
                            context,
                            /*parsed=*/ Some(vec![proto_value]),
                            msg_field,
                        )?;
                        values.push(val);
                    }
                    Ok(Value::List(Arc::new(
                        List::new(t.as_ref().clone(), values.as_slice()).unwrap(),
                    )))
                } else {
                    let proto_value = proto_values.get(0).unwrap().clone();
                    match proto_value {
                        ProtobufValue::Packed(packed_arr) => {
                            let elements = match (t.as_ref(), packed_arr) {
                                (Type::Int, PackedArray::Int64(data)) => {
                                    data.into_iter().map(|d| Value::Int(d)).collect_vec()
                                }
                                (Type::Float, PackedArray::Double(data)) => {
                                    data.into_iter().map(|d| Value::Float(d)).collect_vec()
                                }
                                (Type::Float, PackedArray::Int64(data)) => data
                                    .into_iter()
                                    .map(|d| Value::Float(d as f64))
                                    .collect_vec(),
                                (Type::Bool, PackedArray::Bool(data)) => {
                                    data.into_iter().map(|d| Value::Bool(d)).collect_vec()
                                }
                                (dtype, packed_arr) => {
                                    return Err(anyhow!(
                                        "Unable to parse packed protobuf value: {:?} to list of type: {:?}",
                                        packed_arr,
                                        dtype
                                    ));
                                }
                            };
                            let list = List::new(t.as_ref().clone(), &elements)?;
                            Ok(Value::List(Arc::new(list)))
                        }
                        ProtobufValue::Message(_) => {
                            // Decode the inner type of List
                            let val = Self::from_proto_parsed(
                                schema_field,
                                t.as_ref(),
                                context,
                                /*parsed=*/ Some(vec![proto_value]),
                                msg_field,
                            )?;
                            let list = List::new(t.as_ref().clone(), &[val])?;
                            Ok(Value::List(Arc::new(list)))
                        }
                        // List<Enum> is cannot be decoded by schema_registry_converter. Below logic
                        // handles List<Enum> to List<String>, List<Int>, List<OneOf> conversions
                        ProtobufValue::Unknown(UnknownValue::VariableLength(bytes)) => {
                            if let ProtobufValueType::Enum(enum_ref) = &msg_field.field_type {
                                let field_type = match t.as_ref() {
                                    Type::Int => Type::Int,
                                    Type::OneOf(of) if *of.dtype() == Type::Int => Type::Int,
                                    Type::String => Type::String,
                                    Type::OneOf(of) if *of.dtype() == Type::String => Type::String,
                                    dtype => {
                                        return Err(anyhow!(
                                                "Expected Int/String/OneOf type in schema for field:{} but found:{:?}",
                                                schema_field,
                                                dtype
                                            ));
                                    }
                                };
                                let enum_info = context.resolve_enum(enum_ref.clone());
                                let mut values = Vec::with_capacity(bytes.len());
                                for number in bytes {
                                    let enum_field = enum_info
                                        .get_field_by_value(number as i64)
                                        .ok_or_else(|| {
                                            anyhow!(
                                                "Expected enum field corresponding to number:{} for schema field: {}",
                                                number,
                                                schema_field
                                            )
                                        })?;
                                    let val = if field_type == Type::Int {
                                        Value::Int(enum_field.value)
                                    } else {
                                        Value::String(Arc::new(enum_field.name.clone()))
                                    };
                                    // Perform validation - Useful for OneOf type
                                    let val = val.try_matches(t.as_ref()).map(|_| val)?;
                                    values.push(val);
                                }
                                if field_type == Type::Int {
                                    Ok(Value::List(Arc::new(
                                        List::new(Type::Int, values.as_ref()).unwrap(),
                                    )))
                                } else {
                                    Ok(Value::List(Arc::new(
                                        List::new(Type::String, values.as_ref()).unwrap(),
                                    )))
                                }
                            } else {
                                Err(anyhow!(
                                    "Expected protobuf type for field:{} to be List[Enum] but found List[{:?}]",
                                    schema_field,
                                    &msg_field.field_type
                                ))
                            }
                        }
                        _ => Err(anyhow!(
                            "Invalid protobuf value:{:?} for list field:{}",
                            proto_value,
                            schema_field
                        )),
                    }
                }
            }
            (dtype, Some(values)) => {
                if values.len() > 1 {
                    return Err(anyhow!(
                        "Expected single parsed value for type:{:?} but found: {:?}",
                        dtype,
                        values
                    ));
                }
                let val = values.get(0).unwrap().clone();
                match (dtype, val) {
                    // Handling for Map type's value
                    (Type::Float, ProtobufValue::Message(msg_val))
                    | (Type::Int, ProtobufValue::Message(msg_val))
                    | (Type::Bool, ProtobufValue::Message(msg_val))
                    | (Type::Bytes, ProtobufValue::Message(msg_val))
                    | (Type::Timestamp, ProtobufValue::Message(msg_val))
                    | (Type::Date, ProtobufValue::Message(msg_val))
                    | (Type::String, ProtobufValue::Message(msg_val))
                    | (Type::Embedding(_), ProtobufValue::Message(msg_val))
                    | (Type::Between(_), ProtobufValue::Message(msg_val))
                    | (Type::OneOf(_), ProtobufValue::Message(msg_val))
                    | (Type::Regex(_), ProtobufValue::Message(msg_val))
                    | (Type::Decimal(_), ProtobufValue::Message(msg_val)) => {
                        if msg_val.fields.len() != 1 {
                            return Err(anyhow!(
                                "Expecting one field inside message for field:{} type but found {:?}",
                                schema_field,
                                msg_val.fields
                            ));
                        }
                        let field_val = msg_val.fields.get(0).unwrap().clone().value;
                        Self::from_proto_parsed(
                            schema_field,
                            dtype,
                            context,
                            Some(vec![field_val]),
                            msg_field,
                        )
                    }
                    (Type::Int, ProtobufValue::Int64(val)) => Ok(Value::Int(val)),
                    (Type::Float, ProtobufValue::Double(val)) => Ok(Value::Float(val)),
                    (Type::Bool, ProtobufValue::Bool(b)) => Ok(Value::Bool(b)),
                    (Type::String, ProtobufValue::String(s)) => Ok(Value::String(Arc::new(s))),
                    (Type::Bytes, ProtobufValue::Bytes(b)) => Ok(Value::Bytes(b)),
                    (Type::Timestamp, val) => {
                        Ok(Value::Timestamp(UTCTimestamp::from_proto_parsed(val)?))
                    }
                    (Type::Date, val) => Ok(Value::Date(Date::from_proto_parsed(val)?)),
                    (Type::Embedding(n), ProtobufValue::Packed(packed_arr)) => {
                        let elements = match packed_arr {
                            PackedArray::Double(data) => data.into_iter().map(|d| d).collect_vec(),
                            PackedArray::Int64(data) => {
                                data.into_iter().map(|d| d as f64).collect_vec()
                            }
                            _ => {
                                return Err(anyhow!(
                                    "Expected int/floats for embedding, but got {:?}",
                                    packed_arr
                                ))
                            }
                        };
                        if n != &elements.len() {
                            return Err(anyhow!(
                                "Expected embedding of length {} for field {}, but got {}",
                                n,
                                schema_field,
                                elements.len()
                            ));
                        }
                        Ok(Value::Embedding(Arc::new(elements)))
                    }
                    (Type::Struct(t), ProtobufValue::Message(msg_value)) => {
                        let msg_ref = msg_value.msg_ref;
                        let msg_info = context.resolve_message(msg_ref);
                        // There can be multiple fields with same value
                        let mut field_num_to_val: HashMap<u64, Vec<ProtobufValue>> =
                            HashMap::with_capacity(msg_value.fields.len());
                        for field in msg_value.fields {
                            field_num_to_val
                                .entry(field.number)
                                .or_insert(Vec::new())
                                .push(field.value);
                        }
                        let mut values: Vec<(SmartString, Value)> =
                            Vec::with_capacity(t.fields().len());
                        for field in t.fields() {
                            let field_name = field.name();
                            let field_type = field.dtype();
                            let child_msg_field = msg_info.get_field_by_name(field_name);
                            let val = match child_msg_field {
                                // If field is found, use its multiplicity
                                Some(child_msg_field) => {
                                    let proto_val = field_num_to_val.remove(&child_msg_field.number);
                                    let val = Self::from_proto_parsed(
                                        field_name,
                                        field_type,
                                        context,
                                        proto_val,
                                        &child_msg_field,
                                    )?;
                                    val
                                }
                                // If field is not found in schema, it means the field should be Optional
                                // and set value as None
                                None => match field.is_nullable() {
                                    true => Value::None,
                                    false => return Err(anyhow!(
                                    "Expected non-nullable field: {:?} inside struct field: {:?} with value None",
                                    field.name(),
                                    schema_field
                                )),
                                },
                            };
                            values.push((field.name().to_string().into(), val));
                        }
                        Ok(Value::Struct(Arc::new(Struct::new(values)?)))
                    }
                    (Type::Between(b), ProtobufValue::Int64(val)) if *b.dtype() == Type::Int => {
                        let v = Value::Int(val);
                        v.try_matches(dtype).map(|_| v)
                    }
                    (Type::Between(b), ProtobufValue::Double(val)) if *b.dtype() == Type::Float => {
                        let v = Value::Float(val);
                        v.try_matches(dtype).map(|_| v)
                    }
                    (Type::Int, ProtobufValue::Enum(enum_value)) => {
                        let enum_ref = enum_value.enum_ref;
                        let enum_info = context.resolve_enum(enum_ref);
                        let enum_field = enum_info.get_field_by_value(enum_value.value);
                        match enum_field {
                            None => Err(anyhow!(
                                "Expected value: {} inside Enum field {}",
                                enum_value.value,
                                schema_field,
                            )),
                            Some(enum_field) => Ok(Value::Int(enum_field.value)),
                        }
                    }
                    (Type::String, ProtobufValue::Enum(enum_value)) => {
                        let enum_ref = enum_value.enum_ref;
                        let enum_info = context.resolve_enum(enum_ref);
                        let enum_field = enum_info.get_field_by_value(enum_value.value);
                        match enum_field {
                            None => Err(anyhow!(
                                "Expected value: {} inside Enum field {}",
                                enum_value.value,
                                schema_field,
                            )),
                            Some(enum_field) => {
                                Ok(Value::String(Arc::new(enum_field.name.clone())))
                            }
                        }
                    }
                    (Type::OneOf(of), ProtobufValue::Enum(enum_value)) => match of.dtype() {
                        Type::Int | Type::String => {
                            let enum_ref = enum_value.enum_ref;
                            let enum_info = context.resolve_enum(enum_ref);
                            let enum_field = enum_info.get_field_by_value(enum_value.value);
                            match enum_field {
                                None => Err(anyhow!(
                                    "Expected value: {} inside Enum field {}",
                                    enum_value.value,
                                    schema_field,
                                )),
                                Some(enum_field) => {
                                    if *of.dtype() == Type::Int {
                                        let v = Value::Int(enum_field.value);
                                        v.try_matches(dtype).map(|_| v)
                                    } else {
                                        let v = Value::String(Arc::new(enum_field.name.clone()));
                                        v.try_matches(dtype).map(|_| v)
                                    }
                                }
                            }
                        }
                        _ => Err(anyhow!(
                            "Expected Int/String type for OneOf but found {}",
                            of.dtype()
                        )),
                    },
                    (Type::Regex(_), ProtobufValue::String(s)) => {
                        let v = Value::String(Arc::new(s));
                        v.try_matches(dtype).map(|_| v)
                    }
                    (Type::Decimal(d), ProtobufValue::Double(val)) => {
                        let scale = d.scale();
                        Ok(Value::Decimal(Arc::new(
                            Decimal::try_from(val)?.trunc_with_scale(scale),
                        )))
                    }
                    (_, parsed) => Err(anyhow!(
                        "Unable to parse protobuf value:{:?} for field:{} to type:{:?}",
                        parsed,
                        schema_field,
                        dtype
                    )),
                }
            }
        }
    }

    pub fn from_avro_parsed<T>(
        dtype: &Type,
        parsed: AvroValue,
        schema: T,
        schema_refs: Option<&NamesRef<'_>>,
    ) -> Result<Self>
    where
        T: Deref<Target = AvroSchema>,
    {
        match (dtype, parsed, schema.deref()) {
            // First, handle nulls & optional types for recursive handling.
            (Type::Null, AvroValue::Null, _) => Ok(Value::None),
            (Type::Null, AvroValue::Union(_, inner), _) => match *inner {
                AvroValue::Null => Ok(Value::None),
                v => Err(anyhow!("unexpected value {:?} for null type", v)),
            },
            (Type::Optional(_), AvroValue::Null, _) => Ok(Value::None),
            (Type::Optional(t), AvroValue::Union(position, inner), AvroSchema::Union(us)) => {
                match *inner {
                    AvroValue::Null => Ok(Value::None),
                    v => {
                        let value_schema = us.variants()[position as usize].clone();
                        Value::from_avro_parsed(t.as_ref(), v, &value_schema, schema_refs)
                    }
                }
            }
            (Type::Optional(t), parsed, _) => {
                Self::from_avro_parsed(t.as_ref(), parsed, schema, schema_refs)
            }
            // For avro union, fall back to the runtime (actual) type.
            (_, AvroValue::Union(position, inner), AvroSchema::Union(us)) => match *inner {
                AvroValue::Null => Err(anyhow!("unexpected null for dtype {:?}", dtype)),
                v => {
                    let value_schema = us.variants()[position as usize].clone();
                    Value::from_avro_parsed(dtype, v, &value_schema, schema_refs)
                }
            },
            // We currently do not support parsing avro schemas that contain
            // references.
            (dtype, parsed, AvroSchema::Ref { name }) => {
                let schema = schema_refs
                    .ok_or(anyhow!(
                        "schema contains references, but no schema refs provided"
                    ))?
                    .get(name)
                    .ok_or(anyhow!("schema ref {} not found", name))?;
                Self::from_avro_parsed(dtype, parsed, *schema, schema_refs)
            }
            (Type::Int, AvroValue::Int(n), AvroSchema::Int) => Ok(Value::Int(n as i64)),
            (Type::Int, AvroValue::Long(n), AvroSchema::Long) => Ok(Value::Int(n)),
            (Type::Float, AvroValue::Float(f), AvroSchema::Float) => Ok(Value::Float(f as f64)),
            (Type::Float, AvroValue::Decimal(d), AvroSchema::Decimal(decimal_schema)) => {
                // Get the decimal value as a big-endian int.
                let v = <Vec<u8>>::try_from(&d).unwrap();
                let bi = BigInt::from_signed_bytes_be(&v);
                let scale = 10_u64.pow(decimal_schema.scale as u32);
                let f = bi
                    .to_f64()
                    .ok_or(anyhow!("failed to convert bigint to float"))?
                    / (scale as f64);
                Ok(Value::Float(f))
            }
            (
                Type::Decimal(decimal_type),
                AvroValue::Decimal(d),
                AvroSchema::Decimal(decimal_schema),
            ) => {
                // Get the decimal value as a big-endian int.
                let v = <Vec<u8>>::try_from(&d).unwrap();
                let bi = BigInt::from_signed_bytes_be(&v);

                // Getting scale from schema
                let scale = decimal_schema.scale as u32;

                // Checking if scale in type matches with AvroSchema
                if decimal_type.scale() != scale {
                    return Err(anyhow!("Scale from decimal type : {:?} not matching with scale from AvroSchema {:?}", decimal_type.scale(), scale));
                }

                let decimal = Decimal::from_i128_with_scale(
                    bi.to_i128()
                        .ok_or(anyhow!("failed to convert bigint to i128"))?,
                    scale,
                );
                Ok(Value::Decimal(Arc::new(decimal)))
            }
            (Type::Float, AvroValue::Double(f), AvroSchema::Double) => Ok(Value::Float(f)),
            (Type::Bool, AvroValue::Boolean(b), AvroSchema::Boolean) => Ok(Value::Bool(b)),
            (Type::Timestamp, AvroValue::TimestampMicros(t), AvroSchema::TimestampMicros) => {
                Ok(Value::Timestamp(UTCTimestamp::try_from_micros(t)?))
            }
            (Type::Timestamp, AvroValue::TimestampMillis(t), AvroSchema::TimestampMillis) => {
                Ok(Value::Timestamp(UTCTimestamp::try_from_millis(t)?))
            }
            (Type::Timestamp, AvroValue::String(s), AvroSchema::String) => {
                Ok(Value::Timestamp(UTCTimestamp::try_from(s.as_str())?))
            }
            (Type::Date, AvroValue::String(d), AvroSchema::String) => {
                Ok(Value::Date(Date::try_from(d.as_str())?))
            }
            (Type::Date, AvroValue::Date(d), AvroSchema::String) => {
                Ok(Value::Date(Date::from(d as i64)))
            }
            (Type::Embedding(n), AvroValue::Array(elements), AvroSchema::Array(..)) => {
                if n != &elements.len() {
                    return Err(anyhow!(
                        "expected embedding of length {}, but got {}",
                        n,
                        elements.len()
                    ));
                }
                let v: Vec<f64> = elements
                    .into_iter()
                    .map(|v| match v {
                        AvroValue::Int(v) => Ok(v as f64),
                        AvroValue::Float(v) => Ok(v as f64),
                        AvroValue::Long(v) => Ok(v as f64),
                        AvroValue::Double(v) => Ok(v),
                        _ => Err(anyhow!("expected floats for embedding, but got {:?}", v)),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Value::Embedding(Arc::new(v)))
            }
            (Type::List(t), AvroValue::Array(elems), AvroSchema::Array(inner_schema)) => {
                let v: Vec<Value> = elems
                    .into_iter()
                    .map(|v| {
                        Value::from_avro_parsed(t.as_ref(), v, inner_schema.clone(), schema_refs)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let list = List::new(t.as_ref().clone(), &v)?;
                Ok(Value::List(Arc::new(list)))
            }
            (Type::Map(t), AvroValue::Map(hash), AvroSchema::Map(inner)) => {
                let v = hash
                    .into_iter()
                    .map(|(k, v)| {
                        Ok((
                            k.clone(),
                            Value::from_avro_parsed(t.as_ref(), v, inner.clone(), schema_refs)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let map = Map::new(t.as_ref().clone(), v.as_slice())?;
                Ok(Value::Map(Arc::new(map)))
            }
            (Type::Between(b), AvroValue::Int(n), AvroSchema::Int) if *b.dtype() == Type::Int => {
                let v = Value::Int(n as i64);
                v.try_matches(dtype).map(|_| v)
            }
            (Type::Between(b), AvroValue::Long(n), AvroSchema::Long) if *b.dtype() == Type::Int => {
                let v = Value::Int(n);
                v.try_matches(dtype).map(|_| v)
            }
            (Type::Between(b), AvroValue::Float(f), AvroSchema::Float)
                if *b.dtype() == Type::Float =>
            {
                let v = Value::Float(f as f64);
                v.try_matches(dtype).map(|_| v)
            }
            (Type::Between(b), AvroValue::Double(f), AvroSchema::Double)
                if *b.dtype() == Type::Float =>
            {
                let v = Value::Float(f);
                v.try_matches(dtype).map(|_| v)
            }
            (Type::OneOf(of), AvroValue::Int(n), AvroSchema::Int) if *of.dtype() == Type::Int => {
                let v = Value::Int(n as i64);
                v.try_matches(dtype).map(|_| v)
            }
            (Type::OneOf(of), AvroValue::Long(n), AvroSchema::Long) if *of.dtype() == Type::Int => {
                let v = Value::Int(n);
                v.try_matches(dtype).map(|_| v)
            }
            (Type::OneOf(of), AvroValue::String(s), AvroSchema::String)
                if *of.dtype() == Type::String =>
            {
                let v = Value::String(Arc::new(s));
                v.try_matches(dtype).map(|_| v)
            }
            (Type::Struct(s), AvroValue::Record(fields), AvroSchema::Record(record_schema)) => {
                let mut values: Vec<(SmartString, Value)> = Vec::new();
                let avro_field_to_value = fields.into_iter().collect::<HashMap<_, _>>();
                for field in s.fields() {
                    let name = field.name();
                    let ftype = field.dtype();
                    let v = match avro_field_to_value.get(name) {
                        Some(v) => {
                            let schema_index = record_schema.lookup.get(name).unwrap();
                            let field_schema = &record_schema.fields[*schema_index].schema;
                            Value::from_avro_parsed(ftype, v.clone(), field_schema, schema_refs)?
                        }
                        None if field.is_nullable() => Value::None,
                        None => {
                            return Err(anyhow!(
                                "field {} not found in avro record {:?}, avro record schema {:?}",
                                name,
                                avro_field_to_value,
                                record_schema,
                            ));
                        }
                    };
                    values.push((name.into(), v));
                }
                let struct_ = Struct::new(values)?;
                Ok(Value::Struct(Arc::new(struct_)))
            }
            (Type::String, AvroValue::String(s), AvroSchema::String) => {
                Ok(Value::String(Arc::new(s)))
            }
            (Type::String, AvroValue::Enum(_idx, av), AvroSchema::Enum(_)) => {
                Ok(Value::String(Arc::new(av)))
            }
            (Type::Bytes, AvroValue::Bytes(b), AvroSchema::Bytes) => {
                Ok(Value::Bytes(Bytes::from(b)))
            }
            (Type::Regex(_), AvroValue::String(s), AvroSchema::String) => {
                let v = Value::String(Arc::new(s));
                v.try_matches(dtype).map(|_| v)
            }
            // If the value is string but the type is not, try to parse the string into the type.
            // NOTE: It can be argued that this creates too much flexibility, but we are keeping it
            // for now since some customers have a real need for it.
            (_, AvroValue::String(s), AvroSchema::String) => Self::from_json(dtype, &s),
            (_, parsed, schema) => Err(anyhow!(
                "unable to parse avro value with schema {:?}: expected {:?}, but got {:?}",
                schema,
                dtype,
                parsed
            )),
        }
    }
    pub fn hash(&self) -> Result<u64> {
        let mut writer = Vec::new();
        writer
            .write_u8(self.get_codec())
            .map_err(|err| anyhow!("failed to write codec for value {:?}", err))?;
        match *self {
            Value::None => {}
            Value::Int(i) => {
                writer
                    .write_varint(i)
                    .map_err(|err| anyhow!("failed to write int value {:?}", err))?;
            }
            Value::Bool(b) => {
                writer
                    .write_u8(if b { 1 } else { 0 })
                    .map_err(|err| anyhow!("failed to write bool value {:?}", err))?;
            }
            Value::String(ref s) => {
                writer
                    .write_varint(s.len() as i64)
                    .map_err(|err| anyhow!("failed to write length of string {:?}", err))?;
                writer
                    .write_all(s.as_bytes())
                    .map_err(|err| anyhow!("failed to write string value {:?}", err))?;
            }
            Value::Bytes(ref b) => {
                writer
                    .write_varint(b.len() as i64)
                    .map_err(|err| anyhow!("failed to write length of bytes value {:?}", err))?;
                writer
                    .write_all(b)
                    .map_err(|err| anyhow!("failed to write bytes value {:?}", err))?;
            }
            Value::Timestamp(t) => {
                writer
                    .write_varint(t.micros())
                    .map_err(|err| anyhow!("failed to write timestamp value {:?}", err))?;
            }
            Value::Date(d) => {
                writer
                    .write_varint(d.days())
                    .map_err(|err| anyhow!("failed to write date value {:?}", err))?;
            }
            Value::List(ref l) => {
                writer
                    .write_varint(l.len() as i64)
                    .map_err(|err| anyhow!("failed to write length of list value {:?}", err))?;
                for v in l.iter() {
                    writer.write_varint(v.hash()?).map_err(|err| {
                        anyhow!("failed to write hash of each list values {:?}", err)
                    })?;
                }
            }
            Value::Map(ref m) => {
                writer
                    .write_varint(m.len() as i64)
                    .map_err(|err| anyhow!("failed to write length of map {:?}", err))?;
                for (key, value) in m.keys().zip(m.values()) {
                    writer
                        .write_varint(key.len() as i64)
                        .map_err(|err| anyhow!("failed to write length of key {:?}", err))?;
                    writer
                        .write(key.as_bytes())
                        .map_err(|err| anyhow!("failed to write key {:?}", err))?;
                    writer
                        .write_varint(value.hash()?)
                        .map_err(|err| anyhow!("failed to write value hash {:?}", err))?;
                }
            }
            Value::Float(_) => {
                return Err(anyhow!(
                    "hashing is not supported for float types, due to precision issues",
                ));
            }
            Value::Embedding(_) => {
                return Err(anyhow!(
                    "hashing is not supported for embedding types, due to \
                                         precision issues",
                ));
            }
            Value::Struct(ref s) => {
                writer.write_varint(s.num_fields() as i64).map_err(|err| {
                    anyhow!(
                        "failed to write number of fields for struct value {:?}",
                        err
                    )
                })?;
                for (name, val) in s.data() {
                    writer
                        .write_varint(name.len() as i64)
                        .map_err(|err| anyhow!("failed to write length of name {:?}", err))?;
                    writer
                        .write(name.as_bytes())
                        .map_err(|err| anyhow!("failed to write name {:?}", err))?;
                    writer
                        .write_varint(val.hash()?)
                        .map_err(|err| anyhow!("failed to write value hash {:?}", err))?;
                }
            }
            Value::Decimal(ref d) => {
                writer
                    .write_i128::<BigEndian>(d.mantissa())
                    .map_err(|err| {
                        anyhow!(
                            "failed to write mantissa of the decimal for decimal value {:?}",
                            err
                        )
                    })?;
                writer.write_varint(d.scale()).map_err(|err| {
                    anyhow!(
                        "failed to write scale of the decimal for decimal value {:?}",
                        err
                    )
                })?;
            }
        }
        Ok(xxh3_64(&writer))
    }

    pub fn to_json(&self) -> JsonValue {
        match self {
            Value::None => JsonValue::Null,
            Value::Int(v) => JsonValue::from(*v),
            Value::Float(v) => {
                if v.is_nan() {
                    JsonValue::from("NaN")
                } else if v.is_infinite() && v.is_sign_positive() {
                    JsonValue::from("+infinity")
                } else if v.is_infinite() && v.is_sign_negative() {
                    JsonValue::from("-infinity")
                } else {
                    JsonValue::from(*v)
                }
            }
            Value::String(v) => JsonValue::from(v.as_ref().clone()),
            Value::Bytes(v) => {
                // For json we encode bytes as base64
                JsonValue::String(general_purpose::STANDARD.encode(v.as_ref()))
            }
            Value::Bool(v) => JsonValue::from(*v),
            Value::Timestamp(v) => v.to_json(),
            Value::Date(v) => v.to_json(),
            Value::Embedding(v) => {
                JsonValue::Array(v.iter().map(|v| JsonValue::from(*v)).collect())
            }
            Value::List(v) => {
                JsonValue::Array(v.data.iter().map(|v| v.to_json()).collect::<Vec<_>>())
            }
            Value::Map(v) => {
                let mut object_map = serde_json::Map::with_capacity(v.data.len());
                for (k, v) in v.data.iter() {
                    // assert that the same key is never updated again
                    if object_map.insert(k.clone(), v.to_json()).is_some() {
                        // This should never happen since we are iterating over our own map, which
                        // should not have duplicate keys.
                        panic!("duplicate key {} in map", k);
                    }
                }
                serde_json::Value::Object(object_map)
            }
            Value::Struct(s) => {
                let mut object_map = serde_json::Map::with_capacity(s.num_fields());
                for (name, val) in s.data() {
                    // assert that the same key is never updated again
                    assert!(object_map
                        .insert(name.clone().to_string(), val.to_json())
                        .is_none());
                }
                serde_json::Value::Object(object_map)
            }
            Value::Decimal(d) => {
                JsonValue::from(d.to_f64().expect("Decimal to f64 should always succeed"))
            }
        }
    }

    pub fn as_int(&self) -> Result<i64> {
        match self {
            Value::Int(v) => Ok(*v),
            _ => Err(anyhow!("value {:?} is not an int", self)),
        }
    }

    pub fn as_float(&self) -> Result<f64> {
        match self {
            Value::Float(v) => Ok(*v),
            _ => Err(anyhow!("value {:?} is not a float", self)),
        }
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            Value::Bool(v) => Ok(*v),
            _ => Err(anyhow!("value {:?} is not a bool", self)),
        }
    }

    pub fn as_timestamp(&self) -> Result<UTCTimestamp> {
        match self {
            Value::Timestamp(v) => Ok(*v),
            _ => Err(anyhow!("value {:?} is not a timestamp", self)),
        }
    }

    pub fn as_date(&self) -> Result<Date> {
        match self {
            Value::Date(v) => Ok(*v),
            _ => Err(anyhow!("value {:?} is not a date", self)),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        match self {
            Value::String(v) => Ok(v),
            _ => Err(anyhow!("value {:?} is not a string", self)),
        }
    }

    pub fn as_bytes(&self) -> Result<&[u8]> {
        match self {
            Value::Bytes(v) => Ok(v),
            _ => Err(anyhow!("value {:?} is not a bytes object", self)),
        }
    }

    pub fn as_embedding(&self) -> Result<&[f64]> {
        match self {
            Value::Embedding(v) => Ok(v),
            _ => Err(anyhow!("value {:?} is not an embedding", self)),
        }
    }

    pub fn as_list(&self) -> Result<&List> {
        match self {
            Value::List(v) => Ok(v),
            _ => Err(anyhow!("value {:?} is not a list", self)),
        }
    }

    pub fn as_map(&self) -> Result<&Map> {
        match self {
            Value::Map(v) => Ok(v),
            _ => Err(anyhow!("value {:?} is not a map", self)),
        }
    }

    pub fn as_struct(&self, nullable: bool) -> Result<Option<&Struct>> {
        match self {
            Value::Struct(v) => Ok(Some(v)),
            Value::None => {
                if nullable {
                    Ok(None)
                } else {
                    Err(anyhow!("Received null value for non-nullable struct"))
                }
            }
            _ => Err(anyhow!("value {:?} is not a struct", self)),
        }
    }

    pub fn as_decimal(&self) -> Result<&Decimal> {
        match self {
            Value::Decimal(d) => Ok(d),
            _ => Err(anyhow!("value {:?} is not a decimal", self)),
        }
    }

    /// Writes an efficiently encoded version of the value to the writer
    /// NOTE: this does not encode the type of the value so that it can
    /// NOT be decoded without the type information.
    pub fn encode_many(writer: &mut dyn Write, vals: &[&Self], dtypes: &[&Type]) -> Result<()> {
        Self::encode_many_iter(writer, vals.len(), vals.iter(), dtypes.iter())
    }

    pub fn encode_many_iter<I1, I2, V, T>(
        mut writer: &mut dyn Write,
        count: usize,
        vals: I1,
        dtypes: I2,
    ) -> Result<()>
    where
        I1: Iterator<Item = V>,
        I2: Iterator<Item = T>,
        V: AsRef<Value>,
        T: AsRef<Type>,
    {
        // add a version byte
        writer.write_all(&[CODEC])?;
        writer.write_varint(count as usize)?;
        let mut taken = 0;
        for (v, dtype) in vals.zip(dtypes).take(count) {
            v.as_ref().encode(dtype.as_ref(), &mut writer)?;
            taken += 1;
        }
        if taken != count {
            return Err(anyhow!(
                "expected {} values, but only encoded {}",
                count,
                taken
            ));
        }
        Ok(())
    }

    fn round_float(f: f64) -> i64 {
        (f.clamp(-92233720368547.75807, 92233720368547.75807) * 1e4).round() as i64
    }

    // Use sparingly
    pub fn hash_many_v2(&self, dtype: &Type, mut writer: &mut dyn Write) -> Result<()> {
        // For floats, round to 5 decimal places and hash the integer value
        match (self, dtype) {
            (v, Type::Optional(t)) => {
                if v == &Value::None {
                    writer.write_u8(0)?;
                } else {
                    writer.write_u8(1)?;
                    v.hash_many_v2(t, writer)?;
                }
            }
            (Value::Float(v), Type::Float) => {
                let rounded = Self::round_float(*v);
                writer.write_all(&rounded.to_le_bytes())?;
            }
            (Value::Embedding(v), Type::Embedding(n)) => {
                if v.len() != *n as usize {
                    return Err(anyhow!(
                        "embedding has {} elements, but type has {}",
                        v.len(),
                        n
                    ));
                }
                writer.write_varint(*n as usize)?;
                for f in v.iter() {
                    let rounded = Self::round_float(*f);
                    writer.write_all(&rounded.to_le_bytes())?;
                }
            }
            (Value::List(l), Type::List(t)) => {
                writer.write_varint(l.data.len())?;
                for v in l.data.iter() {
                    v.hash_many_v2(t, writer)?;
                }
            }
            (Value::Map(m), Type::Map(t)) => {
                writer.write_varint(m.data.len())?;
                for (k, v) in m.data.iter() {
                    writer.write_varint(k.len())?;
                    writer.write_all(k.as_bytes())?;
                    v.hash_many_v2(t, writer)?;
                }
            }
            (Value::Struct(s), Type::Struct(t)) => {
                writer.write_varint(s.data().len())?;
                for ((name, v), t) in multizip((s.data(), t.fields())) {
                    writer.write_varint(name.len())?;
                    writer.write_all(name.as_bytes())?;
                    v.hash_many_v2(t.dtype(), writer)?;
                }
            }
            _ => self.encode(dtype, writer)?,
        }
        Ok(())
    }

    pub fn encode(&self, dtype: &Type, mut writer: &mut dyn Write) -> Result<()> {
        // NOTE: Below logic should handle values that correspond to `permissive_schema()`
        // Check if the value matches given type
        if !self.matches(dtype) {
            return Err(anyhow!(
                "can not encode value {:?} with type {:?}. Do not match",
                self,
                dtype
            ));
        }

        match (self, dtype) {
            (Value::None, Type::Null) => writer.write_u8(0)?,
            (Value::None, Type::Optional(_)) => writer.write_u8(0)?,
            (v, Type::Optional(t)) => {
                writer.write_u8(1)?;
                v.encode(t, writer)?
            }
            (Value::Int(v), Type::Int) => {
                writer.write_varint(*v)?;
            }
            (Value::Float(v), Type::Float) => writer.write_all(&v.to_le_bytes())?,
            (Value::String(v), Type::String) => {
                writer.write_varint(v.len() as usize)?;
                writer.write_all(v.as_bytes())?;
            }
            (Value::Bytes(v), Type::Bytes) => {
                writer.write_varint(v.len() as usize)?;
                writer.write_all(v)?;
            }
            (Value::Bool(false), Type::Bool) => writer.write_u8(0)?,
            (Value::Bool(true), Type::Bool) => writer.write_u8(1)?,
            (Value::Timestamp(v), Type::Timestamp) => {
                writer.write_varint(v.micros)?;
            }
            (Value::Date(v), Type::Date) => {
                writer.write_varint(v.days())?;
            }
            (Value::Embedding(v), Type::Embedding(n)) => {
                if v.len() != *n as usize {
                    return Err(anyhow!(
                        "embedding has {} elements, but type has {}",
                        v.len(),
                        n
                    ));
                }
                for f in v.iter() {
                    writer.write_all(&f.to_le_bytes())?;
                }
            }
            (Value::List(l), Type::Embedding(n)) => {
                if l.len() != *n as usize {
                    return Err(anyhow!(
                        "embedding has {} elements, but type has {}",
                        l.len(),
                        n
                    ));
                }

                for v in l.iter() {
                    v.encode(&Type::Float, writer)?;
                }
            }
            (Value::List(l), Type::List(t)) => {
                writer.write_varint(l.data.len() as usize)?;
                for v in l.data.iter() {
                    v.encode(t, writer)?;
                }
            }
            (Value::Map(m), Type::Map(t)) => {
                writer.write_varint(m.data.len() as usize)?;
                for (k, v) in m.data.iter() {
                    writer.write_varint(k.len() as usize)?;
                    writer.write_all(k.as_bytes())?;
                    v.encode(t, writer)?;
                }
            }
            (Value::Int(v), Type::Between(_)) | (Value::Int(v), Type::OneOf(_)) => {
                if self.matches(dtype) {
                    writer.write_varint(*v)?;
                } else {
                    return Err(anyhow!("value {:?} does not match type {:?}", self, dtype));
                }
            }
            (Value::Float(v), Type::Between(_)) => {
                if self.matches(dtype) {
                    writer.write_all(&v.to_le_bytes())?;
                } else {
                    return Err(anyhow!("value {:?} does not match type {:?}", self, dtype));
                }
            }
            (Value::String(v), Type::OneOf(_)) | (Value::String(v), Type::Regex(_)) => {
                writer.write_varint(v.len() as usize)?;
                writer.write_all(v.as_bytes())?;
            }
            (Value::Struct(s), Type::Struct(t)) => {
                writer.write_varint(s.data().len() as usize)?;
                let mut value_struct_index = 0;
                let mut type_index = 0;
                while type_index < t.num_fields() {
                    let type_field = t.fields().get(type_index).unwrap();
                    if value_struct_index >= s.data.len() {
                        if !type_field.is_nullable() {
                            return Err(anyhow!(
                                "value {:?} does not match type {:?}",
                                self,
                                dtype
                            ));
                        }
                        type_index += 1;
                    }
                    let (v_name, v) = s.data().get(value_struct_index).unwrap();
                    if type_field.name() == v_name {
                        writer.write_varint(v_name.len() as usize)?;
                        writer.write_all(v_name.as_bytes())?;
                        v.encode(type_field.dtype(), writer)?;
                        value_struct_index += 1;
                        type_index += 1;
                    } else if type_field.name() < v_name as &str {
                        if !type_field.is_nullable() {
                            return Err(anyhow!(
                                "value {:?} does not match type {:?}",
                                self,
                                dtype
                            ));
                        }
                        type_index += 1;
                    } else {
                        return Err(anyhow!("value {:?} does not match type {:?}", self, dtype));
                    }
                }
            }
            (Value::Decimal(v), Type::Decimal(t)) => {
                if v.scale() != t.scale() {
                    return Err(anyhow!(
                        "Scale {:?} in value does not match scale : {:?} in type",
                        v.scale(),
                        t.scale()
                    ));
                }
                writer.write_i128::<BigEndian>(v.mantissa())?;
                writer.write_u32::<BigEndian>(v.scale())?;
            }
            _ => {
                return Err(anyhow!(
                    "can not encode value {:?} with type {:?}",
                    self,
                    dtype
                ));
            }
        }
        Ok(())
    }

    /// Reads an efficiently encoded version of the value from the reader
    pub fn decode_many<T: AsRef<Type>>(
        reader: &mut impl Read,
        dtypes: &mut impl Iterator<Item = T>,
    ) -> Result<Vec<Self>> {
        let codec = reader.read_u8()?;
        if codec != CODEC {
            return Err(anyhow!("can not decode values: invalid codec: {:?}", codec,));
        }
        let n: usize = reader.read_varint::<usize>()?;
        let mut vals = Vec::with_capacity(n);
        for _ in 0..n {
            let dtype = dtypes
                .next()
                .ok_or_else(|| anyhow!("not enough types to decode values"))?;
            vals.push(Self::decode(reader, dtype.as_ref())?);
        }
        Ok(vals)
    }

    pub fn decode(reader: &mut impl Read, dtype: &Type) -> Result<Self> {
        let mut eight = [0u8; 8];
        match dtype {
            Type::Null => match reader.read_u8()? {
                0 => Ok(Value::None),
                n => Err(anyhow!("invalid null value, expected 0, found {}", n)),
            },
            Type::Int => {
                let n: i64 = reader.read_varint::<i64>()?;
                Ok(Value::Int(n))
            }
            Type::Float => {
                reader.read_exact(eight.as_mut())?;
                let f = f64::from_le_bytes(eight);
                Ok(Value::Float(f))
            }
            Type::Timestamp => {
                let n: i64 = reader.read_varint::<i64>()?;
                Ok(Value::Timestamp(UTCTimestamp::from(n)))
            }
            Type::Date => {
                let days: i64 = reader.read_varint::<i64>()?;
                Ok(Value::Date(Date::from(days)))
            }
            Type::Bool => match reader.read_u8()? {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(anyhow!("invalid bool value")),
            },
            Type::Embedding(n) => {
                let mut v = Vec::with_capacity(*n);
                for _ in 0..*n {
                    reader.read_exact(eight.as_mut())?;
                    let f = f64::from_le_bytes(eight);
                    v.push(f);
                }
                Ok(Value::Embedding(Arc::new(v)))
            }
            Type::String => {
                let n: usize = reader.read_varint::<usize>()?;
                let mut s = String::with_capacity(n);
                reader.take(n as u64).read_to_string(&mut s)?;
                Ok(Value::String(Arc::new(s)))
            }
            Type::Bytes => {
                let n: usize = reader.read_varint::<usize>()?;
                let mut data = vec![0u8; n];
                reader.read_exact(&mut data)?;
                Ok(Value::Bytes(Bytes::from(data)))
            }
            Type::List(t) => {
                let n: usize = reader.read_varint::<usize>()?;
                let mut data = Vec::with_capacity(n);
                for _ in 0..n {
                    data.push(Self::decode(reader, t)?);
                }
                Ok(Value::List(Arc::new(List {
                    dtype: *t.clone(),
                    data,
                })))
            }
            Type::Map(t) => {
                let n: usize = reader.read_varint::<usize>()?;
                let mut data = Vec::with_capacity(n);
                for _ in 0..n {
                    let klen: usize = reader.read_varint::<usize>()?;
                    let mut k = String::with_capacity(klen);
                    reader.take(klen as u64).read_to_string(&mut k)?;
                    let v = Self::decode(reader, t)?;
                    data.push((k, v));
                }
                Ok(Value::Map(Arc::new(Map {
                    dtype: *t.clone(),
                    data,
                })))
            }
            Type::Optional(t) => match reader.read_u8()? {
                0 => Ok(Value::None),
                1 => Ok(Self::decode(reader, t)?),
                _ => Err(anyhow!("invalid optional value")),
            },
            Type::Between(b) => {
                if *b.dtype() == Type::Float {
                    reader.read_exact(eight.as_mut())?;
                    let f = f64::from_le_bytes(eight);
                    if Value::Float(f).matches(dtype) {
                        Ok(Value::Float(f))
                    } else {
                        Err(anyhow!("invalid float between value"))
                    }
                } else {
                    let n: i64 = reader.read_varint::<i64>()?;
                    if Value::Int(n).matches(dtype) {
                        Ok(Value::Int(n))
                    } else {
                        Err(anyhow!("invalid int between value"))
                    }
                }
            }
            Type::Regex(_) => {
                let n: usize = reader.read_varint::<usize>()?;
                let mut s = String::with_capacity(n);
                reader.take(n as u64).read_to_string(&mut s)?;
                let str = Value::String(Arc::new(s));
                if str.matches(dtype) {
                    Ok(str)
                } else {
                    Err(anyhow!("invalid string regex value"))
                }
            }
            Type::OneOf(of) => {
                let options = of.values();
                if *of.dtype() == Type::String {
                    let n: usize = reader.read_varint::<usize>()?;
                    let mut s = String::with_capacity(n);
                    reader.take(n as u64).read_to_string(&mut s)?;
                    let str = Value::String(Arc::new(s));
                    if str.matches(dtype) {
                        Ok(str)
                    } else {
                        Err(anyhow!("Expected one of {:?}, got {:?}", options, str))
                    }
                } else {
                    let n: i64 = reader.read_varint::<i64>()?;
                    if Value::Int(n).matches(dtype) {
                        Ok(Value::Int(n))
                    } else {
                        Err(anyhow!("Expected one of {:?}, got {:?}", options, n))
                    }
                }
            }
            Type::Struct(struct_type) => {
                let num_fields: usize = reader.read_varint::<usize>()?;
                let mut data: Vec<(SmartString, Value)> = Vec::with_capacity(num_fields);
                let fields = struct_type.fields();
                assert_eq!(num_fields, fields.len());
                for i in 0..num_fields {
                    let klen: usize = reader.read_varint::<usize>()?;
                    let mut k = String::with_capacity(klen);
                    reader.take(klen as u64).read_to_string(&mut k)?;
                    let f = fields.get(i).unwrap();
                    data.push((k.into(), Self::decode(reader, &f.dtype())?));
                }
                Ok(Value::Struct(Arc::new(Struct::new(data)?)))
            }
            Type::Decimal(decimal_type) => {
                let mantissa = reader.read_i128::<BigEndian>()?;
                let scale = reader.read_u32::<BigEndian>()?;
                if decimal_type.scale() != scale {
                    return Err(anyhow!(
                        "Expected scale of {:?}, got {:?}",
                        decimal_type.scale(),
                        scale
                    ));
                }
                Ok(Value::Decimal(Arc::new(Decimal::from_i128_with_scale(
                    mantissa, scale,
                ))))
            }
        }
    }

    pub fn random(t: impl AsRef<Type>) -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        match t.as_ref() {
            Type::Null => Value::None,
            Type::Optional(t) => {
                if rng.gen() {
                    Self::random(t)
                } else {
                    Value::None
                }
            }
            Type::Int => Value::Int(rng.gen()),
            Type::Float => Value::Float(rng.gen()),
            Type::Bool => Value::Bool(rng.gen()),
            Type::String => {
                let n = rng.gen_range(0..10);
                let mut s = String::with_capacity(n);
                for _ in 0..n {
                    s.push(rng.gen_range(b'a'..b'z') as char);
                }
                Value::String(Arc::new(s))
            }
            Type::Timestamp => Value::Timestamp(UTCTimestamp::from(rng.gen())),
            Type::Date => Value::Date(Date::from(rng.gen())),
            Type::Embedding(n) => Value::Embedding(Arc::new(
                (0..*n).map(|_| rng.gen::<f64>()).collect::<Vec<_>>().into(),
            )),
            Type::List(t) => Value::List(Arc::new(
                List::new(
                    t.as_ref().clone(),
                    &(0..rng.gen_range(0..10))
                        .map(|_| Self::random(t))
                        .collect_vec(),
                )
                .unwrap(),
            )),
            Type::Map(t) => {
                let n = rng.gen_range(0..10);
                let mut data = Vec::with_capacity(n);
                for _ in 0..n {
                    let k = Self::random(&Type::String);
                    let v = Self::random(t);
                    data.push((k.as_str().unwrap().to_string(), v));
                }
                Value::Map(Arc::new(Map::new(t.as_ref().clone(), &data).unwrap()))
            }
            Type::Struct(t) => {
                let mut data = Vec::with_capacity(t.num_fields());
                for field in t.fields() {
                    let k = field.name().to_string();
                    let v = Self::random(&field.dtype());
                    data.push((k.into(), v));
                }
                Value::Struct(Arc::new(Struct::new(data).unwrap()))
            }
            Type::Decimal(t) => {
                let mantissa = rng.gen_range(i128::MIN..i128::MAX);
                let scale = t.scale();
                Value::Decimal(Arc::new(Decimal::from_i128_with_scale(mantissa, scale)))
            }
            Type::Bytes => {
                let n = rng.gen_range(0..10);
                let mut data = vec![0u8; n];
                for i in 0..n {
                    data[i] = rng.gen();
                }
                Value::Bytes(Bytes::from(data))
            }
            Type::Between(t) => match t.dtype() {
                Type::Int => {
                    let min = t.min().as_int().unwrap();
                    let max = t.max().as_int().unwrap();
                    Value::Int(rng.gen_range(min..max))
                }
                Type::Float => {
                    let min = t.min().as_float().unwrap();
                    let max = t.max().as_float().unwrap();
                    Value::Float(rng.gen_range(min..max))
                }
                _ => unreachable!(),
            },
            Type::Regex(_) => todo!(),
            Type::OneOf(t) => {
                let options = t.values();
                let i = rng.gen_range(0..options.len());
                options[i].clone()
            }
        }
    }

    fn get_codec(&self) -> u8 {
        match self {
            Value::None => 0,
            Value::Int(_) => 1,
            Value::Float(_) => 2,
            Value::Bool(_) => 3,
            Value::String(_) => 4,
            Value::Timestamp(_) => 5,
            Value::Embedding(_) => 6,
            Value::List(_) => 7,
            Value::Map(_) => 8,
            Value::Struct(_) => 9,
            Value::Decimal(_) => 10,
            Value::Date(_) => 11,
            Value::Bytes(_) => 12,
        }
    }
}

type ProtoValue = fproto::schema::Value;

impl TryFrom<ProtoValue> for Value {
    type Error = anyhow::Error;

    fn try_from(v: ProtoValue) -> Result<Self> {
        use fproto::schema::value::Variant;
        if v.variant.is_none() {
            return Err(anyhow!("empty value"));
        }
        match v.variant.unwrap() {
            Variant::None(_) => Ok(Value::None),
            Variant::Int(value) => Ok(Value::Int(value)),
            Variant::Float(value) => Ok(Value::Float(value)),
            Variant::String(value) => Ok(Value::String(Arc::new(value))),
            Variant::Bytes(value) => Ok(Value::Bytes(Bytes::from(value))),
            Variant::Bool(value) => Ok(Value::Bool(value)),
            Variant::Timestamp(ts) => Ok(Value::Timestamp(UTCTimestamp::from(
                ts.seconds as i64 * 1000_000 + ts.nanos as i64 / 1000,
            ))),
            Variant::Date(date) => Ok(Value::Date(Date::from(date.days))),
            Variant::Embedding(e) => Ok(Value::Embedding(Arc::new(e.values))),
            Variant::List(list) => Ok(Value::List(Arc::new(List::new(
                match list.dtype {
                    Some(dtype) => (*dtype).try_into()?,
                    None => return Err(anyhow!("missing list dtype")),
                },
                &list
                    .values
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>>>()?,
            )?))),
            Variant::Map(map) => match (map.key_dtype, map.value_dtype) {
                (Some(key_type), Some(value_dtype))
                    if key_type.as_ref() == &(&Type::String).into() =>
                {
                    Ok(Value::Map(Arc::new(Map::new(
                        (*value_dtype).try_into()?,
                        &map.entries
                            .into_iter()
                            .map(|e| {
                                let key: Value = match e.key {
                                    Some(key) => key.try_into()?,
                                    None => return Err(anyhow!("missing map key")),
                                };
                                let value = match e.value {
                                    Some(value) => value.try_into()?,
                                    None => return Err(anyhow!("missing map value")),
                                };
                                Ok((key.as_str().unwrap().to_string(), value))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    )?)))
                }
                _ => Err(anyhow!("missing/invalid map key/value dtype")),
            },
            Variant::Struct(s) => Ok(Value::Struct(Arc::new(Struct::new(
                s.fields
                    .into_iter()
                    .map(|f| {
                        Ok((
                            f.name.into(),
                            f.value
                                .ok_or_else(|| anyhow!("missing struct field value"))?
                                .try_into()?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?))),
            Variant::Decimal(d) => Ok(Value::Decimal(Arc::new(Decimal::new(
                d.value,
                d.scale as u32,
            )))),
        }
    }
}

impl From<&Value> for ProtoValue {
    fn from(value: &Value) -> Self {
        use fproto::schema::value::Variant;
        match value {
            Value::None => Self {
                variant: Some(Variant::None(fproto::schema::None {})),
            },
            Value::Int(n) => Self {
                variant: Some(Variant::Int(*n)),
            },
            Value::Float(f) => Self {
                variant: Some(Variant::Float(*f)),
            },
            Value::String(s) => Self {
                variant: Some(Variant::String(s.to_string())),
            },
            Value::Bytes(b) => Self {
                variant: Some(Variant::Bytes(b.to_vec())),
            },
            Value::Bool(b) => Self {
                variant: Some(Variant::Bool(*b)),
            },
            Value::Timestamp(ts) => Self {
                variant: Some(Variant::Timestamp(prost_types::Timestamp {
                    seconds: ts.micros() / 1_000_000,
                    nanos: (ts.micros() % 1_000_000) as i32 * 1_000,
                })),
            },
            Value::Date(date) => Self {
                variant: Some(Variant::Date(fproto::schema::Date { days: date.days() })),
            },
            Value::Embedding(e) => Self {
                variant: Some(Variant::Embedding(fproto::schema::Embedding {
                    values: e.to_vec(),
                })),
            },
            Value::List(l) => Self {
                variant: Some(Variant::List(Box::new(fproto::schema::List {
                    dtype: Some(Box::new(l.dtype().into())),
                    values: l.data.iter().map(|v| v.into()).collect(),
                }))),
            },
            Value::Map(m) => Self {
                variant: Some(Variant::Map(Box::new(fproto::schema::Map {
                    key_dtype: Some(Box::new((&Type::String).into())),
                    value_dtype: Some(Box::new(m.dtype().into())),
                    entries: m
                        .data
                        .iter()
                        .map(|(k, v)| fproto::schema::map::Entry {
                            key: Some((&Value::String(Arc::new(k.to_string()))).into()),
                            value: Some(v.into()),
                        })
                        .collect(),
                }))),
            },
            Value::Struct(s) => Self {
                variant: Some(Variant::Struct(fproto::schema::StructValue {
                    fields: s
                        .data()
                        .iter()
                        .map(|(name, v)| fproto::schema::struct_value::Entry {
                            name: name.to_string(),
                            value: Some(v.into()),
                        })
                        .collect(),
                })),
            },
            Value::Decimal(d) => Self {
                variant: Some(Variant::Decimal(fproto::schema::Decimal {
                    scale: d.scale() as i32,
                    value: d.mantissa() as i64,
                })),
            },
        }
    }
}

impl From<Value> for ProtoValue {
    fn from(value: Value) -> Self {
        use fproto::schema::value::Variant;
        match value {
            Value::None => Self {
                variant: Some(Variant::None(fproto::schema::None {})),
            },
            Value::Int(n) => Self {
                variant: Some(Variant::Int(n)),
            },
            Value::Float(f) => Self {
                variant: Some(Variant::Float(f)),
            },
            Value::String(s) => Self {
                variant: Some(Variant::String(s.to_string())),
            },
            Value::Bytes(b) => Self {
                variant: Some(Variant::Bytes(b.to_vec())),
            },
            Value::Bool(b) => Self {
                variant: Some(Variant::Bool(b)),
            },
            Value::Timestamp(ts) => Self {
                variant: Some(Variant::Timestamp(prost_types::Timestamp {
                    seconds: ts.micros() / 1_000_000,
                    nanos: (ts.micros() % 1_000_000) as i32 * 1_000,
                })),
            },
            Value::Date(date) => Self {
                variant: Some(Variant::Date(fproto::schema::Date { days: date.days() })),
            },
            Value::Embedding(e) => Self {
                variant: Some(Variant::Embedding(fproto::schema::Embedding {
                    values: e.to_vec(),
                })),
            },
            Value::List(l) => Self {
                variant: Some(Variant::List(Box::new(fproto::schema::List {
                    dtype: Some(Box::new(l.dtype().into())),
                    values: l.data.iter().map(|v| v.into()).collect(),
                }))),
            },
            Value::Map(m) => Self {
                variant: Some(Variant::Map(Box::new(fproto::schema::Map {
                    key_dtype: Some(Box::new((&Type::String).into())),
                    value_dtype: Some(Box::new(m.dtype().into())),
                    entries: m
                        .data
                        .iter()
                        .map(|(k, v)| fproto::schema::map::Entry {
                            key: Some((&Value::String(Arc::new(k.to_string()))).into()),
                            value: Some(v.into()),
                        })
                        .collect(),
                }))),
            },
            Value::Struct(s) => Self {
                variant: Some(Variant::Struct(fproto::schema::StructValue {
                    fields: s
                        .data()
                        .iter()
                        .map(|(name, v)| fproto::schema::struct_value::Entry {
                            name: name.to_string(),
                            value: Some(v.into()),
                        })
                        .collect(),
                })),
            },
            Value::Decimal(d) => Self {
                variant: Some(Variant::Decimal(fproto::schema::Decimal {
                    scale: d.scale() as i32,
                    value: d.mantissa() as i64,
                })),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use core::panic;

    use apache_avro::schema::Schema as AvroSchema;
    use apache_avro::types::Record as AvroRecord;
    use apache_avro::Reader as AvroReader;
    use apache_avro::Writer as AvroWriter;
    use chrono::{Duration, FixedOffset, Utc};

    use super::*;
    use crate::schema_proto::schema::{data_type::Dtype, value::Variant, IntType};
    use crate::types::DecimalType;
    use crate::{
        schema::Field,
        types::{Between, CompiledRegex, OneOf, StructType},
        Row, Schema,
    };

    #[test]
    fn test_timestamp() {
        let t1 = UTCTimestamp::from(1234567890);
        assert_eq!(t1.micros(), 1234567890);
        let t2 = UTCTimestamp::from_micros(1234567890);
        assert_eq!(t1, t2);
        let t3 = UTCTimestamp::from_millis(1234567890);
        assert_eq!(t3.micros(), 1234567890000);
        let t4 = UTCTimestamp::from_secs(1234567890);
        assert_eq!(t4.micros(), 1234567890000000);
    }

    #[test]
    fn test_date() {
        let date = Date::from(10);
        assert_eq!(date.days(), 10);
        let date = Date::from_milli_secs(1234567890000);
        assert_eq!(date.days(), 14288);
        let date =
            Date::from_naive_date(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).unwrap();
        assert_eq!(date.days(), 0);
        let date = Date::try_from("19700102").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970-01-02").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970-1-2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970/01/02").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970/1/2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("January 2, 1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("Jan 2, 1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("Jan-02-1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("Jan021970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("2 January, 1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("2 Jan, 1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("2-Jan-1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("2Jan1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970, January 2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970, Jan 2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970-Jan-2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970Jan2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970.2").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("2/1970").unwrap();
        assert_eq!(date.days(), 1);
        let date = Date::try_from("1970/2").unwrap();
        assert_eq!(date.days(), 1);
    }

    #[test]
    fn test_timestamp_from_date() {
        let valid_date_strings = vec![
            "2021-11-05",
            "November 05, 2021",
            "November 5, 2021",
            "2021-Nov-05",
            "Nov-05-2021",
            "05-Nov-2021",
            "2021/11/05",
            "20211105",
            "2021.309",
        ];
        for date_string in valid_date_strings {
            let t: UTCTimestamp = date_string.try_into().unwrap();
            assert_eq!(t.micros(), 1636070400000000);
        }

        // The following are reasonable ways to represent date strings but are not supported
        // due to ambiguity in the format
        let invalid_date_strings = vec!["05/11/2021", "11/05/2021", "05-11-2021", "11-05-2021"];
        for date_string in invalid_date_strings {
            let t: Result<UTCTimestamp> = date_string.try_into();
            assert!(
                t.is_err(),
                "{} should not be a valid date string: got {:?}",
                date_string,
                t
            );
        }
    }

    #[test]
    fn test_from_json() {
        let now = 1670840653; // 12 Dec 2022, 2:25am PST
        let as_secs = format!("{}", now);
        let as_millis = format!("{}", now * 1000);
        let as_micros = format!("{}", now * 1000_000);
        let vals = vec![
            (Type::Null, "null", Value::None),
            (Type::Optional(Box::new(Type::Int)), "null", Value::None),
            (Type::Int, "1", Value::Int(1)),
            (Type::Int, "\"1\"", Value::Int(1)),
            (Type::Float, "1.0", Value::Float(1.0)),
            (Type::Float, "\"1\"", Value::Float(1.0)),
            (Type::Float, "\"1.0\"", Value::Float(1.0)),
            (Type::Float, "\"1e-3\"", Value::Float(0.001)),
            (
                Type::String,
                "\"hello\"",
                Value::String(Arc::new("hello".to_string())),
            ),
            (Type::Bool, "true", Value::Bool(true)),
            (
                Type::Timestamp,
                "1",
                Value::Timestamp(UTCTimestamp::from(1000_000)),
            ),
            (
                Type::Timestamp,
                as_secs.as_str(),
                Value::Timestamp(UTCTimestamp::from(now * 1000_000)),
            ),
            (
                Type::Timestamp,
                as_millis.as_str(),
                Value::Timestamp(UTCTimestamp::from(now * 1000_000)),
            ),
            (
                Type::Timestamp,
                as_micros.as_str(),
                Value::Timestamp(UTCTimestamp::from(now * 1000_000)),
            ),
            (
                Type::Timestamp,
                // Can parse RFC3339
                "\"2020-01-01T00:00:00Z\"",
                Value::Timestamp(UTCTimestamp::from(1577836800000000)),
            ),
            (
                Type::Timestamp,
                // can choose the right unit of unix time
                "1577836800000",
                Value::Timestamp(UTCTimestamp::from(1577836800000000)),
            ),
            (
                Type::Timestamp,
                // can choose the right unit of unix time
                "1577836800",
                Value::Timestamp(UTCTimestamp::from(1577836800000000)),
            ),
            (
                Type::Timestamp,
                // can choose the right unit of unix time
                "1577836800000000",
                Value::Timestamp(UTCTimestamp::from(1577836800000000)),
            ),
            (
                Type::Embedding(3),
                "[1.0, 2.0, 3.0]",
                Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])),
            ),
            (
                Type::List(Box::new(Type::Int)),
                "[1, 2, 3]",
                Value::List(Arc::new(
                    List::new(
                        Type::Int,
                        &vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                    )
                    .unwrap(),
                )),
            ),
            (
                Type::List(Box::new(Type::Int)),
                "[1, 2, \"3\"]",
                Value::List(Arc::new(
                    List::new(
                        Type::Int,
                        &vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                    )
                    .unwrap(),
                )),
            ),
            (
                Type::Map(Box::new(Type::Int)),
                "{\"a\": 1, \"b\": 2, \"c\": 3}",
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
            ),
            (
                Type::Map(Box::new(Type::Int)),
                "{\"a\": 1, \"b\": 2, \"c\": \"3\"}",
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
            ),
            (
                Type::Between(
                    Between::new(
                        Type::Float,
                        Value::Float(0.0),
                        Value::Float(2.0),
                        false,
                        false,
                    )
                    .unwrap(),
                ),
                "1.0",
                Value::Float(1.0),
            ),
            (
                Type::Between(
                    Between::new(Type::Int, Value::Int(-1213), Value::Int(123), false, false)
                        .unwrap(),
                ),
                "100",
                Value::Int(100),
            ),
            (
                Type::Regex(CompiledRegex::new(r"[a-zA-Z]+[0-9]".to_string()).unwrap()),
                "\"Rahul9\"",
                Value::String(Arc::new("Rahul9".to_string())),
            ),
            (
                Type::OneOf(
                    OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)])
                        .unwrap(),
                ),
                "1",
                Value::Int(1),
            ),
            (
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
                "\"a\"",
                Value::String(Arc::new("a".to_string())),
            ),
        ];
        for (t, s, v) in vals {
            assert_eq!(Value::from_json(&t, s).unwrap(), v, "type: {:?}", t);
        }
    }

    #[test]
    fn test_from_json_error() {
        let cases = [
            (Type::Null, "1"),
            (Type::Null, ""),
            (Type::Int, "1.0"),
            (Type::Int, "\"1.0\""),
            (Type::Float, "\"hello\""),
            (Type::String, "1"),
            (Type::String, "true"),
            (Type::String, "false"),
            (Type::String, "null"),
            (Type::Bool, "1"),
            (Type::Timestamp, "\"hello\""),
            (Type::Embedding(3), "[1.0, 2.0]"),
            (Type::Embedding(3), "[1.0, 2.0, 3.0, 4.0]"),
            (Type::List(Box::new(Type::Int)), "[1, 2, 3.0]"),
            (Type::Bytes, "1"),
        ];
        for (t, s) in cases {
            assert!(
                Value::from_json(&t, s).is_err(),
                "type: {:?}, string: {:?}",
                t,
                s
            );
        }
    }

    #[test]
    fn test_from_avro_parsed() {
        let raw_schema = r#"
        {
            "type": "record",
            "name": "test",
            "fields": [
                {"name": "int", "type": "int"},
                {"name": "long", "type": "long"},
                {"name": "float", "type": "float"},
                {"name": "double", "type": "double"},
                {"name": "decimal", "type": { "name": "score", "type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2 }},
                {"name": "boolean", "type": "boolean"},
                {"name": "string", "type": "string"},
                {"name": "string_in_opt_string", "type": "string"},
                {"name": "json_string", "type": "string"},
                {"name": "json_string2", "type": "string"},
                {"name": "timestamp_millis", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "timestamp_micros", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "timestamp_rfc3339", "type": "string"},
                {"name": "timestamp_rfc2822", "type": "string"},
                {"name": "timestamp_naive", "type": "string"},
                {"name": "null_int", "type": ["null", "int"]},
                {"name": "null_int2", "type": ["null", "int"]},
                {"name": "null_str", "type": ["null", "string"]},
                {"name": "null_str2", "type": ["null", "string"]},
                {"name": "null_str3", "type": ["null", "string"]},
                {"name": "embedding", "type": {"type": "array", "items": "double"}},
                {"name": "list", "type": {"type": "array", "items": "boolean"}},
                {"name": "map", "type": {"type": "map", "values": "string"}},
                {"name": "struct", "type": {"type": "record", "name": "inner", "fields": [{"name": "id", "type": "int" }, {"name": "name", "type": "string"}]}},
                {"name": "enum", "type": {"type": "enum", "name": "test_enum", "symbols": ["a", "b", "c"], "default": "c"}},
                {"name": "some_random_xdf", "type": "int"},
                {"name": "null1", "type": "null"},
                {"name": "null2", "type": ["null", "int"]}
            ]
        }
        "#;
        let avro_schema = AvroSchema::parse_str(raw_schema).unwrap();
        let value_schema = Type::Struct(Box::new(
            StructType::new(
                "test_struct".into(),
                vec![
                    Field::new("int", Type::Int),
                    Field::new("long", Type::Int),
                    Field::new("float", Type::Float),
                    Field::new("double", Type::Float),
                    Field::new("decimal", Type::Float),
                    Field::new("boolean", Type::Bool),
                    Field::new("string", Type::String),
                    Field::new(
                        "string_in_opt_string",
                        Type::Optional(Box::new(Type::String)),
                    ),
                    Field::new(
                        "json_string",
                        Type::Struct(Box::new(
                            StructType::new("unnmaed".into(), vec![Field::new("x", Type::Int)])
                                .unwrap(),
                        )),
                    ),
                    Field::new("json_string2", Type::Map(Box::new(Type::Int))),
                    Field::new("timestamp_millis", Type::Timestamp),
                    Field::new("timestamp_micros", Type::Timestamp),
                    Field::new("timestamp_rfc3339", Type::Timestamp),
                    Field::new("timestamp_rfc2822", Type::Timestamp),
                    Field::new("timestamp_naive", Type::Timestamp),
                    Field::new("null_int", Type::Optional(Box::new(Type::Int))),
                    Field::new("null_int2", Type::Optional(Box::new(Type::Int))),
                    Field::new("null_str", Type::Optional(Box::new(Type::String))),
                    Field::new("null_str2", Type::Optional(Box::new(Type::String))),
                    Field::new("null_str3", Type::Optional(Box::new(Type::String))),
                    // this field is not present in the avro schema - we should
                    // parse it as None
                    Field::new(
                        "optional_missing_bool",
                        Type::Optional(Box::new(Type::Bool)),
                    ),
                    Field::new("embedding", Type::Embedding(3)),
                    Field::new("list", Type::List(Box::new(Type::Bool))),
                    Field::new("map", Type::Map(Box::new(Type::String))),
                    Field::new(
                        "struct",
                        Type::Struct(Box::new(
                            StructType::new(
                                "inner".into(),
                                vec![
                                    Field::new("id", Type::Int),
                                    Field::new("name", Type::String),
                                ],
                            )
                            .unwrap(),
                        )),
                    ),
                    Field::new("null1", Type::Null),
                    Field::new("null2", Type::Null),
                    Field::new("enum", Type::String),
                ],
            )
            .unwrap(),
        ));
        let mut record = AvroRecord::new(&avro_schema).unwrap();
        record.put("int", 3);
        record.put("long", 4i64);
        record.put("float", 5.0f32);
        record.put("double", 6.0);
        record.put("decimal", AvroValue::Decimal(2486u64.to_be_bytes().into()));
        record.put("boolean", true);
        record.put("string", "foobar");
        record.put("string_in_opt_string", "foobar");
        record.put("json_string", r#"{"x":42}"#);
        record.put("json_string2", r#"{"y":42}"#);
        record.put("timestamp_millis", 100i64);
        record.put("timestamp_micros", 200i64);
        record.put("timestamp_rfc3339", "1996-12-19T16:39:57-08:00");
        record.put("timestamp_rfc2822", "Thu, 19 Dec 1996 16:39:57 -0800");
        record.put("timestamp_naive", "2022-12-08 10:06:28.085000000");
        record.put("null_int", Some(3));
        record.put("null_int2", Option::<i32>::None);
        record.put("null_str", Some("foo"));
        record.put("null_str2", Option::<&str>::None);
        record.put("null_str3", Some("bar"));
        record.put(
            "embedding",
            AvroValue::Array(vec![1.0.into(), 2.0.into(), 3.0.into()]),
        );
        record.put(
            "list",
            AvroValue::Array(vec![true.into(), false.into(), true.into()]),
        );
        record.put(
            "map",
            AvroValue::Map(HashMap::from([("a".to_string(), "b".into())])),
        );
        record.put(
            "struct",
            AvroValue::Record(vec![
                ("id".to_string(), 20.into()),
                ("name".to_string(), "foo".into()),
            ]),
        );
        record.put("null1", AvroValue::Null);
        record.put("null2", AvroValue::Null);
        record.put("enum", AvroValue::Enum(0, "a".to_string()));
        // add a field that we will not read back - make sure we don't crash
        record.put("some_random_xdf", AvroValue::Int(1));

        let mut writer = AvroWriter::new(&avro_schema, Vec::new());
        writer.append(record).unwrap();
        let encoded = writer.into_inner().unwrap();
        let reader = AvroReader::with_schema(&avro_schema, &encoded[..]).unwrap();
        let avro_value = reader.into_iter().next().unwrap().unwrap();
        let parsed_value =
            Value::from_avro_parsed(&value_schema, avro_value, &avro_schema, None).unwrap();

        let rfc_dt = FixedOffset::west_opt(8 * 3600)
            .unwrap()
            .with_ymd_and_hms(1996, 12, 19, 16, 39, 57)
            .unwrap();
        let naive_dt = Utc.with_ymd_and_hms(2022, 12, 8, 10, 6, 28).unwrap()
            + Duration::nanoseconds(085000000);

        let expected = vec![
            ("int", Value::Int(3)),
            ("long", Value::Int(4)),
            ("float", Value::Float(5.0)),
            ("double", Value::Float(6.0)),
            ("decimal", Value::Float(24.86)),
            ("boolean", Value::Bool(true)),
            ("string", Value::String(Arc::new(String::from("foobar")))),
            (
                "string_in_opt_string",
                Value::String(Arc::new(String::from("foobar"))),
            ),
            (
                "json_string",
                Value::Struct(Arc::new(Struct {
                    data: vec![("x".into(), Value::Int(42))],
                })),
            ),
            (
                "json_string2",
                Value::Map(Arc::new(
                    Map::new(Type::Int, &[("y".to_owned(), Value::Int(42))]).unwrap(),
                )),
            ),
            (
                "timestamp_millis",
                Value::Timestamp(UTCTimestamp::from_millis(100)),
            ),
            (
                "timestamp_micros",
                Value::Timestamp(UTCTimestamp::from_micros(200)),
            ),
            (
                "timestamp_rfc3339",
                Value::Timestamp(UTCTimestamp::from_micros(rfc_dt.timestamp_micros())),
            ),
            (
                "timestamp_rfc2822",
                Value::Timestamp(UTCTimestamp::from_micros(rfc_dt.timestamp_micros())),
            ),
            (
                "timestamp_naive",
                Value::Timestamp(UTCTimestamp::from_micros(naive_dt.timestamp_micros())),
            ),
            ("null_int", Value::Int(3)),
            ("null_int2", Value::None),
            ("null_str", Value::String(Arc::new(String::from("foo")))),
            ("null_str2", Value::None),
            ("null_str3", Value::String(Arc::new(String::from("bar")))),
            ("optional_missing_bool", Value::None),
            ("embedding", Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0]))),
            (
                "list",
                Value::List(Arc::new(
                    List::new(
                        Type::Bool,
                        &[Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                    )
                    .unwrap(),
                )),
            ),
            (
                "map",
                Value::Map(Arc::new(
                    Map::new(
                        Type::String,
                        &[("a".to_string(), Value::String(Arc::new("b".to_string())))],
                    )
                    .unwrap(),
                )),
            ),
            (
                "struct",
                Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("id".into(), Value::Int(20)),
                        ("name".into(), Value::String(Arc::new("foo".to_string()))),
                    ])
                    .unwrap(),
                )),
            ),
            ("null1", Value::None),
            ("null2", Value::None),
            ("enum", Value::String(Arc::new("a".to_string()))),
        ];
        let parsed_value = parsed_value.as_struct(false).unwrap();
        for (f, v) in expected {
            assert_eq!(parsed_value.unwrap().get(f), Some(&v));
        }
    }

    #[test]
    fn test_from_json_parsed() {
        let json = r#"{
            "int": 1,
            "int_str": "1",
            "float": 1.0,
            "float_str": "1.0",
            "string": "hello",
            "nullable": null,
            "nullable_str": "\"\"",
            "nullable_str_empty": "",
            "nullable_int": "",
            "bool": true,
            "bool_str": "true",
            "timestamp": 1,
            "timestamp_epoch_str": "1",
            "timestamp_rfc3339": "1996-12-19T16:39:57-08:00",
            "timestamp_rfc2822": "Thu, 19 Dec 1996 16:39:57 -0800",
            "timestamp_naive": "2022-12-08 10:06:28.085000000",
            "embedding": [1.0, 2.0, 3.0],
            "embedding_str": "[1.0, 2.0, 3.0]",
            "list": [true, false, true],
            "list_str": "[true, false, true]",
            "map": {"a": 1, "b": 2, "c": 3},
            "map_str": "{\"a\": 1, \"b\": 2, \"c\": 3}",
            "null_field": null
        }"#;
        let parsed = serde_json::from_str::<serde_json::Value>(json).unwrap();
        assert_eq!(
            Value::from_json_parsed(&Type::Null, &parsed["null_field"]).unwrap(),
            Value::None,
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Int, &parsed["int"]).unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Optional(Box::new(Type::Int)), &parsed["int"]).unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Float, &parsed["float"]).unwrap(),
            Value::Float(1.0)
        );
        assert_eq!(
            Value::from_json_parsed(&Type::String, &parsed["string"]).unwrap(),
            Value::String(Arc::new("hello".to_string()))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Optional(Box::new(Type::Int)), &parsed["nullable"])
                .unwrap(),
            Value::None
        );
        assert_eq!(
            Value::from_json_parsed(
                &Type::Optional(Box::new(Type::String)),
                &parsed["nullable_str"],
            )
            .unwrap(),
            Value::String(Arc::new("\"\"".to_string()))
        );
        assert_eq!(
            Value::from_json_parsed(
                &Type::Optional(Box::new(Type::String)),
                &parsed["nullable_str_empty"],
            )
            .unwrap(),
            Value::String(Arc::new("".to_string()))
        );
        assert_eq!(
            Value::from_json_parsed(
                &Type::Optional(Box::new(Type::Int)),
                &parsed["nullable_int"],
            )
            .unwrap(),
            Value::None
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Bool, &parsed["bool"]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Timestamp, &parsed["timestamp"]).unwrap(),
            Value::Timestamp(UTCTimestamp::from(1000_000))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Timestamp, &parsed["timestamp_epoch_str"]).unwrap(),
            Value::Timestamp(UTCTimestamp::from(1000_000))
        );
        let dt = FixedOffset::west_opt(8 * 3600)
            .unwrap()
            .with_ymd_and_hms(1996, 12, 19, 16, 39, 57)
            .unwrap();
        assert_eq!(
            Value::from_json_parsed(&Type::Timestamp, &parsed["timestamp_rfc3339"]).unwrap(),
            Value::Timestamp(UTCTimestamp::from(dt.timestamp_micros()))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Timestamp, &parsed["timestamp_rfc2822"]).unwrap(),
            Value::Timestamp(UTCTimestamp::from(dt.timestamp_micros()))
        );
        let dt = Utc.with_ymd_and_hms(2022, 12, 8, 10, 6, 28).unwrap()
            + Duration::nanoseconds(085000000);
        assert_eq!(
            Value::from_json_parsed(&Type::Timestamp, &parsed["timestamp_naive"]).unwrap(),
            Value::Timestamp(UTCTimestamp::from(dt.timestamp_micros()))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Embedding(3), &parsed["embedding"]).unwrap(),
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0]))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Embedding(3), &parsed["embedding"]).unwrap(),
            Value::from_json_parsed(&Type::Embedding(3), &parsed["embedding_str"]).unwrap(),
        );
        assert_eq!(
            Value::from_json_parsed(&Type::List(Box::new(Type::Bool)), &parsed["list"]).unwrap(),
            Value::List(Arc::new(
                List::new(
                    Type::Bool,
                    &vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                )
                .unwrap()
            ))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::List(Box::new(Type::Bool)), &parsed["list"]).unwrap(),
            Value::from_json_parsed(&Type::List(Box::new(Type::Bool)), &parsed["list_str"])
                .unwrap(),
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Map(Box::new(Type::Int)), &parsed["map"]).unwrap(),
            Value::Map(Arc::new(
                Map::new(
                    Type::Int,
                    &vec![
                        ("a".to_string(), Value::Int(1)),
                        ("b".to_string(), Value::Int(2)),
                        ("c".to_string(), Value::Int(3)),
                    ],
                )
                .unwrap()
            ))
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Map(Box::new(Type::Int)), &parsed["map"]).unwrap(),
            Value::from_json_parsed(&Type::Map(Box::new(Type::Int)), &parsed["map_str"]).unwrap(),
        );
        assert_eq!(
            Value::from_json_parsed(&Type::Optional(Box::new(Type::Int)), &parsed["not_exist"])
                .unwrap(),
            Value::None
        );
        Value::from_json_parsed(&Type::Int, &parsed["not_exist"]).unwrap_err();
        // Test that we can parse a json object into a string
        let parsed =
            serde_json::from_str::<serde_json::Value>(r#"{"a": 1, "b": 2, "c": 3}"#).unwrap();
        let value = Value::from_json_parsed(&Type::String, &parsed).unwrap();
        assert_eq!(
            value,
            Value::String(Arc::new(r#"{"a":1,"b":2,"c":3}"#.to_string()))
        );

        let json = r#"{
            "int": 1,
            "float": 1.0,
            "string": "hello"
        }"#;
        let parsed = serde_json::from_str::<serde_json::Value>(json).unwrap();
        let value = Value::from_json_parsed(&Type::String, &parsed).unwrap();
        assert_eq!(
            value,
            Value::String(Arc::new(
                r#"{"int":1,"float":1.0,"string":"hello"}"#.to_string()
            ))
        );
    }

    #[test]
    fn test_struct_from_json_parsed() {
        // create a json object and attempt to parse it as a struct
        let json = r#"{
            "json_object": {
                "int": 1,
                "float": 1.0,
                "string": "hello"
            }
        }"#;
        let parsed = serde_json::from_str::<serde_json::Value>(json).unwrap();
        // matches the json object to the struct type
        {
            let struct_type = StructType::new(
                "test".into(),
                vec![Field::new(
                    "json_object",
                    Type::Struct(Box::new(
                        StructType::new(
                            "inner".into(),
                            vec![
                                Field::new("int", Type::Int),
                                Field::new("float", Type::Float),
                                Field::new("string", Type::String),
                            ],
                        )
                        .unwrap(),
                    )),
                )],
            )
            .unwrap();
            let value =
                Value::from_json_parsed(&Type::Struct(Box::new(struct_type)), &parsed).unwrap();
            let value = value.as_struct(false).unwrap().unwrap();
            assert_eq!(
                value.get("json_object").unwrap(),
                &Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("int".into(), Value::Int(1)),
                        ("float".into(), Value::Float(1.0)),
                        (
                            "string".into(),
                            Value::String(Arc::new("hello".to_string()))
                        ),
                    ])
                    .unwrap(),
                ))
            );
        }
        // struct type is subset of the json object
        {
            let struct_type = StructType::new(
                "test".into(),
                vec![Field::new(
                    "json_object",
                    Type::Struct(Box::new(
                        StructType::new(
                            "inner".into(),
                            vec![
                                Field::new("int", Type::Int),
                                Field::new("float", Type::Float),
                            ],
                        )
                        .unwrap(),
                    )),
                )],
            )
            .unwrap();
            let value =
                Value::from_json_parsed(&Type::Struct(Box::new(struct_type)), &parsed).unwrap();
            let value = value.as_struct(false).unwrap().unwrap();
            assert_eq!(
                value.get("json_object").unwrap(),
                &Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("int".into(), Value::Int(1)),
                        ("float".into(), Value::Float(1.0)),
                    ])
                    .unwrap(),
                ))
            );
        }
        // match the json object since the struct type has a nullable field
        {
            let struct_type = StructType::new(
                "test".into(),
                vec![Field::new(
                    "json_object",
                    Type::Struct(Box::new(
                        StructType::new(
                            "inner".into(),
                            vec![
                                Field::new("int", Type::Int),
                                Field::new("float", Type::Float),
                                Field::new("missing_field", Type::Optional(Box::new(Type::String))),
                            ],
                        )
                        .unwrap(),
                    )),
                )],
            )
            .unwrap();
            let value =
                Value::from_json_parsed(&Type::Struct(Box::new(struct_type)), &parsed).unwrap();
            let value = value.as_struct(false).unwrap().unwrap();
            assert_eq!(
                value.get("json_object").unwrap(),
                &Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("int".into(), Value::Int(1)),
                        ("float".into(), Value::Float(1.0)),
                        ("missing_field".into(), Value::None),
                    ])
                    .unwrap(),
                ))
            );
        }
        // field mismatch - a required field in struct is not present in json object
        {
            let struct_type = StructType::new(
                "test".into(),
                vec![Field::new(
                    "json_object",
                    Type::Struct(Box::new(
                        StructType::new(
                            "inner".into(),
                            vec![
                                Field::new("int", Type::Int),
                                Field::new("float", Type::Float),
                                Field::new("missing_field", Type::String),
                            ],
                        )
                        .unwrap(),
                    )),
                )],
            )
            .unwrap();
            Value::from_json_parsed(&Type::Struct(Box::new(struct_type)), &parsed).unwrap_err();
        }
    }

    #[test]
    fn test_similar_hash_values() {
        let vals = vec![
            (
                Value::List(Arc::new(
                    List::new(
                        Type::Int,
                        &vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                    )
                    .unwrap(),
                )),
                Value::List(Arc::new(
                    List::new(
                        Type::List(Box::new(Type::Int)),
                        &vec![
                            Value::List(Arc::new(
                                List::new(Type::Int, &vec![Value::Int(1)]).unwrap(),
                            )),
                            Value::List(Arc::new(
                                List::new(Type::Int, &vec![Value::Int(2), Value::Int(3)]).unwrap(),
                            )),
                        ],
                    )
                    .unwrap(),
                )),
            ),
            (Value::Bool(false), Value::Int(0)),
            (Value::Bool(true), Value::Int(1)),
            (Value::Int(123), Value::String(Arc::new("123".to_string()))),
            (
                Value::Struct(Arc::new(
                    Struct::new(vec![("A".into(), Value::Int(1))]).unwrap(),
                )),
                Value::Struct(Arc::new(
                    Struct::new(vec![("B".into(), Value::Int(1))]).unwrap(),
                )),
            ),
            (
                Value::Bytes(Bytes::from("hello")),
                Value::Bytes(Bytes::from(" hello")),
            ),
        ];
        for (v1, v2) in vals {
            let hash1 = v1.hash().unwrap();
            let hash2 = v2.hash().unwrap();
            assert_ne!(hash1, hash2);
        }
    }

    #[test]
    fn test_hash_values() {
        let vals: Vec<(Value, u64)> = vec![
            (Value::Int(1), 581821386313390087),
            (
                Value::String(Arc::new("hello".to_string())),
                13615883683139929088,
            ),
            (Value::None, 14144645293874801883),
            (Value::Bool(true), 16169524375275942869),
            (Value::Bool(false), 8386526569404862294),
            (
                Value::Timestamp(UTCTimestamp::from(1000_000)),
                14100193952460311549,
            ),
            (
                Value::List(Arc::new(
                    List::new(
                        Type::Bool,
                        &vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                    )
                    .unwrap(),
                )),
                14727303311716914797,
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
                4610246088047781597,
            ),
            (
                Value::Bytes(Bytes::from(vec![1, 2, 3])),
                13318110907574486043,
            ),
            (
                Value::Struct(Arc::new(
                    Struct::new(vec![
                        ("A".into(), Value::Int(1)),
                        (
                            "B".into(),
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
                        ),
                    ])
                    .unwrap(),
                )),
                12685330406246948210,
            ),
        ];
        for (v, s) in vals {
            let hash = v.hash().unwrap();
            assert_eq!(hash, s, "hash mismatch for {:?}", v);
        }
    }

    #[test]
    fn test_hash_corner_case() {
        let x = "x";
        let y = "y";
        // Case A: list of single string s, such that s = x + codec for string + y
        let s = format!(
            "{}{}{}",
            x,
            Value::get_codec(&Value::String(Arc::new(x.to_string()))),
            y
        );
        let v = Value::List(Arc::new(
            List::new(Type::String, &vec![Value::String(Arc::new(s.to_string()))]).unwrap(),
        ));
        let hash1 = v.hash().unwrap();

        // Case B: list of two strings x, y
        let v = Value::List(Arc::new(
            List::new(
                Type::String,
                &vec![
                    Value::String(Arc::new(x.to_string())),
                    Value::String(Arc::new(y.to_string())),
                ],
            )
            .unwrap(),
        ));
        let hash2 = v.hash().unwrap();
        assert_ne!(hash1, hash2, "Hashes for case A and B should be different");
    }

    #[test]
    fn test_to_json() {
        let vals = vec![
            (Value::Int(1), "1"),
            (Value::Float(1.0), "1.0"),
            (Value::Float(f64::NAN), "\"NaN\""),
            (Value::Float(f64::INFINITY), "\"+infinity\""),
            (Value::Float(f64::NEG_INFINITY), "\"-infinity\""),
            (Value::String(Arc::new("hello".to_string())), "\"hello\""),
            (Value::None, "null"),
            (Value::Bool(true), "true"),
            (Value::Bool(false), "false"),
            // this returns timestamp in rfc3339 format
            (
                // 1 second
                Value::Timestamp(UTCTimestamp::from(1000_000)),
                "\"1970-01-01T00:00:01.000000Z\"",
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
            assert_eq!(v.to_json().to_string(), s);
        }

        // on a valid `Value`, there should be no error to transform to a JSON Value
    }

    #[test]
    fn test_encode_decode() {
        let pairs = vec![
            (Value::None, Type::Null),
            (Value::None, Type::Optional(Box::new(Type::Int))),
            (Value::None, Type::Optional(Box::new(Type::String))),
            (Value::None, Type::Optional(Box::new(Type::Float))),
            (Value::None, Type::Optional(Box::new(Type::Bool))),
            (Value::None, Type::Optional(Box::new(Type::Timestamp))),
            (
                Value::None,
                Type::Optional(Box::new(Type::List(Box::new(Type::Int)))),
            ),
            (
                Value::None,
                Type::Optional(Box::new(Type::Map(Box::new(Type::Int)))),
            ),
            (Value::None, Type::Optional(Box::new(Type::Embedding(5)))),
            (Value::Int(1), Type::Int),
            (Value::Int(5), Type::Int),
            (Value::Int(-1), Type::Int),
            (Value::Int(-5), Type::Int),
            (Value::Float(1.0), Type::Float),
            (Value::Float(5.0), Type::Float),
            (Value::Float(-1.0), Type::Float),
            (Value::Float(-5.0), Type::Float),
            (Value::String(Arc::new("hello".to_string())), Type::String),
            (Value::String(Arc::new("world".to_string())), Type::String),
            (Value::Bytes(Bytes::from(vec![1, 2, 3])), Type::Bytes),
            (Value::Bool(true), Type::Bool),
            (Value::Bool(false), Type::Bool),
            (
                Value::Bytes(Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
                Type::Bytes,
            ),
            (
                Value::Bytes(Bytes::from("my name is anthony gonsalves")),
                Type::Bytes,
            ),
            (Value::Timestamp(UTCTimestamp::from(1)), Type::Timestamp),
            (
                Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])),
                Type::Embedding(3),
            ),
            (
                Value::List(Arc::new(List {
                    dtype: Type::Int,
                    data: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                })),
                Type::List(Box::new(Type::Int)),
            ),
            (
                Value::List(Arc::new(List {
                    dtype: Type::Optional(Box::new(Type::Bool)),
                    data: vec![Value::Bool(true), Value::Bool(false)],
                })),
                Type::List(Box::new(Type::Optional(Box::new(Type::Bool)))),
            ),
            (
                Value::Map(Arc::new(Map {
                    dtype: Type::Int,
                    data: vec![
                        ("a".to_string(), Value::Int(1)),
                        ("b".to_string(), Value::Int(2)),
                        ("c".to_string(), Value::Int(3)),
                    ],
                })),
                Type::Map(Box::new(Type::Int)),
            ),
        ];
        for (v, dtype) in &pairs {
            let mut buf = vec![];
            v.encode(dtype, &mut buf).unwrap();
            let mut r = buf.as_slice();
            assert_eq!(
                &Value::decode(&mut r, dtype)
                    .expect(format!("failed to decode: {:#?}", v).as_str()),
                v,
            );
        }
        let mut buf = vec![];
        let vals = pairs.iter().map(|(v, _)| v).collect::<Vec<_>>();
        let dtypes = pairs.iter().map(|(_, dtype)| dtype).collect::<Vec<_>>();
        Value::encode_many(&mut buf, vals.as_slice(), dtypes.as_slice()).unwrap();
        let got = Value::decode_many(&mut buf.as_slice(), &mut dtypes.into_iter()).unwrap();
        for (v, g) in vals.iter().zip(got.iter()) {
            assert_eq!(v, &g);
        }
    }

    #[test]
    fn test_encode_decode_fail() {
        let pairs = vec![
            (Value::Int(1), Type::Null),
            (Value::None, Type::Int),
            (Value::None, Type::String),
            (Value::Int(1), Type::String),
            (
                Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])),
                Type::Embedding(2),
            ),
            (
                Value::List(Arc::new(List {
                    dtype: Type::Int,
                    data: vec![Value::Int(1), Value::Int(2), Value::Int(3)],
                })),
                Type::List(Box::new(Type::String)),
            ),
            (
                Value::Map(Arc::new(Map {
                    dtype: Type::Int,
                    data: vec![
                        ("a".to_string(), Value::Int(1)),
                        ("b".to_string(), Value::Int(2)),
                        ("c".to_string(), Value::Int(3)),
                    ],
                })),
                Type::Map(Box::new(Type::String)),
            ),
        ];
        for (v, dtype) in &pairs {
            let mut buf = vec![];
            v.encode(dtype, &mut buf).unwrap_err();
        }
    }

    #[test]
    fn test_value_matches() {
        assert!(Value::None.matches(&Type::Null));
        assert!(Value::None.matches(&Type::Optional(Box::new(Type::Int))));
        assert!(Value::Int(1).matches(&Type::Int));
        assert!(Value::Int(1).matches(&Type::Optional(Box::new(Type::Int))));
        assert!(Value::Float(1.0).matches(&Type::Float));
        assert!(Value::Float(1.0).matches(&Type::Optional(Box::new(Type::Float))));
        assert!(Value::String(Arc::new("hello".to_string())).matches(&Type::String));
        assert!(Value::String(Arc::new("hello".to_string()))
            .matches(&Type::Optional(Box::new(Type::String))));
        assert!(Value::Bool(true).matches(&Type::Bool));
        assert!(Value::Bool(false).matches(&Type::Bool));
        assert!(Value::Bool(true).matches(&Type::Optional(Box::new(Type::Bool))));
        assert!(Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).matches(&Type::Embedding(3)));
        assert!(Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0]))
            .matches(&Type::Optional(Box::new(Type::Embedding(3)))));
        assert!(Value::None.matches(&Type::Optional(Box::new(Type::Embedding(3)))));
        assert!(Value::Timestamp(UTCTimestamp::from(1)).matches(&Type::Timestamp));
        assert!(Value::Timestamp(UTCTimestamp::from(1))
            .matches(&Type::Optional(Box::new(Type::Timestamp))));
        assert!(Value::List(Arc::new(
            List::new(Type::Int, &vec![Value::Int(1), Value::Int(2)]).unwrap()
        ))
        .matches(&Type::List(Box::new(Type::Int))));
        assert!(Value::Map(Arc::new(
            Map::new(Type::Int, &vec![("hi".to_string(), Value::Int(1))]).unwrap()
        ))
        .matches(&Type::Map(Box::new(Type::Int))));

        assert!(Value::Float(2.0).matches(&Type::Between(
            Between::new(
                Type::Float,
                Value::Float(1.0),
                Value::Float(3.0),
                false,
                false,
            )
            .unwrap()
        )));
        assert!(Value::Float(2.0).matches(&Type::Between(
            Between::new(
                Type::Float,
                Value::Float(1.0),
                Value::Float(3.0),
                true,
                true,
            )
            .unwrap()
        )));
        assert!(Value::Int(2).matches(&Type::Between(
            Between::new(Type::Int, Value::Int(1), Value::Int(3), false, false).unwrap()
        )));
        assert!(Value::Int(3).matches(&Type::Between(
            Between::new(Type::Int, Value::Int(1), Value::Int(3), true, false).unwrap()
        )));
        assert!(Value::Int(1).matches(&Type::Between(
            Between::new(Type::Int, Value::Int(1), Value::Int(3), false, true).unwrap()
        )));
        assert!(Value::Int(2).matches(&Type::OneOf(
            OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap()
        )));
        assert!(
            Value::String(Arc::new("hello".to_string())).matches(&Type::OneOf(
                OneOf::new(
                    Type::String,
                    vec![
                        Value::String(Arc::new("hello".to_string())),
                        Value::String(Arc::new("world".to_string())),
                    ],
                )
                .unwrap()
            ))
        );
        assert!(
            Value::String(Arc::new("Rahul9".to_string())).matches(&Type::Regex(
                CompiledRegex::new(r"[a-zA-Z]+[0-9]".to_string()).unwrap()
            ))
        );
    }

    #[test]
    fn test_value_matches_optional() {
        // verify that optional(optional(T)) matches all values that match optional(T)
        assert!(
            Value::None.matches(&Type::Optional(Box::new(Type::Optional(Box::new(
                Type::Int
            )))))
        );
        assert!(
            Value::Int(1).matches(&Type::Optional(Box::new(Type::Optional(Box::new(
                Type::Int
            )))))
        );
        assert!(
            Value::Float(1.0).matches(&Type::Optional(Box::new(Type::Optional(Box::new(
                Type::Float
            )))))
        );
        assert!(
            Value::String(Arc::new("hello".to_string())).matches(&Type::Optional(Box::new(
                Type::Optional(Box::new(Type::String))
            )))
        );
        assert!(
            Value::Bool(true).matches(&Type::Optional(Box::new(Type::Optional(Box::new(
                Type::Bool
            )))))
        );
        assert!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).matches(&Type::Optional(Box::new(
                Type::Optional(Box::new(Type::Embedding(3)))
            )))
        );
        assert!(
            Value::Timestamp(UTCTimestamp::from(1)).matches(&Type::Optional(Box::new(
                Type::Optional(Box::new(Type::Timestamp))
            )))
        );
        assert!(Value::List(Arc::new(
            List::new(Type::Int, &vec![Value::Int(1), Value::Int(2)]).unwrap()
        ))
        .matches(&Type::Optional(Box::new(Type::Optional(Box::new(
            Type::List(Box::new(Type::Int))
        ))))));
    }

    #[test]
    fn test_value_does_not_match_type() {
        assert!(!Value::None.matches(&Type::Int));
        assert!(!Value::None.matches(&Type::Float));
        assert!(!Value::None.matches(&Type::String));
        assert!(!Value::None.matches(&Type::Bool));
        assert!(!Value::None.matches(&Type::Embedding(3)));
        assert!(!Value::None.matches(&Type::List(Box::new(Type::Int))));
        assert!(!Value::None.matches(&Type::Map(Box::new(Type::Int))));

        assert!(!Value::Int(0).matches(&Type::Null));
        assert!(!Value::Int(1).matches(&Type::Float));
        assert!(!Value::Int(1).matches(&Type::Optional(Box::new(Type::Float))));
        assert!(!Value::Int(1).matches(&Type::String));
        assert!(!Value::Int(1).matches(&Type::Bool));
        assert!(!Value::Int(1).matches(&Type::Embedding(3)));
        assert!(!Value::Int(1).matches(&Type::List(Box::new(Type::Int))));
        assert!(!Value::Int(1).matches(&Type::Map(Box::new(Type::Int))));

        assert!(!Value::Float(0.0).matches(&Type::Null));
        assert!(!Value::Float(1.0).matches(&Type::Int));
        assert!(!Value::Float(1.0).matches(&Type::Optional(Box::new(Type::Int))));
        assert!(!Value::Float(1.0).matches(&Type::String));
        assert!(!Value::Float(1.0).matches(&Type::Bool));
        assert!(!Value::Float(1.0).matches(&Type::Embedding(3)));
        assert!(!Value::Float(1.0).matches(&Type::List(Box::new(Type::Int))));
        assert!(!Value::Float(1.0).matches(&Type::Map(Box::new(Type::Int))));

        assert!(!Value::String(Arc::new("hello".to_string())).matches(&Type::Null));
        assert!(!Value::String(Arc::new("hello".to_string())).matches(&Type::Int));
        assert!(!Value::String(Arc::new("hello".to_string()))
            .matches(&Type::Optional(Box::new(Type::Int))));
        assert!(!Value::String(Arc::new("hello".to_string())).matches(&Type::Float));
        assert!(!Value::String(Arc::new("hello".to_string())).matches(&Type::Bool));
        assert!(!Value::String(Arc::new("hello".to_string())).matches(&Type::Embedding(3)));
        assert!(!Value::String(Arc::new("hello".to_string()))
            .matches(&Type::List(Box::new(Type::String))));
        assert!(!Value::String(Arc::new("hello".to_string()))
            .matches(&Type::Map(Box::new(Type::String))));

        assert!(!Value::Bool(true).matches(&Type::Int));
        assert!(!Value::Bool(true).matches(&Type::Optional(Box::new(Type::Int))));
        assert!(!Value::Bool(true).matches(&Type::Float));
        assert!(!Value::Bool(true).matches(&Type::String));
        assert!(!Value::Bool(true).matches(&Type::Embedding(3)));
        assert!(!Value::Bool(true).matches(&Type::List(Box::new(Type::Int))));
        assert!(!Value::Bool(true).matches(&Type::Map(Box::new(Type::Int))));

        // embeddings match only if the length matches
        assert!(!Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).matches(&Type::Embedding(4)));
        assert!(!Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).matches(&Type::Embedding(2)));
        assert!(!Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0]))
            .matches(&Type::Optional(Box::new(Type::Embedding(4)))));

        // lists of different types are different
        assert!(!Value::List(Arc::new(
            List::new(Type::Int, &vec![Value::Int(1), Value::Int(2)]).unwrap()
        ))
        .matches(&Type::List(Box::new(Type::Float))));

        // lists of lists are different from lists
        assert!(!Value::List(Arc::new(
            List::new(Type::Int, &vec![Value::Int(1), Value::Int(2)]).unwrap()
        ))
        .matches(&Type::List(Box::new(Type::List(Box::new(Type::Int))))));

        // maps of different types are different
        assert!(!Value::Map(Arc::new(
            Map::new(Type::Int, &vec![("hi".to_string(), Value::Int(1))]).unwrap()
        ))
        .matches(&Type::Map(Box::new(Type::Float))));

        // maps of maps are different from maps
        assert!(!Value::Map(Arc::new(
            Map::new(Type::Int, &vec![("hi".to_string(), Value::Int(1))]).unwrap()
        ))
        .matches(&Type::Map(Box::new(Type::Map(Box::new(Type::Int))))));
        assert!(!Value::Float(4.0).matches(&Type::Between(
            Between::new(
                Type::Float,
                Value::Float(1.0),
                Value::Float(3.0),
                false,
                false,
            )
            .unwrap()
        )));
        assert!(!Value::Float(3.0).matches(&Type::Between(
            Between::new(
                Type::Float,
                Value::Float(1.0),
                Value::Float(3.0),
                true,
                true,
            )
            .unwrap()
        )));
        assert!(!Value::Int(123).matches(&Type::Between(
            Between::new(Type::Int, Value::Int(1), Value::Int(3), false, false).unwrap()
        )));
        assert!(!Value::Int(3).matches(&Type::Between(
            Between::new(Type::Int, Value::Int(1), Value::Int(3), true, true).unwrap()
        )));
        assert!(!Value::Int(1).matches(&Type::Between(
            Between::new(Type::Int, Value::Int(1), Value::Int(3), true, true).unwrap()
        )));
        assert!(!Value::Int(4).matches(&Type::OneOf(
            OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap()
        )));
        assert!(
            !Value::String(Arc::new("he2llo".to_string())).matches(&Type::OneOf(
                OneOf::new(
                    Type::String,
                    vec![
                        Value::String(Arc::new("hello".to_string())),
                        Value::String(Arc::new("world".to_string())),
                    ],
                )
                .unwrap()
            ))
        );
    }

    #[test]
    fn test_typecast_int() {
        matches!(Value::Int(1).as_int(), Ok(1));
        matches!(Value::Int(1).as_float(), Err { .. });
        matches!(Value::Int(1).as_bool(), Err { .. });
        matches!(Value::Int(1).as_str(), Err { .. });
        matches!(Value::Int(1).as_embedding(), Err { .. });
        matches!(Value::Int(1).as_timestamp(), Err { .. });
        matches!(Value::Int(1).as_list(), Err { .. });
        matches!(Value::Int(1).as_map(), Err { .. });
    }

    #[test]
    fn test_typecast_float() {
        matches!(Value::Float(1.0).as_int(), Err { .. });
        // doing a float comparison here because of warning about
        // rounding errors in the match
        match Value::Float(1.0).as_float() {
            Ok(f) => assert_eq!(f, 1.0),
            Err(e) => panic!("Unexpected error: {}", e),
        }
        matches!(Value::Float(1.0).as_bool(), Err { .. });
        matches!(Value::Float(1.0).as_str(), Err { .. });
        matches!(Value::Float(1.0).as_embedding(), Err { .. });
        matches!(Value::Float(1.0).as_timestamp(), Err { .. });
        matches!(Value::Float(1.0).as_list(), Err { .. });
        matches!(Value::Float(1.0).as_map(), Err { .. });
    }

    #[test]
    fn test_typecast_string() {
        matches!(
            Value::String(Arc::new("hello".to_string())).as_str(),
            Ok("hello")
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_int(),
            Err { .. }
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_float(),
            Err { .. }
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_bool(),
            Err { .. }
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_timestamp(),
            Err { .. }
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_embedding(),
            Err { .. }
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_list(),
            Err { .. }
        );
        matches!(
            Value::String(Arc::new("hello".to_string())).as_map(),
            Err { .. }
        );
    }

    #[test]
    fn test_typecast_bool() {
        matches!(Value::Bool(true).as_bool(), Ok(true));
        matches!(Value::Bool(true).as_int(), Err { .. });
        matches!(Value::Bool(true).as_float(), Err { .. });
        matches!(Value::Bool(true).as_embedding(), Err { .. });
        matches!(Value::Bool(true).as_list(), Err { .. });
        matches!(Value::Bool(true).as_map(), Err { .. });
        matches!(Value::Bool(true).as_str(), Err { .. });
    }

    #[test]
    fn test_typecast_embedding() {
        assert_eq!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0]))
                .as_embedding()
                .unwrap(),
            &[1.0, 2.0, 3.0]
        );
        matches!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).as_int(),
            Err { .. }
        );
        matches!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).as_float(),
            Err { .. }
        );
        matches!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).as_bool(),
            Err { .. }
        );
        matches!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).as_list(),
            Err { .. }
        );
        matches!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).as_map(),
            Err { .. }
        );
        matches!(
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])).as_timestamp(),
            Err { .. }
        );
    }

    #[test]
    fn test_typecast_timestamp() {
        let ts = UTCTimestamp::from(1231);
        assert_eq!(Value::Timestamp(ts).as_timestamp().unwrap(), ts);
        matches!(Value::Timestamp(ts).as_bool(), Err { .. });
        matches!(Value::Timestamp(ts).as_int(), Err { .. });
        matches!(Value::Timestamp(ts).as_float(), Err { .. });
        matches!(Value::Timestamp(ts).as_embedding(), Err { .. });
        matches!(Value::Timestamp(ts).as_list(), Err { .. });
        matches!(Value::Timestamp(ts).as_map(), Err { .. });
    }

    #[test]
    fn test_typecast_list() {
        // TODO: add a test for lists
    }

    #[test]
    fn test_typecast_map() {
        // TODO: add a test for map
    }

    #[test]
    fn test_timestamp_add_sub_duration() {
        use std::time::Duration;
        let ts = UTCTimestamp::from(1695529835378784);
        let dur = Duration::from_secs(1);
        assert_eq!(ts + dur, UTCTimestamp::from(1695529836378784));
        assert_eq!(ts - dur, UTCTimestamp::from(1695529834378784));
    }

    #[test]
    fn test_timestamp_parsing_json() {
        let v1 = JsonValue::String("1677220740".to_string());
        let ts1 = UTCTimestamp::from_json_parsed(&v1);
        assert!(matches!(ts1, Ok(_)));
        let v2 = JsonValue::Number(1677220740.into());
        let ts2 = UTCTimestamp::from_json_parsed(&v2);
        assert!(matches!(ts2, Ok(_)));
        assert_eq!(ts1.unwrap(), ts2.unwrap());
        // postgres timestamp without time-zone - %Y-%m-%dT%H:%M:%S%.f%z
        let v3 = JsonValue::String("2023-09-11T12:52:20.000000".to_string());
        let ts3 = UTCTimestamp::from_json_parsed(&v3).unwrap();
        assert_eq!(ts3, UTCTimestamp::from(1694436740000000));
        // postgres timestamp with TZ
        let v4 = JsonValue::String("2023-09-11T12:52:20.000000+08:30".to_string());
        let ts4 = UTCTimestamp::from_json_parsed(&v4).unwrap();
        assert_eq!(ts4, UTCTimestamp::from(1694406140000000));
        let v5 = JsonValue::String("2023-09-11T12:52:20.000000-08:30".to_string());
        let ts5 = UTCTimestamp::from_json_parsed(&v5).unwrap();
        assert_eq!(ts5, UTCTimestamp::from(1694467340000000));
        let v6 = JsonValue::String("2023-09-11 12:52:20.000000-08:30".to_string());
        let ts6 = UTCTimestamp::from_json_parsed(&v6).unwrap();
        assert_eq!(ts6, UTCTimestamp::from(1694467340000000));
        let v7 = JsonValue::String("2016-09-20 23:15:17.063+00".to_string());
        let ts7 = UTCTimestamp::from_json_parsed(&v7).unwrap();
        assert_eq!(ts7, UTCTimestamp::from(1474413317063000));
        let v8 = JsonValue::String("2025-01-15 04:55:39.940528 UTC".to_string());
        let ts8 = UTCTimestamp::from_json_parsed(&v8).unwrap();
        assert_eq!(ts8, UTCTimestamp::from(1736916939940528));
        let v9 = JsonValue::String("2025-01-15 10:25:39.940528 Asia/Kolkata".to_string());
        let ts9 = UTCTimestamp::from_json_parsed(&v9).unwrap();
        assert_eq!(ts9, UTCTimestamp::from(1736916939940528));
    }

    #[test]
    fn test_float_nan_parsing_json() {
        let cases = vec!["nan", "NaN", "NAN"];
        for input in cases {
            assert_eq!(
                Value::Float(f64::NAN),
                Value::from_json_parsed(&Type::Float, &JsonValue::String(input.to_string()))
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_float_pinf_parsing_json() {
        let cases = vec!["inf", "infinity", "+infinity"];
        for input in cases {
            assert_eq!(
                Value::Float(f64::INFINITY),
                Value::from_json_parsed(&Type::Float, &JsonValue::String(input.to_string()))
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_float_ninf_parsing_json() {
        let cases = vec!["-inf", "-infinity"];
        for input in cases {
            assert_eq!(
                Value::Float(f64::NEG_INFINITY),
                Value::from_json_parsed(&Type::Float, &JsonValue::String(input.to_string()))
                    .unwrap()
            );
        }
    }

    #[test]
    fn test_timestamp_parsing_proto() {
        let v1 = ProtobufValue::String("1677220740".to_string());
        let ts1 = UTCTimestamp::from_proto_parsed(v1);
        assert!(matches!(ts1, Ok(_)));
        let v2 = ProtobufValue::Int32(1677220740);
        let ts2 = UTCTimestamp::from_proto_parsed(v2);
        assert!(matches!(ts2, Ok(_)));
        assert_eq!(ts1.unwrap(), ts2.unwrap());
        // postgres timestamp without time-zone - %Y-%m-%dT%H:%M:%S%.f%z
        let v3 = ProtobufValue::String("2023-09-11T12:52:20.000000".to_string());
        let ts3 = UTCTimestamp::from_proto_parsed(v3).unwrap();
        assert_eq!(ts3, UTCTimestamp::from(1694436740000000));
        // postgres timestamp with TZ
        let v4 = ProtobufValue::String("2023-09-11T12:52:20.000000+08:30".to_string());
        let ts4 = UTCTimestamp::from_proto_parsed(v4).unwrap();
        assert_eq!(ts4, UTCTimestamp::from(1694406140000000));
        let v5 = ProtobufValue::String("2023-09-11T12:52:20.000000-08:30".to_string());
        let ts5 = UTCTimestamp::from_proto_parsed(v5).unwrap();
        assert_eq!(ts5, UTCTimestamp::from(1694467340000000));
        let v6 = ProtobufValue::String("2023-09-11 12:52:20.000000-08:30".to_string());
        let ts6 = UTCTimestamp::from_proto_parsed(v6).unwrap();
        assert_eq!(ts6, UTCTimestamp::from(1694467340000000));
        let v7 = ProtobufValue::String("2016-09-20 23:15:17.063+00".to_string());
        let ts7 = UTCTimestamp::from_proto_parsed(v7).unwrap();
        assert_eq!(ts7, UTCTimestamp::from(1474413317063000));
    }

    #[test]
    fn test_value_struct_ordering() {
        // Repeated fields are not allowed
        assert!(Struct::new(
            [
                ("field1".into(), Value::Int(1)),
                ("field2".into(), Value::String(Arc::new("foo".into()))),
                ("field3".into(), Value::Bool(true)),
                ("field3".into(), Value::Int(2)),
            ]
            .to_vec(),
        )
        .is_err());

        let struct_value_1 = Value::Struct(Arc::new(
            Struct::new(
                [
                    ("field1".into(), Value::Int(1)),
                    ("field2".into(), Value::String(Arc::new("foo".into()))),
                    ("field3".into(), Value::Bool(true)),
                    ("field4".into(), Value::Int(2)),
                ]
                .to_vec(),
            )
            .unwrap(),
        ));

        let struct_value_2 = Value::Struct(Arc::new(
            Struct::new(
                [
                    ("field4".into(), Value::Int(2)),
                    ("field2".into(), Value::String(Arc::new("foo".into()))),
                    ("field1".into(), Value::Int(1)),
                    ("field3".into(), Value::Bool(true)),
                ]
                .to_vec(),
            )
            .unwrap(),
        ));

        assert_eq!(struct_value_1, struct_value_2);

        let hash_1 = struct_value_1.hash().unwrap();
        let hash_2 = struct_value_2.hash().unwrap();
        assert_eq!(hash_1, hash_2);
    }

    #[test]
    fn test_to_from_ts_proto() {
        let ts = Value::Timestamp(UTCTimestamp::from(1695529835378784));
        let proto: ProtoValue = (&ts).into();
        let ts2: Value = proto.try_into().unwrap();
        assert_eq!(ts, ts2);
    }

    #[test]
    fn test_to_from_proto() {
        let values = vec![
            Value::Int(1),
            Value::Float(1.0),
            Value::String(Arc::new("hello".to_string())),
            Value::Bool(true),
            Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])),
            Value::Timestamp(UTCTimestamp::from(1231)),
            Value::List(Arc::new(
                List::new(Type::Int, &[Value::Int(1), Value::Int(4)]).unwrap(),
            )),
            Value::Map(Arc::new(
                Map::new(
                    Type::Int,
                    &[
                        ("hi".to_string(), Value::Int(4)),
                        ("byte".to_string(), Value::Int(5)),
                    ],
                )
                .unwrap(),
            )),
        ];
        for v in values.iter() {
            let proto: ProtoValue = v.into();
            let v2 = proto.try_into().unwrap();
            assert_eq!(v, &v2);
        }
        use crate::schema_proto::schema;
        use crate::schema_proto::schema::DataType as ProtoType;
        // todo: add more invalid cases
        let invalids: Vec<ProtoValue> = vec![
            ProtoValue {
                variant: Some(Variant::List(Box::new(schema::List {
                    // dtype is not set
                    dtype: None,
                    values: vec![],
                }))),
            },
            // for list dtype is set, but elements don't match it
            ProtoValue {
                variant: Some(Variant::List(Box::new(schema::List {
                    dtype: Some(Box::new(ProtoType {
                        dtype: Some(Dtype::IntType(IntType {})),
                    })),
                    values: vec![ProtoValue {
                        variant: Some(Variant::Float(1.0)),
                    }],
                }))),
            },
        ];
        for proto in invalids {
            let v: Result<Value> = proto.try_into();
            assert!(v.is_err());
        }
    }

    #[test]
    fn test_equals() {
        assert_eq!(Value::None, Value::None);
        assert_ne!(Value::None, Value::Float(f64::NAN));
        assert_eq!(Value::Float(f64::NAN), Value::Float(f64::NAN));
        assert_ne!(Value::Float(f64::NAN), Value::Float(0.));
        assert_eq!(Value::Int(2), Value::Int(2));
        assert_ne!(Value::Int(2), Value::Float(2.0));
    }

    #[test]
    fn test_null_from_json() {
        let val = Value::from_json(&Type::Optional(Box::new(Type::String)), "null").unwrap();
        assert_eq!(val, Value::None);
        assert_eq!(val.to_json(), JsonValue::Null);
    }

    #[test]
    fn test_value_from_proto_parsed() {
        // Basic field - Int
        {
            let schema_field = "int_field";
            let dtype = Type::Int;
            let context = ProtoContext::new();
            let value = ProtobufValue::SInt32(123);
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::SInt32);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(val, Value::Int(123));
        }
        // Basic fields - String
        {
            let schema_field = "string_field";
            let dtype = Type::String;
            let context = ProtoContext::new();
            let value = ProtobufValue::String("temp_string".to_string());
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::String);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(val, Value::String(Arc::new("temp_string".to_string())));
        }
        // Basic fields - bool
        {
            let schema_field = "bool_field";
            let dtype = Type::Bool;
            let context = ProtoContext::new();
            let value = ProtobufValue::Bool(true);
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::Bool);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(val, Value::Bool(true));
        }
        // Basic fields - Timestamp as string
        {
            let schema_field = "timestamp_field";
            let dtype = Type::Timestamp;
            let context = ProtoContext::new();
            let value = ProtobufValue::String("2020-01-01T00:02:00Z".to_string());
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::String);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(
                val,
                Value::Timestamp(UTCTimestamp::from_micros(1577836920000000))
            );
        }
        // Basic fields - Timestamp as Int
        {
            let schema_field = "timestamp_field";
            let dtype = Type::Timestamp;
            let context = ProtoContext::new();
            let value = ProtobufValue::Int64(1577836920000000);
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::Int32);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(
                val,
                Value::Timestamp(UTCTimestamp::from_micros(1577836920000000))
            );
        }
        // Basic fields - Decimal
        {
            let schema_field = "decimal_field";
            let dtype = Type::Decimal(DecimalType::new(2).unwrap());
            let context = ProtoContext::new();
            let value = ProtobufValue::Float(1.234);
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::Float);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(val, Value::Decimal(Arc::new(Decimal::new(123, 2))));
        }
        // Complex field - List of floats
        {
            let schema_field = "list_field";
            let dtype = Type::List(Box::new(Type::Float));
            let context = ProtoContext::new();
            let value = ProtobufValue::Packed(PackedArray::Float(vec![1.1, 2.2]));
            // Setting it as bytes for now since we cannot create MessageRef outside
            let msg_field =
                MessageField::new("random_field".to_string(), 1, ProtobufValueType::Bytes);
            let val = Value::from_proto_parsed(
                schema_field,
                &dtype,
                &context,
                Some(vec![value]),
                &msg_field,
            )
            .unwrap();
            assert_eq!(
                val,
                Value::List(Arc::new(
                    List::new(Type::Float, &[Value::Float(1.1), Value::Float(2.2)]).unwrap()
                ))
            );
        }
    }

    #[test]
    fn test_gen_random_value() {
        let types = [
            Type::Int,
            Type::Float,
            Type::String,
            Type::Bool,
            Type::Timestamp,
            Type::List(Box::new(Type::Int)),
            Type::Map(Box::new(Type::Int)),
            Type::Between(
                Between::new(Type::Int, Value::Int(0), Value::Int(100), false, false).unwrap(),
            ),
            Type::Embedding(3),
            Type::Optional(Box::new(Type::Int)),
            Type::Optional(Box::new(Type::String)),
            Type::OneOf(
                OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(2), Value::Int(3)]).unwrap(),
            ),
            Type::Struct(Box::new(
                StructType::new(
                    "mystruct".into(),
                    vec![
                        Field::new("field1", Type::Int),
                        Field::new("field2", Type::String),
                    ],
                )
                .unwrap(),
            )),
        ];
        for t in types {
            let v = Value::random(&t);
            assert!(v.matches(&t));
        }
    }

    #[test]
    fn test_from_protobuf_val_to_parent_type() {
        // Basic field - Int
        {
            let value = ProtobufValue::SInt32(123);
            let value = Value::from_protobuf_val_to_parent_type(value);
            assert_eq!(value, ProtobufValue::Int64(123));
        }
        // Basic field - Int
        {
            let value = ProtobufValue::Fixed32(123);
            let value = Value::from_protobuf_val_to_parent_type(value);
            assert_eq!(value, ProtobufValue::Int64(123));
        }
        // Basic field - float
        {
            let value = ProtobufValue::Float(1.23);
            let value = Value::from_protobuf_val_to_parent_type(value);
            if let ProtobufValue::Double(v) = value {
                assert!((v - 1.23).abs() < 0.001);
            } else {
                panic!("Expected double value to be 1.23 but found {:?}", value)
            }
        }
        // Complex field - List of Int
        {
            let value = ProtobufValue::Packed(PackedArray::UInt32(vec![1, 2, 3]));
            let value = Value::from_protobuf_val_to_parent_type(value);
            assert_eq!(
                value,
                ProtobufValue::Packed(PackedArray::Int64(vec![1, 2, 3]))
            );
        }
        // Non transformed field - String
        {
            let value = ProtobufValue::String("temp".to_string());
            let value = Value::from_protobuf_val_to_parent_type(value);
            assert_eq!(value, ProtobufValue::String("temp".to_string()));
        }
    }

    #[test]
    fn test_row_encode_decode_using_permissive_schema() {
        let complex_struct_type = Type::Struct(Box::new(
            StructType::new(
                "st".into(),
                vec![
                    Field::new("int_field", Type::Int),
                    Field::new("float_field", Type::List(Box::new(Type::Float))),
                ],
            )
            .unwrap(),
        ));
        let inner_struct_type = Type::Struct(Box::new(
            StructType::new(
                "inner_list_type".into(),
                vec![Field::new("values", Type::List(Box::new(Type::Int)))],
            )
            .unwrap(),
        ));
        let complex_list_type = Type::List(Box::new(inner_struct_type.clone()));

        let schema = Schema::new(vec![
            Field::new("int_field", Type::Int),
            Field::new("string_field", Type::String),
            Field::new("float_field", Type::Float),
            Field::new("bool_field", Type::Bool),
            Field::new("date_field", Type::Date),
            Field::new("timestamp_field", Type::Timestamp),
            Field::new("decimal_field", Type::Decimal(DecimalType::new(2).unwrap())),
            Field::new(
                "struct_field",
                Type::Struct(Box::new(
                    StructType::new(
                        "struct_field".into(),
                        vec![
                            Field::new("sub_int_field", Type::Int),
                            Field::new("sub_string_field", Type::String),
                        ],
                    )
                    .unwrap(),
                )),
            ),
            Field::new("list_field", Type::List(Box::new(Type::Int))),
            Field::new("map_field", Type::Map(Box::new(Type::String))),
            Field::new("embedding_field", Type::Embedding(3)),
            Field::new(
                "oneof_field",
                Type::OneOf(
                    OneOf::new(Type::Int, vec![Value::Int(1), Value::Int(3), Value::Int(5)])
                        .unwrap(),
                ),
            ),
            Field::new("optional_field", Type::Optional(Box::new(Type::Int))),
            Field::new("complex_struct_field", complex_struct_type),
            Field::new("complex_list_field", complex_list_type),
            Field::new(
                "complex_optional_type",
                Type::Optional(Box::new(Type::List(Box::new(Type::Optional(Box::new(
                    Type::Int,
                )))))),
            ),
        ])
        .unwrap();

        // Valid values using permissive schema
        let complex_struct_value = Value::Struct(Arc::new(
            Struct::new(vec![
                ("int_field".into(), Value::Int(2)),
                (
                    "float_field".into(),
                    Value::List(Arc::new(
                        List::new(
                            Type::Optional(Box::new(Type::Optional(Box::new(Type::Float)))),
                            &[Value::Float(1.1), Value::Float(2.2)],
                        )
                        .unwrap(),
                    )),
                ),
            ])
            .unwrap(),
        ));
        let complex_list_value = Value::List(Arc::new(
            List::new(
                inner_struct_type,
                &[
                    Value::Struct(Arc::new(
                        Struct::new(vec![(
                            "values".into(),
                            Value::List(Arc::new(
                                List::new(
                                    Type::Optional(Box::new(Type::Int)),
                                    &[Value::Int(2), Value::Int(3)],
                                )
                                .unwrap(),
                            )),
                        )])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        Struct::new(vec![(
                            "values".into(),
                            Value::List(Arc::new(
                                List::new(
                                    Type::Optional(Box::new(Type::Int)),
                                    &[Value::Int(4), Value::Int(5)],
                                )
                                .unwrap(),
                            )),
                        )])
                        .unwrap(),
                    )),
                ],
            )
            .unwrap(),
        ));
        let values = vec![
            Value::Int(10),
            Value::String(Arc::new("temp".to_string())),
            Value::Float(1.1),
            Value::Bool(true),
            Value::Date(Date::from(12345)),
            Value::Timestamp(UTCTimestamp::from(123456000000)),
            Value::Decimal(Arc::new(Decimal::new(12345, 2))),
            Value::Struct(Arc::new(
                Struct::new(vec![
                    ("sub_int_field".into(), Value::Int(2)),
                    (
                        "sub_string_field".into(),
                        Value::String(Arc::new("sub_string".to_string())),
                    ),
                ])
                .unwrap(),
            )),
            Value::List(Arc::new(
                List::new(
                    Type::Optional(Box::new(Type::Int)),
                    &[Value::Int(2), Value::Int(3)],
                )
                .unwrap(),
            )),
            Value::Map(Arc::new(
                Map::new(
                    Type::Optional(Box::new(Type::String)),
                    &[
                        ("a".to_string(), Value::String(Arc::new("b".to_string()))),
                        ("c".to_string(), Value::String(Arc::new("d".to_string()))),
                    ],
                )
                .unwrap(),
            )),
            Value::Embedding(Arc::new(vec![1.2, 2.3, 3.4])),
            Value::Int(3),
            Value::None,
            complex_struct_value,
            complex_list_value,
            Value::List(Arc::new(
                List::new(
                    Type::Optional(Box::new(Type::Optional(Box::new(Type::Optional(
                        Box::new(Type::Int),
                    ))))),
                    &[Value::None, Value::Int(2)],
                )
                .unwrap(),
            )),
        ];
        let row = Row::new(Arc::new(schema.clone()), Arc::new(values)).unwrap();

        // Row encode should succeed
        let mut encoded = Vec::new();
        row.encode(&mut encoded).unwrap();

        // Row decode should succeed
        let _ = Row::decode(Arc::new(schema), &mut encoded.as_slice()).unwrap();
    }
}
