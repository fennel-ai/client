use std::{sync::Arc, time::Duration};

use anyhow::{anyhow, Result};
use itertools::Itertools;

pub trait Dtyped {
    fn dtyped() -> Type;
}

use crate::{
    value::{UTCTimestamp, Value},
    List, Type,
};

impl<T> TryInto<Value> for Vec<T>
where
    T: Into<Value>,
    T: Dtyped,
{
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Value> {
        let list = List::new(
            T::dtyped(),
            &self.into_iter().map(|v| v.into()).collect_vec(),
        )?;
        Ok(Value::List(Arc::new(list)))
    }
}

impl<T> TryInto<Value> for Option<Vec<T>>
where
    T: Into<Value>,
    T: Dtyped,
{
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Value> {
        match self {
            Some(v) => v.try_into(),
            None => Ok(Value::None),
        }
    }
}

impl Dtyped for i64 {
    fn dtyped() -> Type {
        Type::Int
    }
}
impl Dtyped for f64 {
    fn dtyped() -> Type {
        Type::Float
    }
}
impl Dtyped for String {
    fn dtyped() -> Type {
        Type::String
    }
}
impl Dtyped for &str {
    fn dtyped() -> Type {
        Type::String
    }
}

impl Dtyped for bool {
    fn dtyped() -> Type {
        Type::Bool
    }
}

impl Dtyped for UTCTimestamp {
    fn dtyped() -> Type {
        Type::Timestamp
    }
}

impl<T: Dtyped> Dtyped for Option<T> {
    fn dtyped() -> Type {
        Type::optional(T::dtyped())
    }
}

// A primitive value is a value that can be converted to a Type and Value.
// If type T is Primitive, so is Option<T>.
impl<T> Into<Value> for Option<T>
where
    T: Into<Value>,
{
    // Option<Option<T>> is the same type as Option<T>
    fn into(self) -> Value {
        match self {
            Some(v) => v.into(),
            None => Value::None,
        }
    }
}

// Implement Primitive for all types that can be converted to i64
impl Into<Value> for i64 {
    fn into(self) -> Value {
        Value::Int(self)
    }
}

// Implement Primitive for all types that can be converted to f64
impl Into<Value> for f64 {
    fn into(self) -> Value {
        Value::Float(self)
    }
}

// Implement Primitive for all types that can be converted to &str
impl Into<Value> for String {
    fn into(self) -> Value {
        Value::String(Arc::new(self))
    }
}

impl Into<Value> for &str {
    fn into(self) -> Value {
        Value::String(Arc::new(self.to_string()))
    }
}

impl Into<Value> for bool {
    fn into(self) -> Value {
        Value::Bool(self)
    }
}

/// Can convert duration to timestamp where duration is
/// expected to be the number of microseconds since epoch
impl From<Duration> for Value {
    fn from(d: Duration) -> Self {
        Value::Timestamp(UTCTimestamp::from(d.as_micros() as i64))
    }
}

// And it's possible to convert a Value to timestamp
// Useful for testing
impl TryInto<UTCTimestamp> for Value {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<UTCTimestamp> {
        match self {
            Value::Timestamp(t) => Ok(t),
            Value::Int(n) => Ok(UTCTimestamp::from(n)),
            _ => Err(anyhow!("not a timestamp")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ints_to_value_type() {
        let n = 10i64;
        let v: Value = n.into();
        assert_eq!(v, Value::Int(10));

        let n = Some(10i64);
        let v: Value = n.into();
        assert_eq!(v, Value::Int(10));

        let n: Option<i64> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let n: Option<Option<i64>> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let s = Some(Some(10i64));
        let v: Value = s.into();
        assert_eq!(v, Value::Int(10));
    }

    #[test]
    fn test_bools_to_value_type() {
        let n = true;
        let v: Value = n.into();
        assert_eq!(v, Value::Bool(true));

        let n = Some(true);
        let v: Value = n.into();
        assert_eq!(v, Value::Bool(true));

        let n: Option<bool> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let n: Option<Option<bool>> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let s = Some(Some(true));
        let v: Value = s.into();
        assert_eq!(v, Value::Bool(true));
    }

    #[test]
    fn test_floats_to_value_type() {
        let n = 10.0f64;
        let v: Value = n.into();
        assert_eq!(v, Value::Float(10.0));

        let n = Some(10.0f64);
        let v: Value = n.into();
        assert_eq!(v, Value::Float(10.0));

        let n: Option<f64> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let n: Option<Option<f64>> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let s = Some(Some(10.0f64));
        let v: Value = s.into();
        assert_eq!(v, Value::Float(10.0));
    }

    #[test]
    fn test_strs_to_value_type() {
        let n = "hello";
        let v: Value = n.into();
        assert_eq!(v, Value::String(Arc::new("hello".to_string())));

        let n = Some("hello");
        let v: Value = n.into();
        assert_eq!(v, Value::String(Arc::new("hello".to_string())));

        let n: Option<&str> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let n: Option<Option<&str>> = None;
        let v: Value = n.into();
        assert_eq!(v, Value::None);

        let s = Some(Some("hello"));
        let v: Value = s.into();
        assert_eq!(v, Value::String(Arc::new("hello".to_string())));
    }

    #[test]
    fn test_value_to_timestamp() {
        let cases = [
            (
                Value::Timestamp(UTCTimestamp::from(10)),
                UTCTimestamp::from(10),
                false,
            ),
            (Value::Int(10), UTCTimestamp::from(10), false),
            (Value::Float(10.0), UTCTimestamp::from(1), true),
            (
                Value::String(Arc::new("hi".to_string())),
                UTCTimestamp::from(1),
                true,
            ),
            (Value::Bool(false), UTCTimestamp::from(1), true),
        ];
        for (v, expected, err) in cases {
            let found: Result<UTCTimestamp> = v.try_into();
            if err {
                assert!(found.is_err());
            } else {
                assert_eq!(found.unwrap(), expected);
            }
        }
    }

    #[test]
    fn test_value_from_duration() {
        let d = Duration::from_micros(10);
        let v: Value = d.into();
        assert_eq!(v, Value::Timestamp(UTCTimestamp::from(10)));
    }
}
