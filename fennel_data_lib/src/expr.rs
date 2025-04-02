use polars::prelude::{
    DataType as PolarsType, Expr as PolarsExpr, Field as PolarsField, ListNameSpaceExtension,
    LiteralValue as PolarsLV, NamedFrom as _, Operator as PolarsOperator, SpecialEq,
    StringFunction, TimeUnit as PolarsTimeUnit, NULL,
};
use polars::{lazy::dsl as pl, series::Series};

use crate::{
    schema::{Field, Schema},
    types::{DecimalType, StructType, Type},
    value::Value,
    UTCTimestamp,
};
use anyhow::{anyhow, bail, Context, Result};
use builders::lit;
use itertools::Itertools;
use polars_plan::prelude::FunctionOptions;
use serde::Serialize;
use std::collections::HashMap;
use std::{fmt, sync::Arc};

#[derive(Debug, Clone)]
pub struct EvalContext {
    now_col_name: Option<String>,
    index_col_name: Option<String>,
}

impl EvalContext {
    pub fn new(now_col_name: Option<String>) -> Self {
        Self {
            now_col_name,
            index_col_name: None,
        }
    }

    pub fn now_col_name(&self) -> Option<&String> {
        self.now_col_name.as_ref()
    }
    pub fn index_col_name(&self) -> Option<&String> {
        self.index_col_name.as_ref()
    }
    pub fn with_index_col_name(mut self, index_col_name: String) -> Self {
        self.index_col_name = Some(index_col_name);
        self
    }
}

impl TryFrom<&Type> for PolarsType {
    type Error = anyhow::Error;

    fn try_from(dtype: &Type) -> Result<Self> {
        use PolarsType as DataType;
        match dtype {
            Type::Null => Ok(DataType::Null),
            Type::Bool => Ok(DataType::Boolean),
            Type::Int => Ok(DataType::Int64),
            Type::Float => Ok(DataType::Float64),
            Type::String => Ok(DataType::String),
            Type::Bytes => Ok(DataType::Binary),
            Type::Date => Ok(DataType::Date),
            Type::List(t) => Ok(DataType::List(Box::new(t.as_ref().try_into()?))),
            Type::Struct(s) => {
                let fields = s
                    .fields()
                    .iter()
                    .map(|f| match f.dtype().try_into() {
                        Ok(dt) => Ok(PolarsField::new(f.name(), dt)),
                        Err(e) => Err(e),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(fields))
            }
            Type::Optional(t) => t.as_ref().try_into(),
            Type::Between(b) => b.dtype().try_into(),
            Type::OneOf(o) => o.dtype().try_into(),
            Type::Regex(_) => Ok(DataType::String),
            Type::Embedding(_) => Ok(DataType::Float32),
            Type::Timestamp => Ok(DataType::Datetime(
                PolarsTimeUnit::Microseconds,
                Some("UTC".to_string()),
            )),
            Type::Map(_) => bail!("map/dict types are not supported"),
            Type::Decimal(d) => Ok(DataType::Decimal(Some(d.scale() as usize), None)),
        }
    }
}

impl From<&BinOp> for PolarsOperator {
    fn from(op: &BinOp) -> Self {
        use PolarsOperator as Operator;
        match op {
            BinOp::Eq => Operator::Eq,
            BinOp::Neq => Operator::NotEq,
            BinOp::Gt => Operator::Gt,
            BinOp::Gte => Operator::GtEq,
            BinOp::Lt => Operator::Lt,
            BinOp::Lte => Operator::LtEq,
            BinOp::And => Operator::And,
            BinOp::Or => Operator::Or,
            BinOp::Add => Operator::Plus,
            BinOp::Sub => Operator::Minus,
            BinOp::Mul => Operator::Multiply,
            BinOp::Div => Operator::Divide,
            BinOp::Mod => Operator::Modulus,
            BinOp::FloorDiv => Operator::FloorDivide,
        }
    }
}

impl TryFrom<Value> for PolarsExpr {
    type Error = anyhow::Error;
    fn try_from(value: Value) -> Result<Self> {
        use PolarsLV as LiteralValue;
        match &value {
            Value::None => Ok(pl::lit(LiteralValue::Null)),
            Value::Bool(b) => Ok(pl::lit(LiteralValue::Boolean(*b))),
            Value::Int(i) => Ok(pl::lit(LiteralValue::Int64(*i))),
            Value::Float(f) => Ok(pl::lit(LiteralValue::Float64(*f))),
            Value::String(s) => Ok(pl::lit(LiteralValue::String(s.to_string()))),
            Value::Bytes(b) => Ok(pl::lit(LiteralValue::Binary(b.to_vec()))),
            Value::Timestamp(t) => Ok(pl::lit(LiteralValue::DateTime(
                t.micros(),
                polars::prelude::TimeUnit::Microseconds,
                Some("UTC".to_string()),
            ))),
            Value::Date(d) => Ok(pl::lit(LiteralValue::Date(d.days() as i32))),
            Value::List(l) => {
                let as_str = value.to_json().to_string();
                let dtype = Type::List(Box::new(l.dtype().clone()));
                let pltype = (&dtype).try_into()?;
                let expr = pl::lit(as_str).str().json_decode(Some(pltype), None);
                Ok(expr)
            }
            Value::Decimal(d) => Ok(pl::lit(LiteralValue::Decimal(
                d.mantissa(),
                d.scale() as usize,
            ))),
            Value::Embedding(e) => {
                let name = format!("embedding_{}", value.hash()?);
                let series = Series::new(&name, e.as_ref());
                Ok(pl::lit(LiteralValue::Series(SpecialEq::new(series))))
            }
            Value::Struct(s) => {
                let as_str = value.to_json().to_string();
                let mut fields = vec![];
                for (k, v) in s.fields() {
                    let field = Field::new(k.to_string(), natural_type(v));
                    fields.push(field);
                }
                let dtype = Type::Struct(Box::new(
                    StructType::new("__anon__".into(), fields).unwrap(),
                ));
                let pltype = (&dtype).try_into()?;
                let expr = pl::lit(as_str).str().json_decode(Some(pltype), None);
                Ok(expr)
            }
            Value::Map(_) => Err(anyhow!("map/dict literals are not supported")),
        }
    }
}

use crate::schema_proto::expression as eproto;
use crate::schema_proto::expression::EvalContext as ProtoEvalContext;
use crate::schema_proto::expression::Expr as ProtoExpr;
use crate::schema_proto::expression::TimeUnit as ProtoTimeUnit;
use crate::schema_proto::expression::Timezone as ProtoTimezone;

#[derive(Clone, PartialEq, Serialize)]
pub enum Expr {
    Var {
        name: String,
    },
    Ref {
        name: String,
    },
    Lit {
        value: Value,
    },
    // NOTE: Cast should not be exposed to the user, it is only used internally
    // to safely cast types.
    Cast {
        expr: Box<Expr>,
        dtype: Type,
    },
    Unary {
        op: UnOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Case {
        when_thens: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>, // if none, treated as None literal
    },
    // Given expression, checks if it is null. Expr can be of any type
    // always returns a boolean (not optional)
    IsNull {
        expr: Box<Expr>,
    },
    // Given expression, returns the default if it is null, otherwise the
    // expression itself. Expr can be of any type. Expr & default should be
    // promotable to the same type
    FillNull {
        expr: Box<Expr>,
        default: Box<Expr>,
    },
    ListFn {
        list: Box<Expr>,
        func: Box<ListFn>,
    },
    MathFn {
        func: MathFn,
        expr: Box<Expr>,
    },
    StructFn {
        struct_: Box<Expr>,
        func: Box<StructFn>,
    },
    DictFn {
        dict: Box<Expr>,
        func: Box<DictFn>,
    },
    StringFn {
        func: Box<StringFn>,
        expr: Box<Expr>,
    },
    DatetimeFn {
        func: Box<DateTimeFn>,
        expr: Box<Expr>,
    },
    FromEpoch {
        expr: Box<Expr>,
        unit: TimeUnit,
    },
    MakeStruct {
        struct_type: StructType,
        fields: Vec<(String, Box<Expr>)>,
    },
    DateTimeLiteral {
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        microsecond: u32,
        timezone: Option<String>,
    },
    Now,
    Zip {
        struct_type: StructType,
        fields: Vec<(String, Box<Expr>)>,
    },
    Repeat {
        expr: Box<Expr>,
        count: Box<Expr>,
    },
}
impl AsRef<Expr> for Expr {
    fn as_ref(&self) -> &Expr {
        self
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub enum TimeUnit {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl std::fmt::Debug for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimeUnit::Microsecond => write!(f, "microsecond"),
            TimeUnit::Millisecond => write!(f, "millisecond"),
            TimeUnit::Second => write!(f, "second"),
            TimeUnit::Minute => write!(f, "minute"),
            TimeUnit::Hour => write!(f, "hour"),
            TimeUnit::Day => write!(f, "day"),
            TimeUnit::Week => write!(f, "week"),
            TimeUnit::Month => write!(f, "month"),
            TimeUnit::Year => write!(f, "year"),
        }
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub enum DateTimeFn {
    SinceEpoch {
        unit: TimeUnit,
    },
    Since {
        other: Box<Expr>,
        unit: TimeUnit,
    },
    Part {
        unit: TimeUnit,
        timezone: Option<String>,
    },
    Stftime {
        format: String,
        timezone: Option<String>,
    },
}

impl fmt::Debug for DateTimeFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DateTimeFn::SinceEpoch { unit } => write!(f, "since_epoch({:?})", unit),
            DateTimeFn::Since { other, unit } => {
                write!(f, "since({:?}, unit=\"{:?}\")", other, unit)
            }
            DateTimeFn::Part { unit, timezone } => match (unit, timezone) {
                (TimeUnit::Year, None) => write!(f, "year"),
                (TimeUnit::Month, None) => write!(f, "month"),
                (TimeUnit::Day, None) => write!(f, "day"),
                (TimeUnit::Hour, None) => write!(f, "hour"),
                (TimeUnit::Minute, None) => write!(f, "minute"),
                (TimeUnit::Second, None) => write!(f, "second"),
                (TimeUnit::Year, Some(tz)) => write!(f, "year(timezone=\"{:?}\")", tz),
                (TimeUnit::Month, Some(tz)) => write!(f, "month(timezone=\"{:?}\")", tz),
                (TimeUnit::Day, Some(tz)) => write!(f, "day(timezone=\"{:?}\")", tz),
                (TimeUnit::Hour, Some(tz)) => write!(f, "hour(timezone=\"{:?}\")", tz),
                (TimeUnit::Minute, Some(tz)) => write!(f, "minute(timezone=\"{:?}\")", tz),
                (TimeUnit::Second, Some(tz)) => write!(f, "second(timezone=\"{:?}\")", tz),
                (TimeUnit::Millisecond, _) | (TimeUnit::Microsecond, _) | (TimeUnit::Week, _) => {
                    panic!("invalid time unit for part function")
                }
            },
            DateTimeFn::Stftime { format, timezone } => {
                let tz = timezone.as_ref().map(|tz| tz.as_str()).unwrap_or("UTC");
                write!(f, "strftime({:?}, timezone=\"{}\")", format, tz)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StringFn {
    Len,
    ToLower,
    ToUpper,
    Contains {
        key: Expr,
    },
    StartsWith {
        key: Expr,
    },
    EndsWith {
        key: Expr,
    },
    Concat {
        other: Expr,
    },
    Strptime {
        format: String,
        timezone: Option<String>,
    },
    JsonDecode {
        dtype: Type,
    },
    Split {
        sep: String,
    },
    JsonExtract {
        path: String,
    },
    ToInt,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum DictFn {
    Len,
    Get { key: Expr, default: Option<Expr> },
    Contains { key: Expr },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum ListFn {
    Len,
    HasNull,
    Get { index: Expr },
    Contains { item: Expr },
    Sum,
    Min,
    Max,
    All,
    Any,
    Mean,
    Filter { var: String, predicate: Expr },
    Map { var: String, func: Expr },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum StructFn {
    Get { field: String },
}

/// A compiled expression is an expression that has been validated
/// and type-checked against a schema and has a known output type.
#[derive(Clone, PartialEq, Serialize)]
pub struct CompiledExpr {
    rewritten: Expr,
    original: Expr,
    schema: Arc<Schema>,
    dtype: Type,
}

impl CompiledExpr {
    pub fn org_expr(&self) -> &Expr {
        &self.original
    }
    /// Checks if the given type CAN be made to match the expression.
    ///
    /// For instance, if the expression naturally evaluates to an int, it will
    /// match int, float, optional[int], or optional[float] but won't match
    /// string, list[int], or optional[string] etc.
    ///
    /// The only exception is synthetic types like between, oneof, regex etc.
    pub fn matches(&self, dtype: &Type) -> bool {
        match (&self.dtype, dtype) {
            (t1, t2) if promotable(t1, t2) => true,
            (t, Type::Between(b)) => t == b.dtype(),
            (t, Type::OneOf(o)) => t == o.dtype(),
            (Type::String, Type::Regex(_)) => true,
            _ => false,
        }
    }

    /// Returns the output type of the expression
    pub fn dtype(&self) -> &Type {
        &self.dtype
    }

    /// Converts compiled expression to polars expression
    pub fn into_polar_expr(&self, ctx: Option<EvalContext>) -> Result<PolarsExpr> {
        let exp = &self.rewritten;
        Ok(exp.into_polars_expr(ctx)?)
    }

    /// Given a type, add casts, if necessary, to the expression
    /// to safely cast it to the given type
    fn safecast(&self, dtype: &Type) -> Result<Expr> {
        if self.dtype == *dtype {
            return Ok(self.rewritten.clone());
        }
        // there are only 3 valid cases of casting:
        // 1. going from t -> senior(t)
        // 2. going from optional[t] -> senior(t) -- we accept this to be a safe
        // promotion since arrow doesn't distinguish between null and non-null
        // we can combine these two cases by checking senior of inner type
        // 3. going from list[t] -> list[senior(t)]
        let inner = self.dtype.inner();
        if promotable(inner, dtype) {
            return Ok(Expr::Cast {
                expr: Box::new(self.rewritten.clone()),
                dtype: dtype.clone(),
            });
        }
        match (&self.dtype, dtype) {
            (Type::List(l), Type::List(r)) if promotable(l.inner(), r) => Ok(Expr::Cast {
                expr: Box::new(self.rewritten.clone()),
                dtype: dtype.clone(),
            }),
            _ => bail!(
                "can not safely cast expression {:?} of type {:?} to {:?}",
                self.rewritten,
                self.dtype,
                dtype,
            ),
        }
    }
}

/// Returns true if the given type can be promoted to the target type
fn promotable(from: &Type, to: &Type) -> bool {
    use Type::*;
    match (from, to) {
        (t1, t2) if t1 == t2 => true,
        (Null, Optional(_)) => true,
        (Optional(t1), Optional(t2)) => promotable(t1, t2),
        (t1, Optional(t2)) => promotable(t1, t2),
        (Int, Float) => true,
        (Struct(s1), Struct(s2)) => {
            if s1.fields().len() != s2.fields().len() {
                return false;
            }
            // Sort the fields by name to ensure that the order of fields
            // doesn't matter
            let s1_fields: Vec<&Field> = s1.fields().iter().sorted_by_key(|f| f.name()).collect();
            let s2_fields: Vec<&Field> = s2.fields().iter().sorted_by_key(|f| f.name()).collect();
            s1_fields
                .iter()
                .zip(s2_fields)
                .all(|(f1, f2)| promotable(f1.dtype(), f2.dtype()) && f1.name() == f2.name())
        }
        _ => false,
    }
}

/// Given a type, returns the list of senior types that it can be promoted to
/// The senior are given in the order of seniority, from most junior to most senior
fn seniors(of: &Type) -> Option<Vec<Type>> {
    use Type::*;
    match of {
        Null => None,
        Int => Some(vec![Int, Type::optional(Int), Float, Type::optional(Float)]),
        Optional(t) => match t.as_ref() {
            Null => None,
            Int => Some(vec![Type::optional(Int), Type::optional(Float)]),
            _ => Some(vec![of.clone()]),
        },
        Between(b) => seniors(b.dtype()),
        OneOf(o) => seniors(o.dtype()),
        Regex(_) => seniors(&Type::String),
        Struct(s) => Some(vec![Type::Struct(Box::new(
            StructType::new("anon".into(), s.fields().to_vec()).unwrap(),
        ))]),
        non_optional => Some(vec![
            non_optional.clone(),
            Type::optional(non_optional.clone()),
        ]),
    }
}

/// Given two types, return the least common ancestor if one exists
fn lca(t1: &Type, t2: &Type) -> Option<Type> {
    use Type::*;
    match (t1, t2) {
        (Null, Null) => Some(Null),
        (Null, t) | (t, Null) => Some(Type::optional(t.clone())),
        (t1, t2) => {
            let seniors1 = seniors(t1)?;
            let seniors2 = seniors(t2)?;
            let mut common = seniors1
                .iter()
                .filter(|t1| seniors2.iter().any(|t2| t1 == &t2))
                .collect_vec();
            common.sort_by(|t1, t2| {
                let i1 = seniors1.iter().position(|t| &t == t1).unwrap();
                let i2 = seniors1.iter().position(|t| &t == t2).unwrap();
                i1.cmp(&i2)
            });
            match common.first() {
                Some(t) => Some((*t).clone()),
                None => None,
            }
        }
    }
}

impl Expr {
    /// Return if this expression will always evaluate to Null
    /// (possibly with some output type attached to it)
    fn is_null(&self) -> bool {
        matches!(self, Expr::Lit { value: Value::None })
    }

    fn into_polars_expr(&self, ctx: Option<EvalContext>) -> Result<PolarsExpr> {
        match self {
            Expr::Var { .. } => Ok(pl::col("")),
            Expr::Now => {
                if let Some(ctx) = ctx {
                    if let Some(col_name) = ctx.now_col_name() {
                        Ok(pl::col(col_name))
                    } else {
                        Ok(Value::Timestamp(UTCTimestamp::now()?).try_into()?)
                    }
                } else {
                    Ok(Value::Timestamp(UTCTimestamp::now()?).try_into()?)
                }
            }
            Expr::Ref { name } => Ok(pl::col(name)),
            Expr::Lit { value } => Ok(value.clone().try_into()?),
            Expr::Repeat { expr, count } => {
                let expr = expr.as_ref().into_polars_expr(ctx.clone())?;
                let count = count.as_ref().into_polars_expr(ctx)?;
                Ok(expr.repeat_by(count))
            }
            Expr::DateTimeLiteral {
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
                timezone,
            } => {
                let dt_args = pl::DatetimeArgs::new(pl::lit(*year), pl::lit(*month), pl::lit(*day))
                    .with_hms(pl::lit(*hour), pl::lit(*minute), pl::lit(*second))
                    .with_microsecond(pl::lit(*microsecond))
                    .with_time_zone(timezone.clone());
                Ok(pl::datetime(dt_args))
            }
            Expr::Binary { op, left, right } => {
                let left = left.as_ref().into_polars_expr(ctx.clone())?;
                let right = right.as_ref().into_polars_expr(ctx)?;
                Ok(pl::binary_expr(left, op.into(), right))
            }
            Expr::Cast { expr, dtype } => {
                let expr = expr.as_ref().into_polars_expr(ctx)?;
                Ok(pl::cast(expr, dtype.try_into()?))
            }
            Expr::Unary { op, expr } => {
                let expr = expr.as_ref().into_polars_expr(ctx)?;
                match op {
                    UnOp::Not => Ok(pl::not(expr)),
                    UnOp::Neg => Ok(-expr),
                }
            }
            Expr::Zip { fields, .. } => {
                let index_col_name = ctx
                    .as_ref()
                    .ok_or_else(|| anyhow!("can not zip without index column"))?
                    .index_col_name()
                    .ok_or_else(|| anyhow!("can not zip without index column"))?;
                // explode all fields, build a struct with the exploded fields
                // build a series of index_col_name of the same size and do
                // implode over index_col_name
                let mut names = Vec::with_capacity(fields.len());
                let mut pexprs = Vec::with_capacity(fields.len());
                for (name, expr) in fields {
                    let expr = expr.as_ref().into_polars_expr(ctx.clone())?;
                    names.push(name);
                    pexprs.push(expr);
                }
                let horizontal_min = pl::min_horizontal(
                    pexprs.iter().map(|e| e.clone().list().len()).collect_vec(),
                )?;
                let fields: Vec<_> = names
                    .iter()
                    .zip(pexprs)
                    .map(|(name, expr)| {
                        let pexpr = expr
                            .list()
                            .head(horizontal_min.clone())
                            .explode()
                            .alias(name);
                        Ok(pexpr)
                    })
                    .collect::<Result<_>>()?;
                let struct_expr = pl::as_struct(fields)
                    .implode()
                    .over(&[pl::col(index_col_name)]);
                Ok(struct_expr)
            }
            Expr::MakeStruct { fields, .. } => {
                let fields: Vec<_> = fields
                    .iter()
                    .map(|(name, expr)| {
                        let plexpr = expr.as_ref().into_polars_expr(ctx.clone());
                        match plexpr {
                            Ok(expr) => Ok(expr.alias(name)),
                            Err(e) => Err(e),
                        }
                    })
                    .collect::<Result<_>>()?;
                Ok(pl::as_struct(fields))
            }
            Expr::Case {
                when_thens,
                otherwise,
            } => {
                let otherwise: PolarsExpr = match otherwise {
                    None => lit(Value::None).as_ref().into_polars_expr(ctx.clone())?,
                    Some(o) => o.as_ref().into_polars_expr(ctx.clone())?,
                };
                match when_thens.len() {
                    0 => bail!("when_thens is empty"),
                    1 => {
                        let (w, t) = when_thens.first().unwrap();
                        let w = w.into_polars_expr(ctx.clone())?;
                        let t = t.into_polars_expr(ctx.clone())?;
                        Ok(pl::when(w).then(t).otherwise(otherwise))
                    }
                    _ => {
                        let (w1, t1) = when_thens.first().unwrap();
                        let w1 = w1.into_polars_expr(ctx.clone())?;
                        let t1 = t1.into_polars_expr(ctx.clone())?;
                        let (w2, t2) = when_thens.iter().skip(1).next().unwrap();
                        let w2 = w2.into_polars_expr(ctx.clone())?;
                        let t2 = t2.into_polars_expr(ctx.clone())?;
                        let mut ret = pl::when(w1).then(t1).when(w2).then(t2);
                        for (w, t) in when_thens.iter().skip(2) {
                            let w = w.into_polars_expr(ctx.clone())?;
                            let t = t.into_polars_expr(ctx.clone())?;
                            ret = ret.when(w).then(t);
                        }
                        Ok(ret.otherwise(otherwise))
                    }
                }
            }
            Expr::IsNull { expr } => {
                let expr = expr.as_ref().into_polars_expr(ctx)?;
                Ok(pl::is_null(expr))
            }
            Expr::FillNull { expr, default } => {
                let expr = expr.as_ref().into_polars_expr(ctx.clone())?;
                let default = default.as_ref().into_polars_expr(ctx)?;
                Ok(pl::Expr::Function {
                    input: vec![expr, default],
                    function: pl::FunctionExpr::FillNull,
                    options: FunctionOptions::default(),
                })
            }
            Expr::MathFn { func, expr } => {
                let expr = expr.as_ref().into_polars_expr(ctx.clone())?;
                if matches!(func, MathFn::ToString) {
                    return Ok(expr);
                }
                let (func, options) = match func {
                    MathFn::Abs => (pl::FunctionExpr::Abs, FunctionOptions::default()),
                    MathFn::Ceil => (pl::FunctionExpr::Ceil, FunctionOptions::default()),
                    MathFn::Floor => (pl::FunctionExpr::Floor, FunctionOptions::default()),
                    MathFn::Round { precision } => (
                        pl::FunctionExpr::Round {
                            decimals: *precision as u32,
                        },
                        FunctionOptions::default(),
                    ),
                    MathFn::ToString => unreachable!(),
                    MathFn::Pow { exponent } => {
                        let exponent = exponent.as_ref().into_polars_expr(ctx)?;
                        return Ok(expr.pow(exponent));
                    }
                    MathFn::Log { base } => return Ok(expr.log(*base as f64)),
                    MathFn::Sqrt => return Ok(expr.sqrt()),
                    MathFn::Sin => return Ok(expr.sin()),
                    MathFn::Cos => return Ok(expr.cos()),
                    MathFn::Tan => return Ok(expr.tan()),
                    MathFn::Asin => return Ok(expr.arcsin()),
                    MathFn::Acos => return Ok(expr.arccos()),
                    MathFn::Atan => return Ok(expr.arctan()),
                    MathFn::IsNan => return Ok(expr.is_nan()),
                    MathFn::IsInfinite => return Ok(expr.is_infinite()),
                };
                Ok(PolarsExpr::Function {
                    input: vec![expr],
                    function: func,
                    options,
                })
            }
            Expr::StringFn { func, expr } => {
                let expr = expr.as_ref().into_polars_expr(ctx.clone())?;
                let str = expr.clone().str();
                match func.as_ref() {
                    StringFn::Len => Ok(str.len_chars()),
                    StringFn::ToInt => Ok(str.to_integer(pl::lit(10), false)),
                    StringFn::ToLower => Ok(str.to_lowercase()),
                    StringFn::ToUpper => Ok(str.to_uppercase()),
                    StringFn::Contains { key } => {
                        Ok(str.contains_literal(key.as_ref().into_polars_expr(ctx)?))
                    }
                    StringFn::StartsWith { key } => {
                        Ok(str.starts_with(key.as_ref().into_polars_expr(ctx)?))
                    }
                    StringFn::EndsWith { key } => {
                        Ok(str.ends_with(key.as_ref().into_polars_expr(ctx)?))
                    }
                    StringFn::Concat { other } => Ok(pl::Expr::Function {
                        input: vec![expr.clone(), other.as_ref().into_polars_expr(ctx)?],
                        function: pl::FunctionExpr::StringExpr(StringFunction::ConcatHorizontal {
                            delimiter: "".to_string(),
                            ignore_nulls: false,
                        }),
                        options: FunctionOptions::default(),
                    }),
                    StringFn::Strptime { format, timezone } => {
                        let strptime_opts = pl::StrptimeOptions {
                            format: Some(format.clone()),
                            strict: true,
                            exact: true,
                            cache: false,
                        };
                        let timezone = timezone.as_deref().map(|s| s.to_string());
                        Ok(str.to_datetime(
                            Some(PolarsTimeUnit::Microseconds),
                            timezone,
                            strptime_opts,
                            pl::lit("raise"),
                        ))
                    }
                    StringFn::JsonDecode { dtype } => {
                        // NOTE: when dtype is set to timestamp, we always
                        // parse the json as micros since epoch (unlike the auto
                        // detection we do in our own json -> value conversion)
                        // NOTE: when dtype is set to int, polars will parse even
                        // the floats and truncate them to int
                        let pltype = dtype.try_into()?;
                        let expr = str.json_decode(Some(pltype), None);
                        Ok(expr)
                    }
                    StringFn::Split { sep } => {
                        let sep = lit(sep.clone()).into_polars_expr(ctx)?;
                        Ok(str.split(sep))
                    }
                    StringFn::JsonExtract { path } => {
                        let path = lit(path.clone()).into_polars_expr(ctx)?;
                        Ok(str.json_path_match(path))
                    }
                }
            }
            Expr::ListFn { list, func } => {
                let expr = list.as_ref().into_polars_expr(ctx.clone())?;
                let list = expr.clone().list();
                match func.as_ref() {
                    ListFn::Len => Ok(pl::when(expr.is_null())
                        .then(pl::lit(NULL))
                        .otherwise(list.len())),
                    ListFn::Contains { item } => {
                        // if list is null, return null
                        // if item is null and list is not empty, return null,
                        // else return False
                        // if item is not null and is in list, return true
                        // if item is not null, list has null, return null
                        // return false
                        let item = item.as_ref().into_polars_expr(ctx)?;
                        let len = list.len();
                        Ok(pl::when(
                            expr.clone()
                                .is_null()
                                .or(item.clone().is_null().and(len.clone().gt(pl::lit(0)))),
                        )
                        .then(pl::lit(NULL))
                        .when(item.clone().is_null())
                        .then(pl::lit(false))
                        .when(expr.clone().list().contains(item.clone()))
                        .then(pl::lit(true))
                        .when(len.gt(expr.clone().list().drop_nulls().list().len()))
                        .then(pl::lit(NULL))
                        .otherwise(pl::lit(false)))
                    }
                    ListFn::HasNull => {
                        let without_nulls = expr.clone().list().drop_nulls();
                        Ok(list.len().gt(without_nulls.list().len()))
                    }
                    ListFn::Get { index } => {
                        let index = index.as_ref().into_polars_expr(ctx)?;
                        Ok(list.get(index, true))
                    }
                    ListFn::Sum => {
                        // if list has any nulls, return null else return sum
                        let list_without_nulls = expr.clone().list().drop_nulls();
                        Ok(pl::when(list.len().gt(list_without_nulls.list().len()))
                            .then(pl::lit(NULL))
                            .otherwise(expr.list().sum()))
                    }
                    ListFn::Mean => {
                        // if list has any nulls, return null else return sum / len
                        let without_nulls = expr.clone().list().drop_nulls();
                        Ok(
                            pl::when(expr.clone().list().len().gt(without_nulls.list().len()))
                                .then(pl::lit(NULL))
                                .otherwise(expr.list().mean()),
                        )
                    }
                    ListFn::Min => {
                        // if list has any nulls, return null else return min
                        let without_nulls = expr.clone().list().drop_nulls();
                        Ok(
                            pl::when(expr.clone().list().len().gt(without_nulls.list().len()))
                                .then(pl::lit(NULL))
                                .otherwise(expr.list().min()),
                        )
                    }
                    ListFn::Max => {
                        // if list has any nulls, return null else return max
                        let without_nulls = expr.clone().list().drop_nulls();
                        Ok(
                            pl::when(expr.clone().list().len().gt(without_nulls.list().len()))
                                .then(pl::lit(NULL))
                                .otherwise(expr.list().max()),
                        )
                    }
                    ListFn::All => {
                        // if list has any nulls, return null else return all
                        let without_nulls = expr.clone().list().drop_nulls();
                        Ok(
                            pl::when(expr.clone().list().len().gt(without_nulls.list().len()))
                                .then(pl::lit(NULL))
                                .otherwise(expr.list().all()),
                        )
                    }
                    ListFn::Any => {
                        // if list is null, return NULL
                        // else if it has a true, return true
                        // else if it has even one NULL, return NULL
                        // else return False
                        let without_nulls = expr.clone().list().drop_nulls();
                        Ok(pl::when(expr.clone().is_null())
                            .then(pl::lit(NULL))
                            .when(expr.clone().list().any())
                            .then(pl::lit(true))
                            .when(expr.clone().list().len().gt(without_nulls.list().len()))
                            .then(pl::lit(NULL))
                            .otherwise(pl::lit(false)))
                    }
                    ListFn::Filter { predicate, .. } => {
                        let predicate = predicate.as_ref().into_polars_expr(ctx)?;
                        let predicate = pl::col("").filter(predicate);
                        let ret = pl::ListNameSpaceExtension::eval(expr.list(), predicate, false);
                        Ok(ret)
                    }
                    ListFn::Map { func, .. } => {
                        let func = func.as_ref().into_polars_expr(ctx)?;
                        Ok(pl::ListNameSpaceExtension::eval(expr.list(), func, false))
                    }
                }
            }
            Expr::StructFn { struct_, func } => {
                let expr = struct_.as_ref().into_polars_expr(ctx)?;
                let struct_ = expr.clone().struct_();
                match func.as_ref() {
                    StructFn::Get { field } => Ok(struct_.field_by_name(field)),
                }
            }
            Expr::DatetimeFn { func, expr } => {
                let expr = expr.as_ref().into_polars_expr(ctx.clone())?;
                let datetime = expr.clone().dt();
                match func.as_ref() {
                    DateTimeFn::SinceEpoch { unit } => {
                        let dt_args =
                            pl::DatetimeArgs::default().with_time_zone(Some("UTC".to_string()));
                        let epoch = pl::datetime(dt_args);

                        let duration = (expr.clone() - epoch).dt();
                        Ok(pl::when(expr.is_null())
                            .then(pl::lit(NULL))
                            .otherwise(match unit {
                                TimeUnit::Microsecond => duration.total_microseconds(),
                                TimeUnit::Millisecond => duration.total_milliseconds(),
                                TimeUnit::Second => duration.total_seconds(),
                                TimeUnit::Minute => duration.total_minutes(),
                                TimeUnit::Hour => duration.total_hours(),
                                TimeUnit::Day => duration.total_days(),
                                TimeUnit::Week => pl::binary_expr(
                                    duration.total_days(),
                                    PolarsOperator::FloorDivide,
                                    pl::lit(7),
                                ),
                                TimeUnit::Month => {
                                    panic!("month is not a valid time unit for since_epoch")
                                }
                                TimeUnit::Year => pl::binary_expr(
                                    duration.total_days(),
                                    PolarsOperator::FloorDivide,
                                    pl::lit(365),
                                ),
                            }))
                    }
                    DateTimeFn::Since { other, unit } => {
                        let other = other.as_ref().into_polars_expr(ctx)?;
                        let duration = (expr.clone() - other).dt();
                        Ok(pl::when(expr.is_null())
                            .then(pl::lit(NULL))
                            .otherwise(match unit {
                                TimeUnit::Microsecond => duration.total_microseconds(),
                                TimeUnit::Millisecond => duration.total_milliseconds(),
                                TimeUnit::Second => duration.total_seconds(),
                                TimeUnit::Minute => duration.total_minutes(),
                                TimeUnit::Hour => duration.total_hours(),
                                TimeUnit::Day => duration.total_days(),
                                TimeUnit::Week => pl::binary_expr(
                                    duration.total_days(),
                                    PolarsOperator::FloorDivide,
                                    pl::lit(7),
                                ),
                                TimeUnit::Month => {
                                    panic!("month is not a valid time unit for since_epoch")
                                }
                                TimeUnit::Year => pl::binary_expr(
                                    duration.total_days(),
                                    PolarsOperator::FloorDivide,
                                    pl::lit(365),
                                ),
                            }))
                    }
                    DateTimeFn::Part { unit, timezone } => {
                        let datetime = match timezone {
                            Some(tz) => datetime.convert_time_zone(tz.to_string()).dt(),
                            None => datetime,
                        };
                        match unit {
                            TimeUnit::Year => Ok(datetime.year()),
                            TimeUnit::Month => Ok(datetime.month()),
                            TimeUnit::Day => Ok(datetime.day()),
                            TimeUnit::Hour => Ok(datetime.hour()),
                            TimeUnit::Minute => Ok(datetime.minute()),
                            TimeUnit::Second => Ok(datetime.second()),
                            TimeUnit::Millisecond => Ok(datetime.millisecond()),
                            TimeUnit::Microsecond => Ok(datetime.microsecond()),
                            TimeUnit::Week => Ok(datetime.week()),
                        }
                    }
                    DateTimeFn::Stftime { format, timezone } => {
                        let datetime = match timezone {
                            Some(tz) => datetime.convert_time_zone(tz.to_string()).dt(),
                            None => datetime,
                        };
                        Ok(datetime.strftime(format))
                    }
                }
            }
            Expr::FromEpoch { expr, unit } => {
                let expr = expr.as_ref().into_polars_expr(ctx)?;
                let expr_is_null = expr.clone().is_null();
                // Polars never sees any timezone other than UTC
                let tz = "UTC".to_string();
                let dt_args = pl::DatetimeArgs::default().with_time_zone(Some(tz));
                let epoch = pl::datetime(dt_args);
                let duration = match unit {
                    TimeUnit::Microsecond => pl::DurationArgs::new().with_microseconds(expr),
                    TimeUnit::Millisecond => pl::DurationArgs::new().with_milliseconds(expr),
                    TimeUnit::Second => pl::DurationArgs::new().with_seconds(expr),
                    TimeUnit::Minute => pl::DurationArgs::new().with_minutes(expr),
                    TimeUnit::Hour => pl::DurationArgs::new().with_hours(expr),
                    TimeUnit::Day => pl::DurationArgs::new().with_days(expr),
                    TimeUnit::Week => pl::DurationArgs::new().with_days(expr * pl::lit(7)),
                    TimeUnit::Month => panic!("month is not a valid time unit for from_epoch"),
                    TimeUnit::Year => pl::DurationArgs::new().with_days(expr * pl::lit(365)),
                };
                Ok(pl::when(expr_is_null)
                    .then(pl::lit(NULL))
                    .otherwise(epoch + pl::duration(duration)))
            }
            Expr::DictFn { dict, func } => {
                let expr = dict.as_ref().into_polars_expr(ctx.clone())?;
                // internally, dicts are lists of structs
                let dict = expr.clone().list();
                match func.as_ref() {
                    DictFn::Get { key, default } => {
                        let key = key.as_ref().into_polars_expr(ctx.clone())?;
                        let default = match default {
                            Some(e) => Some(e.as_ref().into_polars_expr(ctx)?),
                            None => None,
                        };
                        // use list eval to create 1 for match and 0 for no match
                        // we will then do arg_max to get the index of the first match
                        // and then use that index to get the value from the dict
                        let matches = dict.eval(
                            pl::col("").struct_().field_by_name("__key__").eq(key),
                            false,
                        );
                        let any_match = matches.clone().list().any();
                        let zero_one = matches.clone().list().eval(
                            pl::when(pl::col("")).then(pl::lit(1)).otherwise(pl::lit(0)),
                            false,
                        );
                        let index = zero_one.list().arg_max();
                        let value = expr
                            .clone()
                            .list()
                            .get(index, true)
                            .struct_()
                            .field_by_name("__value__");
                        let otherwise = default.unwrap_or(pl::lit(NULL));
                        Ok(pl::when(expr.is_null())
                            .then(pl::lit(NULL))
                            .otherwise(pl::when(any_match).then(value).otherwise(otherwise)))
                    }
                    DictFn::Len => {
                        // if dict is null, return null
                        // else return len
                        Ok(pl::when(expr.is_null())
                            .then(pl::lit(NULL))
                            .otherwise(dict.len()))
                    }
                    DictFn::Contains { key } => {
                        let key = key.as_ref().into_polars_expr(ctx)?;
                        let matches = dict.eval(
                            pl::col("").struct_().field_by_name("__key__").eq(key),
                            false,
                        );
                        let any_match = matches.clone().list().any();
                        Ok(any_match)
                    }
                }
            }
        }
    }

    pub fn compile(&self, schema: Arc<Schema>) -> Result<CompiledExpr> {
        let scope = HashMap::new();
        self.compile_scoped(schema, &scope)
    }

    fn compile_scoped(
        &self,
        schema: Arc<Schema>,
        scope: &HashMap<String, Type>,
    ) -> Result<CompiledExpr> {
        use Type::*;
        match self {
            Expr::Var { name } => {
                let dtype = scope
                    .get(name)
                    .ok_or_else(|| anyhow!("variable '{}' not found in scope", name))?;
                Ok(CompiledExpr {
                    rewritten: self.clone(),
                    original: self.clone(),
                    schema,
                    dtype: dtype.clone(),
                })
            }
            Expr::Now => Ok(CompiledExpr {
                rewritten: self.clone(),
                original: self.clone(),
                schema,
                dtype: Type::Timestamp,
            }),
            Expr::Lit { value } => Ok(CompiledExpr {
                rewritten: self.clone(),
                original: self.clone(),
                schema,
                dtype: natural_type(value),
            }),
            Expr::DateTimeLiteral {
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
                timezone,
                ..
            } => {
                if month < &1 || month > &12 {
                    bail!("invalid expression: month must be between 1 and 12");
                }
                if day < &1 || day > &31 {
                    bail!("invalid expression: day must be between 1 and 31");
                }
                if hour > &23 {
                    bail!("invalid expression: hour must be between 0 and 23");
                }
                if minute > &59 {
                    bail!("invalid expression: minute must be between 0 and 59");
                }
                if second > &59 {
                    bail!("invalid expression: second must be between 0 and 59");
                }
                if microsecond > &999_999 {
                    bail!("invalid expression: microsecond must be between 0 and 999_999");
                }
                // check if timezone is valid
                if let Some(tz) = timezone {
                    validate_timezone(&tz, self)?;
                }
                Ok(CompiledExpr {
                    rewritten: self.clone(),
                    original: self.clone(),
                    schema,
                    dtype: Timestamp,
                })
            }
            Expr::FromEpoch { expr, unit } => {
                // verify that expr is of type int
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                if !promotable(expr.dtype(), &Type::optional(Int)) {
                    bail!(
                        "invalid expression: expected int for function 'from_epoch' but found {:?}",
                        expr.dtype()
                    );
                }
                let valid_units = vec![
                    TimeUnit::Microsecond,
                    TimeUnit::Millisecond,
                    TimeUnit::Second,
                ];
                if !valid_units.contains(unit) {
                    bail!(
                        "invalid expression: invalid time unit {:?} for function 'from_epoch', valid units are {:?}",
                        unit, valid_units
                    );
                }
                let basetype = Timestamp;
                // make optional if expr is optional
                let outtype = match &expr.dtype() {
                    Null | Optional(_) => Type::optional(basetype),
                    _ => basetype,
                };
                let rewritten = match expr.rewritten.is_null() {
                    true => none_as(&outtype),
                    _ => Expr::FromEpoch {
                        expr: Box::new(expr.rewritten.clone()),
                        unit: unit.clone(),
                    },
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::Repeat { expr, count } => {
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                let count = count.compile_scoped(schema.clone(), scope)?;
                // verify that count is of type int
                if !promotable(count.dtype(), &Int) {
                    bail!(
                        "invalid expression: expected int for function 'repeat' but found {:?}",
                        count.dtype()
                    );
                }
                let outtype = Type::List(Box::new(expr.dtype().clone()));
                Ok(CompiledExpr {
                    rewritten: self.clone(),
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::Zip {
                struct_type,
                fields,
            } => {
                if fields.is_empty() {
                    bail!("invalid expression: struct must have at least one field");
                }
                // all field names must be unique
                if fields.iter().unique_by(|(name, _)| name).count() != fields.len() {
                    bail!("invalid expression: struct field names must be unique");
                }

                if fields.len() != struct_type.fields().len() {
                    bail!(
                        "invalid expression: expected {} fields in struct but found {}",
                        struct_type.fields().len(),
                        fields.len()
                    );
                }
                let needed_fields = struct_type
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f.dtype()))
                    .collect::<HashMap<_, _>>();
                for (name, _) in fields {
                    if !needed_fields.contains_key(&name.as_str()) {
                        bail!(
                            "invalid expression: struct field '{}' not found in the zip expression",
                            name
                        );
                    }
                }
                let mut rewritten_fields = vec![];
                for (name, expr) in fields {
                    let needed_type = needed_fields.get(&name.as_str()).unwrap();
                    let expr = expr.compile_scoped(schema.clone(), scope)?;
                    // we need expr's type to be List[T] where T can be promoted to
                    // the needed type of the struct field
                    match expr.dtype() {
                        List(t) => {
                            if !promotable(t, needed_type) {
                                bail!(
                                    "invalid zip: expected struct field '{}' to be of type {:?} but found {:?}",
                                name,
                                needed_type,
                                expr.dtype(),
                                );
                            }
                            let needed_list_type = List(Box::new((*needed_type).clone()));
                            rewritten_fields.push((name.clone(), Box::new(expr.safecast(&needed_list_type)?)));
                        }
                        _ => bail!("invalid zip: expression for field '{}' expected to be a list, found {:?}", name, expr.dtype()),
                    };
                }
                let rewritten = Expr::Zip {
                    struct_type: struct_type.clone(),
                    fields: rewritten_fields,
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: Type::List(Box::new(Type::Struct(Box::new(struct_type.clone())))),
                })
            }
            Expr::MakeStruct {
                struct_type,
                fields,
            } => {
                if fields.is_empty() {
                    bail!("invalid expression: struct must have at least one field");
                }
                // all field names must be unique
                if fields.iter().unique_by(|(name, _)| name).count() != fields.len() {
                    bail!("invalid expression: struct field names must be unique");
                }
                if fields.len() != struct_type.fields().len() {
                    bail!(
                        "invalid expression: expected {} fields in struct but found {}",
                        struct_type.fields().len(),
                        fields.len()
                    );
                }
                let needed_fields = struct_type
                    .fields()
                    .iter()
                    .map(|f| (f.name(), f.dtype()))
                    .collect::<HashMap<_, _>>();
                for (name, _) in fields {
                    if !needed_fields.contains_key(&name.as_str()) {
                        bail!(
                            "invalid expression: field '{}' not found in the schema",
                            name
                        );
                    }
                }
                let mut all_none = true;
                let mut rewritten_fields = vec![];
                for (name, expr) in fields {
                    let expr = expr.compile_scoped(schema.clone(), scope)?;
                    let needed_type = needed_fields.get(&name.as_str()).unwrap();
                    if !promotable(expr.dtype(), needed_type) {
                        bail!(
                            "invalid expression: expected field '{}' to be of type {:?} but found {:?}",
                            name,
                            needed_type,
                            expr.dtype(),
                        );
                    }
                    all_none = all_none && expr.rewritten.is_null();
                    rewritten_fields.push((name.clone(), Box::new(expr.safecast(needed_type)?)));
                }
                let rewritten = match all_none {
                    // if all fields are none, it must mean that struct can take
                    // optional[T] for all fields. In that case, while the output
                    // type stays Optional[struct], in practice it will be none expression
                    true => none_as(&Type::Struct(Box::new(struct_type.clone()))),
                    _ => Expr::MakeStruct {
                        struct_type: struct_type.clone(),
                        fields: rewritten_fields,
                    },
                };

                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: Type::Struct(Box::new(struct_type.clone())),
                })
            }
            Expr::Ref { name } => match schema.find(name) {
                None => bail!("field '{}' not found in the schema", name),
                Some(field) => Ok(CompiledExpr {
                    rewritten: self.clone(),
                    original: self.clone(),
                    schema: schema.clone(),
                    dtype: field.dtype().clone(),
                }),
            },
            Expr::ListFn { list, func } => {
                let list = list.compile_scoped(schema.clone(), scope)?;
                let dtype = list.dtype();
                let eltype = match dtype.inner() {
                    Null => None,
                    List(l) => Some(l.as_ref().clone()),
                    Embedding(_) => Some(Float),
                    _ => bail!("invalid expression: expected list but found {:?}", dtype),
                };
                // compute the output type assuming the list is not optional
                match func.as_ref() {
                    ListFn::Contains { item } => {
                        // list is none
                        if eltype.is_none() {
                            return Ok(CompiledExpr {
                                rewritten: none_as(&Type::optional(Bool)),
                                original: self.clone(),
                                schema,
                                dtype: Type::optional(Bool),
                            });
                        }
                        let eltype = eltype.as_ref().unwrap();
                        let item = item.compile_scoped(schema.clone(), scope)?;
                        match item.dtype() {
                            t if promotable(t, &eltype) => {
                                // if either list or item is optional, output is optional bool
                                let outtype = if matches!(dtype, Null | Optional(_)) || matches!(eltype, Null | Optional(_)) {
                                    Type::optional(Bool)
                                } else {
                                    Bool
                                };
                                let rewritten = Expr::ListFn {
                                    list: Box::new(list.rewritten.clone()),
                                    func: Box::new(ListFn::Contains { item: item.rewritten }),
                                };
                                Ok(CompiledExpr {
                                    rewritten: Expr::Cast {
                                        expr: Box::new(rewritten),
                                        dtype: outtype.clone(),
                                    },
                                    original: self.clone(),
                                    schema,
                                    dtype: outtype,
                                })
                            }
                            _ => bail!(
                                "invalid expression: expected item to be of type {:?} but found {:?}",
                                eltype,
                                item.dtype()
                            ),
                        }
                    }
                    ListFn::HasNull => {
                        if eltype.is_none() {
                            return Ok(CompiledExpr {
                                rewritten: none_as(&Type::optional(Bool)),
                                original: self.clone(),
                                schema,
                                dtype: Type::optional(Bool),
                            });
                        }
                        let eltype = eltype.as_ref().unwrap();
                        // if element type is not null or optional, output is false
                        let rewritten = match &eltype {
                            Null | Optional(_) => Expr::ListFn {
                                list: Box::new(list.rewritten.clone()),
                                func: Box::new(ListFn::HasNull),
                            },
                            _ => Expr::Lit {
                                value: Value::Bool(false),
                            },
                        };
                        // if list is optional, output is optional bool else bool
                        // if list is None, output is None
                        let (outtype, rewritten) = match &dtype {
                            Optional(_) => (
                                Type::optional(Bool),
                                Expr::Case {
                                    when_thens: vec![(
                                        Expr::IsNull {
                                            expr: Box::new(list.rewritten.clone()),
                                        },
                                        none_as(&Type::optional(Bool)),
                                    )],
                                    otherwise: Some(Box::new(rewritten.clone())),
                                },
                            ),
                            _ => (Bool, rewritten),
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    _ => {
                        // compute the output type assuming the list is not optional
                        let (outtype, func) = match func.as_ref() {
                            ListFn::Len if eltype.is_none() => {
                                return Ok(CompiledExpr {
                                    rewritten: none_as(&Type::optional(Int)),
                                    original: self.clone(),
                                    schema,
                                    dtype: Type::optional(Int),
                                })
                            }
                            ListFn::Len => (Int, ListFn::Len),
                            ListFn::Get { index } => {
                                let index = index.compile_scoped(schema.clone(), scope)?;
                                match index.dtype() {
                                    // accessing out of bounds index returns None
                                    // so output type is optional[eltype]
                                    Int if eltype.is_none() => bail!("invalid expression: can not access index of None list"),
                                    Int => (Type::optional(eltype.as_ref().unwrap().clone()), ListFn::Get { index: index.rewritten }),
                                    _ => bail!(
                                        "invalid expression: expected index to be of int type but found {:?}",
                                        index.dtype()
                                    ),
                                }
                            }
                            ListFn::HasNull => unreachable!("handled separately"),
                            ListFn::Contains { .. } => unreachable!("handled separately"),
                            ListFn::Sum => {
                                if eltype.is_none() {
                                    return Ok(CompiledExpr {
                                        rewritten: none_as(&Type::optional(Int)),
                                        original: self.clone(),
                                        schema,
                                        dtype: Type::optional(Int),
                                    });
                                }
                                if !promotable(eltype.as_ref().unwrap(), &Type::optional(Float)) {
                                    bail!("invalid expression: expected list of int/float for function '{:?}' but found {:?}", func, eltype.as_ref().unwrap());
                                }
                                (eltype.as_ref().unwrap().clone(), func.as_ref().clone())
                            }
                            ListFn::Min | ListFn::Max => {
                                if eltype.is_none() {
                                    return Ok(CompiledExpr {
                                        rewritten: none_as(&Type::optional(Int)),
                                        original: self.clone(),
                                        schema,
                                        dtype: Type::optional(Int),
                                    });
                                }
                                if !promotable(eltype.as_ref().unwrap(), &Type::optional(Float)) {
                                    bail!("invalid expression: expected list of int/float for function '{:?}' but found {:?}", func, eltype.as_ref().unwrap());
                                }
                                // min/max of list[T] is always list[Optional[T]]
                                // because if list is empty, we create None
                                let eltype = eltype.as_ref().unwrap();
                                let outtype = match eltype.is_nullable() {
                                    true => eltype.clone(),
                                    false => Type::optional(eltype.clone()),
                                };
                                (outtype, func.as_ref().clone())
                            }
                            ListFn::Mean => {
                                if eltype.is_none() {
                                    return Ok(CompiledExpr {
                                        rewritten: none_as(&Type::optional(Float)),
                                        original: self.clone(),
                                        schema,
                                        dtype: Type::optional(Float),
                                    });
                                }
                                let eltype = eltype.as_ref().unwrap();
                                if !promotable(eltype, &Type::optional(Float)) {
                                    bail!("invalid expression: expected list of int/float for function 'mean' but found {:?}", eltype);
                                }
                                // output type is always optional (since we return
                                // None on empty lists)
                                (Type::optional(Float), ListFn::Mean)
                            }
                            ListFn::All | ListFn::Any => {
                                if eltype.is_none() {
                                    return Ok(CompiledExpr {
                                        rewritten: none_as(&Type::optional(Bool)),
                                        original: self.clone(),
                                        schema,
                                        dtype: Type::optional(Bool),
                                    });
                                }
                                if !promotable(eltype.as_ref().unwrap(), &Type::optional(Bool)) {
                                    bail!("invalid expression: expected list of bool for function '{:?}' but found {:?}", func, eltype.as_ref().unwrap());
                                }
                                (eltype.as_ref().unwrap().clone(), func.as_ref().clone())
                            }
                            ListFn::Filter { var, predicate } => {
                                if scope.len() > 0 {
                                    let first = scope.keys().next().unwrap();
                                    bail!("invalid expression: can not have multi-level scoped variables, var `{}` in scope while evaluating {}", first, var);
                                }
                                if eltype.is_none() {
                                    bail!("can not apply `filter` on None list");
                                }
                                let eltype = eltype.as_ref().unwrap();
                                let mut new_scope = scope.clone();
                                new_scope.insert(var.to_string(), eltype.clone());
                                let predicate =
                                    predicate.compile_scoped(schema.clone(), &new_scope)?;
                                if predicate.dtype() != &Bool {
                                    bail!("invalid expression: expected predicate to be of type `bool` but found {:?}, expression: {:?}", predicate.dtype(), self);
                                }
                                let rewritten_func = ListFn::Filter {
                                    var: var.clone(),
                                    predicate: predicate.rewritten,
                                };
                                (dtype.clone(), rewritten_func)
                            }
                            ListFn::Map { var, func } => {
                                if scope.len() > 0 {
                                    let first = scope.keys().next().unwrap();
                                    bail!("invalid expression: can not have multi-level scoped variables, var `{}` in scope while evaluating {}", first, var);
                                }
                                if eltype.is_none() {
                                    bail!("can not apply `map` on None list");
                                }
                                let eltype = eltype.as_ref().unwrap();
                                let mut new_scope = scope.clone();
                                new_scope.insert(var.to_string(), eltype.clone());
                                let func = func.compile_scoped(schema.clone(), &new_scope)?;
                                let rewritten_func = ListFn::Map {
                                    var: var.clone(),
                                    func: func.rewritten.clone(),
                                };
                                let outtype = Type::List(Box::new(func.dtype().clone()));
                                (outtype, rewritten_func)
                            }
                        };
                        // adjust the output type if the list is optional
                        let outtype = if list.rewritten.is_null() || matches!(dtype, Optional(_)) {
                            Type::optional(outtype)
                        } else {
                            outtype
                        };
                        Ok(CompiledExpr {
                            rewritten: Expr::ListFn {
                                list: Box::new(list.rewritten),
                                func: Box::new(func),
                            },
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                }
            }
            Expr::StructFn { struct_, func } => {
                let struct_ = struct_.compile_scoped(schema.clone(), scope)?;
                let dtype = struct_.dtype();
                let stype = match dtype.inner() {
                    Struct(s) => s.as_ref(),
                    _ => bail!("invalid expression: expected struct but found {:?}", dtype),
                };
                // compute the output type assuming the struct is not optional
                let (outtype, func) =
                    match func.as_ref() {
                        StructFn::Get { field } => {
                            let field = stype.fields().iter().find(|f| f.name() == field).ok_or(
                                anyhow!(
                                    "invalid expression: field '{}' not found in struct {:?}",
                                    field,
                                    stype
                                ),
                            )?;
                            (field.dtype().clone(), func.as_ref().clone())
                        }
                    };
                // adjust the output type if the struct is optional
                let outtype = match &dtype {
                    Optional(_) => Type::optional(outtype),
                    _ => outtype,
                };
                Ok(CompiledExpr {
                    rewritten: Expr::StructFn {
                        struct_: Box::new(struct_.rewritten),
                        func: Box::new(func),
                    },
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::DictFn { dict, func } => {
                let dict = dict.compile_scoped(schema.clone(), scope)?;
                let dtype = dict.dtype();
                let eltype = match dtype.inner() {
                    Map(m) => m.as_ref().clone(),
                    _ => bail!("invalid expression: expected dict but found {:?}", dtype),
                };
                // compute the output type assuming the dict is not optional
                let (outtype, func) = match func.as_ref() {
                    DictFn::Len => (Int, DictFn::Len),
                    DictFn::Get { key, default } => {
                        let key = key.compile_scoped(schema.clone(), scope)?;
                        if !promotable(key.dtype(), &Type::optional(String)) {
                            bail!("invalid expression: expected key to be of string type but found {:?}", key.dtype());
                        }
                        let opt_eltype = match eltype.inner() {
                            Null | Optional(_) => eltype.clone(),
                            _ => Type::optional(eltype.clone()),
                        };
                        if key.dtype() == &Null {
                            return Ok(CompiledExpr {
                                rewritten: none_as(&opt_eltype),
                                original: self.clone(),
                                schema,
                                dtype: opt_eltype.clone(),
                            });
                        }
                        let default = default
                            .as_ref()
                            .map(|d| d.compile_scoped(schema.clone(), scope))
                            .transpose()?;
                        if let Some(default) = default.as_ref() {
                            if !promotable(default.dtype(), &opt_eltype) {
                                bail!("invalid expression: expected default value to be of type {:?} but found {:?}", opt_eltype, default.dtype());
                            }
                        }
                        let outtype = match &default {
                            None => opt_eltype,
                            // can unwrap because we verified that default can
                            // be promoted to optional[eltype]
                            Some(d) => lca(&eltype, &d.dtype()).unwrap(),
                        };
                        let newfn = DictFn::Get {
                            key: key.rewritten,
                            default: default.map(|d| d.rewritten),
                        };
                        (outtype, newfn)
                    }
                    DictFn::Contains { .. } => todo!(),
                };
                // adjust the output type if the dict is optional
                let outtype = match &dtype {
                    Optional(_) => Type::optional(outtype.clone()),
                    _ => outtype.clone(),
                };
                Ok(CompiledExpr {
                    rewritten: Expr::Cast {
                        expr: Box::new(Expr::DictFn {
                            dict: Box::new(dict.rewritten),
                            func: Box::new(func),
                        }),
                        dtype: outtype.clone(),
                    },
                    original: self.clone(),
                    schema,
                    dtype: outtype.clone(),
                })
            }
            Expr::StringFn { func, expr } => {
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                if !promotable(expr.dtype(), &Type::optional(String)) {
                    bail!(
                        "invalid expression: expected string type for function '{:?}' but found {:?}",
                        func,
                        expr.dtype(),
                    );
                }
                let (func, outtype, all_null) = match func.as_ref() {
                    StringFn::Len => (StringFn::Len, Int, false),
                    StringFn::ToLower => (StringFn::ToLower, String, false),
                    StringFn::ToUpper => (StringFn::ToUpper, String, false),
                    StringFn::ToInt => (StringFn::ToInt, Int, false),
                    StringFn::Contains { key }
                    | StringFn::Concat { other: key }
                    | StringFn::StartsWith { key }
                    | StringFn::EndsWith { key } => {
                        let expr = key.compile_scoped(schema.clone(), scope)?;
                        if !promotable(expr.dtype(), &Type::optional(String)) {
                            bail!(
                                "invalid expression: expected string type for function '{:?}' but found {:?}, expr: {:?}",
                                func,
                                expr.dtype(),
                                key
                            );
                        }
                        let outtype = match func.as_ref() {
                            StringFn::Contains { .. }
                            | StringFn::StartsWith { .. }
                            | StringFn::EndsWith { .. } => Bool,
                            StringFn::Concat { .. } => String,
                            _ => unreachable!(),
                        };
                        // adjust the output type if the string is optional
                        let outtype = match expr.dtype() {
                            Null | Optional(_) => Type::optional(outtype.clone()),
                            String => outtype.clone(),
                            _ => unreachable!(),
                        };
                        let all_null = expr.rewritten.is_null();
                        let rewritten = match func.as_ref() {
                            StringFn::Contains { .. } => StringFn::Contains {
                                key: expr.rewritten,
                            },
                            StringFn::StartsWith { .. } => StringFn::StartsWith {
                                key: expr.rewritten,
                            },
                            StringFn::EndsWith { .. } => StringFn::EndsWith {
                                key: expr.rewritten,
                            },
                            StringFn::Concat { .. } => StringFn::Concat {
                                other: expr.rewritten,
                            },
                            _ => unreachable!(),
                        };
                        (rewritten, outtype, all_null)
                    }
                    StringFn::Strptime { format, timezone } => {
                        // check if the format string is valid
                        let _ = chrono::format::strftime::StrftimeItems::new(format)
                            .parse()
                            .with_context(|| {
                                format!("invalid strftime format string: {}", format)
                            })?;
                        // verify that timezone, if present, is a valid timezone
                        if let Some(tz) = timezone {
                            validate_timezone(&tz, self)?;
                        }
                        (func.as_ref().clone(), Timestamp, false)
                    }
                    StringFn::JsonDecode { dtype } => (func.as_ref().clone(), dtype.clone(), false),
                    StringFn::Split { .. } => {
                        (func.as_ref().clone(), List(Box::new(String)), false)
                    }
                    StringFn::JsonExtract { path } => {
                        let as_path = jsonpath_rust::JsonPath::try_from(path.as_str());
                        if as_path.is_err() {
                            return Err(anyhow!(
                                "invalid json path in json_extract function: {}",
                                path
                            )
                            .context(as_path.err().unwrap()));
                        }
                        (func.as_ref().clone(), Type::optional(String), false)
                    }
                };

                // adjust the output type if the string is optional
                let outtype = match &expr.dtype() {
                    Null | Optional(_) => Type::optional(outtype.clone()),
                    _ => outtype.clone(),
                };
                let rewritten = match all_null || expr.rewritten.is_null() {
                    true => none_as(&outtype),
                    false => Expr::StringFn {
                        func: Box::new(func),
                        expr: Box::new(expr.rewritten),
                    },
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::MathFn { func, expr } => {
                // Validate: expr can be promoted to a numeric type
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                if !promotable(expr.dtype(), &Type::optional(Float)) {
                    bail!(
                        "invalid expression: math function {} expected argument to be of numeric type but found {:?}",
                        func,
                        expr.dtype()
                    );
                }

                match func {
                    MathFn::Ceil | MathFn::Floor => {
                        // ceil/floor always returns an integer, maybe optional
                        let outtype = match &expr.dtype {
                            Null | Optional(_) => Type::optional(Int),
                            _ => Int,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::Cast {
                                expr: Box::new(Expr::MathFn {
                                    func: func.clone(),
                                    expr: Box::new(expr.safecast(&Type::optional(Float))?),
                                }),
                                dtype: outtype.clone(),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::Round { precision } => {
                        // round returns an int when precision is 0 and float otherwise, maybe optional
                        let outtype = match (&expr.dtype, precision) {
                            (Null | Optional(_), 0) => Type::optional(Int),
                            (Null | Optional(_), _) => Type::optional(Float),
                            (_, 0) => Int,
                            (_, _) => Float,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::Cast {
                                expr: Box::new(Expr::MathFn {
                                    func: func.clone(),
                                    expr: Box::new(expr.safecast(&Type::optional(Float))?),
                                }),
                                dtype: outtype.clone(),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::ToString => {
                        let outtype = match &expr.dtype {
                            Null | Optional(_) => Type::optional(String),
                            _ => String,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::Cast {
                                expr: Box::new(Expr::MathFn {
                                    func: func.clone(),
                                    expr: Box::new(expr.rewritten),
                                }),
                                dtype: outtype.clone(),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::Abs => {
                        let outtype = match &expr.dtype {
                            Null => Type::optional(Int),
                            Optional(t) if t.as_ref().eq(&Int) => Type::optional(Int),
                            Optional(_) => Type::optional(Float),
                            Int => Int,
                            _ => Float,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::Cast {
                                expr: Box::new(Expr::MathFn {
                                    func: func.clone(),
                                    expr: Box::new(expr.safecast(&Type::optional(Float))?),
                                }),
                                dtype: outtype.clone(),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::Pow { exponent } => {
                        let exponent = exponent.compile_scoped(schema.clone(), scope)?;
                        if !promotable(exponent.dtype(), &Type::optional(Float)) {
                            bail!(
                                "invalid expression: exponent in function `pow` is of type {:?}, expected to be int/float, expr: {:?}",
                                exponent.dtype(),
                                exponent
                            );
                        }
                        if exponent.rewritten.is_null() || expr.rewritten.is_null() {
                            return Ok(CompiledExpr {
                                rewritten: none_as(&Float),
                                original: self.clone(),
                                schema,
                                dtype: Type::optional(Float),
                            });
                        }
                        // if even one of the types is float, the result is float
                        // otherwise it is int
                        let outtype = match (exponent.dtype().inner(), expr.dtype().inner()) {
                            (Float, _) | (_, Float) => Float,
                            _ => Int,
                        };
                        // if even one of the types is optional, the result is optional
                        let outtype = match (exponent.dtype(), expr.dtype()) {
                            (Type::Optional(_), _) | (_, Type::Optional(_)) => {
                                Type::optional(outtype)
                            }
                            _ => outtype,
                        };
                        Ok(CompiledExpr {
                            rewritten: Expr::MathFn {
                                func: MathFn::Pow {
                                    exponent: Box::new(exponent.rewritten.clone()),
                                },
                                expr: Box::new(expr.rewritten.clone()),
                            },
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::Log { .. }
                    | MathFn::Sin
                    | MathFn::Cos
                    | MathFn::Tan
                    | MathFn::Asin
                    | MathFn::Acos
                    | MathFn::Atan => {
                        let outtype = match &expr.dtype {
                            Null | Optional(_) => Type::optional(Float),
                            _ => Float,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::MathFn {
                                func: func.clone(),
                                expr: Box::new(expr.rewritten),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::Sqrt => {
                        let outtype = match &expr.dtype {
                            Null | Optional(_) => Type::optional(Float),
                            _ => Float,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::MathFn {
                                func: func.clone(),
                                expr: Box::new(expr.rewritten),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                    MathFn::IsNan | MathFn::IsInfinite => {
                        let outtype = match &expr.dtype {
                            Null | Optional(_) => Type::optional(Bool),
                            _ => Bool,
                        };
                        let rewritten = match expr.rewritten.is_null() {
                            true => none_as(&outtype),
                            false => Expr::MathFn {
                                func: func.clone(),
                                expr: Box::new(expr.rewritten),
                            },
                        };
                        Ok(CompiledExpr {
                            rewritten,
                            original: self.clone(),
                            schema,
                            dtype: outtype,
                        })
                    }
                }
            }
            Expr::DatetimeFn { func, expr } => {
                // Validate: expr can be promoted to optional timestamp
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                if !promotable(expr.dtype(), &Type::optional(Timestamp)) {
                    bail!(
                        "invalid expression: dt function `{:?}` used on expression of type {:?}, expected to be datetime",
                        func,
                        expr.dtype()
                    );
                }

                let (inner, func, always_null) = match func.as_ref() {
                    DateTimeFn::SinceEpoch { unit } => {
                        if matches!(unit, TimeUnit::Month) {
                            bail!(
                                "invalid expression: `month` is not a valid unit for since_epoch"
                            );
                        }
                        (Int, func.as_ref().clone(), false)
                    }
                    DateTimeFn::Since { other, unit } => {
                        let other = other.compile_scoped(schema.clone(), scope)?;
                        if !promotable(other.dtype(), &Type::optional(Timestamp)) {
                            bail!(
                                "`since` used with expression of type {:?}, expected to be datetime, expr: {:?}",
                                other.dtype(), other
                            );
                        }
                        if matches!(unit, TimeUnit::Month) {
                            bail!("invalid expression: `month` is not a valid unit for since");
                        }
                        // if other is optional, output is optional int else int
                        let inner = match &other.dtype {
                            Null | Optional(_) => Type::optional(Int),
                            Timestamp => Int,
                            _ => unreachable!(),
                        };
                        let always_null = other.rewritten.is_null();
                        let func = DateTimeFn::Since {
                            other: Box::new(other.rewritten),
                            unit: unit.clone(),
                        };
                        (inner, func, always_null)
                    }
                    DateTimeFn::Part { timezone, .. } => {
                        // verify that timezone, if present, is a valid timezone
                        if let Some(tz) = timezone {
                            validate_timezone(&tz, self)?;
                        }
                        (Int, func.as_ref().clone(), false)
                    }
                    DateTimeFn::Stftime { format, timezone } => {
                        // verify that the format string is valid
                        chrono::format::strftime::StrftimeItems::new(format)
                            .parse()
                            .with_context(|| {
                                format!("invalid strftime format string: {}", format)
                            })?;
                        // verify that timezone, if present, is a valid timezone
                        if let Some(tz) = timezone {
                            validate_timezone(&tz, self)?;
                        }
                        (String, func.as_ref().clone(), false)
                    }
                };
                // inner is the output type of the function if the input is not optional
                // now we need to adjust the output type based on the input type
                let outtype = match &expr.dtype {
                    Null | Optional(_) => Type::optional(inner),
                    _ => inner,
                };
                // also rewrite the expression to use null if input is null
                let rewritten = if always_null || matches!(expr.dtype, Null) {
                    none_as(&outtype)
                } else {
                    Expr::DatetimeFn {
                        func: Box::new(func),
                        expr: Box::new(expr.rewritten.clone()),
                    }
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::Unary { op, expr } => match op {
                UnOp::Not => {
                    let predicate = expr.compile_scoped(schema.clone(), scope)?;
                    // 1. Validate: predicate can be promoted to a boolean
                    if !promotable(predicate.dtype(), &Type::optional(Bool)) {
                        bail!("invalid expression: expected boolean expression");
                    }
                    // 2. Find output type: either a boolean or optional boolean
                    let outtype = match &predicate.dtype {
                        Bool => Bool,
                        Null => Type::optional(Bool),
                        Optional(t) if t.as_ref().eq(&Bool) => Type::optional(Bool),
                        _ => unreachable!(),
                    };

                    // 3. Rewrite: use null if predicate is null
                    let rewritten = match predicate.rewritten.is_null() {
                        true => none_as(&outtype),
                        _ => self.clone(),
                    };

                    Ok(CompiledExpr {
                        rewritten,
                        original: self.clone(),
                        schema,
                        dtype: outtype,
                    })
                }
                UnOp::Neg => {
                    let expr = expr.compile_scoped(schema.clone(), scope)?;
                    // 1. Validate: expr can be promoted to a numeric type
                    if !promotable(expr.dtype(), &Type::optional(Float)) {
                        bail!(
                            "invalid expression: expected numeric type but found {:?}",
                            expr.dtype()
                        );
                    }
                    // 2. Find output type: either a float or optional float
                    let outtype = match &expr.dtype {
                        Int => Int,
                        Float => Float,
                        Null => Type::optional(Float),
                        Optional(t) if t.as_ref().eq(&Int) => Type::optional(Int),
                        Optional(t) if t.as_ref().eq(&Float) => Type::optional(Float),
                        _ => unreachable!(),
                    };
                    // 3. Rewrite: use null if expr is null
                    let rewritten = match expr.rewritten.is_null() {
                        true => none_as(&outtype),
                        _ => self.clone(),
                    };
                    Ok(CompiledExpr {
                        rewritten,
                        original: self.clone(),
                        schema,
                        dtype: outtype,
                    })
                }
            },
            Expr::Binary { op, left, right } => {
                let left = left.compile_scoped(schema.clone(), scope)?;
                let right = right.compile_scoped(schema.clone(), scope)?;
                let ltype = left.dtype();
                let rtype = right.dtype();
                let (outtype, casttype) = match op {
                    BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Mod => {
                        // 1. Validate: both sides can be promoted to a numeric type
                        let max = Type::optional(Float);
                        if !promotable(ltype, &max) || !promotable(rtype, &max) {
                            bail!("invalid expression: both sides of '{}' must be numeric types but found {} & {}, left: {:?}, right: {:?}", op, ltype, rtype, left.original, right.original);
                        }

                        // 2. Find output type: either int, or float, or options
                        let outtype = match (ltype, rtype) {
                            (Null, Null) => Type::optional(Int),
                            // can unwrap because know both are numerics, so lca exists
                            (l, r) => lca(l, r).unwrap(),
                        };
                        (outtype.clone(), outtype)
                    }
                    BinOp::Div => {
                        // 1. Validate: both sides can be promoted to a numeric type
                        let max = Type::optional(Float);
                        if !promotable(ltype, &max) || !promotable(rtype, &max) {
                            bail!(
                                "invalid expression: both sides of '{}' must be numeric types but found {} & {}",
                                op,
                                ltype,
                                rtype
                            );
                        }
                        // can unwrap because we know both are numerics, so lca exists
                        let common = lca(ltype, rtype).unwrap();
                        // 2. Find output type: either a float or optional float
                        let outtype = match common {
                            Null | Optional(_) => Type::optional(Float),
                            _ => Float,
                        };
                        (outtype.clone(), outtype)
                    }
                    BinOp::FloorDiv => {
                        let max = Type::optional(Float);
                        if !promotable(ltype, &max) || !promotable(rtype, &max) {
                            bail!(
                                "invalid expression: both sides of '{}' must be numeric types but found {} & {}",
                                op,
                                ltype,
                                rtype
                            );
                        }
                        // can unwrap because we know both are numerics, so lca exists
                        let common = lca(ltype, rtype).unwrap();
                        // 2. Find output type: either int, or float, or options
                        let outtype = match &common {
                            Null => Type::optional(Int),
                            Int | Float | Optional(_) => common.clone(),
                            _ => unreachable!(
                                "valid numeric types are Int, Float, Optional[Int], Optional[Float]"
                            ),
                        };
                        (outtype.clone(), outtype)
                    }
                    BinOp::Eq | BinOp::Neq => {
                        // 1. Validate: both sides can be promoted to a common type
                        let common = match lca(ltype, rtype) {
                            Some(t) => t,
                            None => bail!(
                                "invalid expression: both sides of '{}' must be of compatible types but found {} & {}",
                                op,
                                ltype,
                                rtype
                            ),
                        };
                        // 2. Find output type: either a boolean or optional boolean
                        let (casttype, outtype) = match &common {
                            Null => (Type::optional(Int), Type::optional(Bool)),
                            Optional(_) => (common, Type::optional(Bool)),
                            _ => (common, Bool),
                        };
                        (outtype, casttype)
                    }
                    BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte => {
                        // 1. Validate: both sides can be promoted to numeric types
                        let max = Type::optional(Float);
                        if !promotable(ltype, &max) || !promotable(rtype, &max) {
                            bail!(
                                "invalid expression: both sides of '{}' must be numeric types but found {} & {}",
                                op,
                                ltype,
                                rtype
                            );
                        }
                        // 2. Find output type: either a boolean or optional boolean
                        // can unwrap because we know both are numerics, so lca exists
                        let common = lca(ltype, rtype).unwrap();
                        let (casttype, outtype) = match &common {
                            Null => (Type::optional(Int), Type::optional(Bool)),
                            Optional(_) => (common, Type::optional(Bool)),
                            _ => (common, Bool),
                        };
                        (outtype, casttype)
                    }
                    BinOp::And | BinOp::Or => {
                        // 1. Validate: both sides can be promoted to a boolean
                        if !promotable(ltype, &Type::optional(Bool))
                            || !promotable(rtype, &Type::optional(Bool))
                        {
                            bail!(
                                "invalid expression: both sides of '{}' must be boolean types but found {} & {}",
                                op,
                                ltype,
                                rtype
                            );
                        }
                        // 2. Find output type: either a boolean or optional boolean
                        // can unwrap because we know both are booleans, so lca exists
                        let common = lca(ltype, rtype).unwrap();
                        let outtype = match common {
                            Null | Optional(_) => Type::optional(Bool),
                            _ => Bool,
                        };
                        (outtype.clone(), outtype)
                    }
                };
                let rewritten = if left.rewritten.is_null() && right.rewritten.is_null() {
                    none_as(&outtype)
                } else if (left.rewritten.is_null() || right.rewritten.is_null())
                    && ![BinOp::And, BinOp::Or].contains(&op)
                {
                    none_as(&outtype)
                } else {
                    Expr::Binary {
                        op: op.clone(),
                        left: Box::new(left.safecast(&casttype)?),
                        right: Box::new(right.safecast(&casttype)?),
                    }
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: outtype.clone(),
                })
            }
            Expr::Case {
                when_thens,
                otherwise,
            } => {
                if when_thens.is_empty() {
                    bail!("invalid expression: CASE must have at least one WHEN clause");
                }
                let mut cwts = vec![];
                for (when, then) in when_thens {
                    let cw = when.compile_scoped(schema.clone(), scope)?;
                    let ct = then.compile_scoped(schema.clone(), scope)?;
                    if !promotable(cw.dtype(), &Type::optional(Bool)) {
                        bail!(
                            "expected expression {:?} to be boolean for WHEN but found type {:?}",
                            when,
                            cw.dtype()
                        );
                    }
                    cwts.push((cw, ct));
                }
                // missing otherwise is allowed, in which case it is treated as None
                let otherwise = match otherwise {
                    None => Expr::Lit { value: Value::None },
                    Some(e) => e.as_ref().clone(),
                };
                let otherwise = otherwise.compile_scoped(schema.clone(), scope)?;

                // all then expressions + otherwise must be of compatible types
                let mut then_types = cwts.iter().map(|(_, t)| t.dtype()).collect_vec();
                then_types.push(&otherwise.dtype);
                // now starting with lca of first two, find lca of all
                let mut outtype = then_types[0].clone();
                for t in then_types.iter().skip(1) {
                    outtype = match lca(&outtype, t) {
                        Some(t) => t,
                        None => bail!("incompatible types in THEN expressions: {:?}", then_types),
                    };
                }
                let rewritten = match &outtype {
                    // if outtype is null, then all branches are null, just rewrite
                    Null => Expr::Lit { value: Value::None },
                    _ => {
                        // cast all whens to eval to bool and thens to eval to outtype
                        let mut rwts = vec![];
                        for (cw, ct) in cwts {
                            rwts.push((
                                cw.safecast(&Type::optional(Bool))?,
                                ct.safecast(&outtype)?,
                            ));
                        }
                        // rewrite the case expression
                        Expr::Case {
                            when_thens: rwts,
                            otherwise: Some(Box::new(otherwise.safecast(&outtype)?)),
                        }
                    }
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::IsNull { expr } => {
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                Ok(CompiledExpr {
                    rewritten: Expr::IsNull {
                        expr: Box::new(expr.rewritten),
                    },
                    original: self.clone(),
                    schema,
                    dtype: Bool,
                })
            }
            Expr::FillNull { expr, default } => {
                let expr = expr.compile_scoped(schema.clone(), scope)?;
                let default = default.compile_scoped(schema.clone(), scope)?;
                // Validate: both sides can be promoted to a common type
                let outtype = match expr.dtype() {
                    Null => default.dtype().clone(),
                    _ => match lca(expr.dtype().inner(), default.dtype()) {
                        Some(t) => t,
                        None => bail!(
                            "invalid expression: both inputs of 'FILLNULL' must be of compatible types but found {} & {}",
                            expr.dtype(),
                            default.dtype()
                        ),
                    },
                };
                // Rewrite: use null if expr is null, casts otherwise
                let rewritten = match (expr.dtype(), &outtype) {
                    (_, Null) => Expr::Lit { value: Value::None },
                    // if expr is null, always use default
                    (Null, _) => default.rewritten.clone(),
                    _ => Expr::FillNull {
                        expr: Box::new(expr.safecast(&outtype)?),
                        default: Box::new(default.safecast(&outtype)?),
                    },
                };
                Ok(CompiledExpr {
                    rewritten,
                    original: self.clone(),
                    schema,
                    dtype: outtype,
                })
            }
            Expr::Cast { .. } => panic!("cast should not be exposed to the end user"),
        }
    }
}

#[derive(PartialEq, Clone, Serialize)]
pub enum MathFn {
    // number of decimal places
    Round { precision: u32 },
    Ceil,
    Floor,
    Abs,
    ToString,
    Pow { exponent: Box<Expr> },
    Log { base: f64 },
    Sqrt,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    IsNan,
    IsInfinite,
}

impl std::fmt::Display for MathFn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MathFn::Round { precision } => write!(f, "ROUND({})", precision),
            MathFn::Ceil => write!(f, "CEIL"),
            MathFn::Floor => write!(f, "FLOOR"),
            MathFn::Abs => write!(f, "ABS"),
            MathFn::ToString => write!(f, "TOSTRING"),
            MathFn::Pow { exponent } => write!(f, "POW({})", exponent),
            MathFn::Log { base } => write!(f, "LOG(base={})", base),
            MathFn::Sqrt => write!(f, "SQRT"),
            MathFn::Sin => write!(f, "SIN"),
            MathFn::Cos => write!(f, "COS"),
            MathFn::Tan => write!(f, "TAN"),
            MathFn::Asin => write!(f, "ASIN"),
            MathFn::Acos => write!(f, "ACOS"),
            MathFn::Atan => write!(f, "ATAN"),
            MathFn::IsNan => write!(f, "ISNAN"),
            MathFn::IsInfinite => write!(f, "ISINFINITE"),
        }
    }
}

#[derive(Copy, Clone, PartialEq, Serialize, Debug)]
pub enum UnOp {
    Neg,
    Not,
}

#[derive(Copy, Clone, PartialEq, Serialize, Debug)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    FloorDiv,
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
    And,
    Or,
}

impl std::fmt::Display for CompiledExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.original)
    }
}

impl std::fmt::Debug for CompiledExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} -> {:?}", self.original, self.rewritten)
    }
}

impl fmt::Display for Expr {
    // Display is same as Debug
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expr::Var { name } => write!(f, "var(\"{}\")", name),
            Expr::Now => write!(f, "now()"),
            Expr::Ref { name } => write!(f, "col(\"{}\")", name),
            Expr::Lit { value } => write!(f, "lit({:?})", value),
            Expr::DateTimeLiteral {
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
                timezone,
            } => {
                let timezone = timezone
                    .as_ref()
                    .map(|tz| format!(", timezone=\"{}\"", tz))
                    .unwrap_or("".to_string());
                write!(
                    f,
                    "datetime({}, {}, {}, {}, {}, {}, {}{})",
                    year, month, day, hour, minute, second, microsecond, timezone
                )
            }
            Expr::FromEpoch { expr, unit } => {
                write!(f, "from_epoch({:?}, unit=\"{:?}\")", expr, unit)
            }
            Expr::Cast { expr, dtype } => write!(f, "CAST({:?} AS {:?})", expr, dtype),
            Expr::Binary { op, left, right } => write!(f, "({:?} {} {:?})", left, op, right),
            Expr::Unary { op, expr } => write!(f, "({} {:?})", op, expr),
            Expr::Repeat { expr, count } => write!(f, "{:?}.repeat({:?})", expr, count),
            Expr::Zip {
                struct_type,
                fields,
            } => {
                let fields = fields
                    .iter()
                    .map(|(name, expr)| format!("{}={:?}", name, expr))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{}.zip({})", struct_type.name(), fields)
            }
            Expr::MakeStruct {
                struct_type,
                fields,
            } => {
                let fields = fields
                    .iter()
                    .map(|(name, expr)| format!("{}: {:?}", name, expr))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "struct({:?}, {{ {} }})", struct_type.name(), fields)
            }
            Expr::Case {
                when_thens,
                otherwise,
            } => {
                // desired: CASE WHEN ... THEN ... WHEN ... THEN ... ELSE ...END
                let mut s = String::from("CASE ");
                for (when, then) in when_thens {
                    s.push_str(&format!("WHEN {{ {:?} }} THEN {{ {:?} }} ", when, then));
                }
                let end = match otherwise {
                    None => "END".to_string(),
                    Some(e) => format!("ELSE {{ {:?} }} END", e),
                };
                s.push_str(&end);
                write!(f, "{}", s)
            }
            Expr::IsNull { expr } => write!(f, "ISNULL({:?})", expr),
            Expr::FillNull { expr, default } => write!(f, "FILLNULL({:?}, {:?})", expr, default),
            Expr::MathFn { func, expr } => match func {
                MathFn::Ceil => write!(f, "{:?}.ceil()", expr),
                MathFn::Floor => write!(f, "{:?}.floor()", expr),
                MathFn::Round { precision } => write!(f, "{:?}.round({})", expr, precision),
                MathFn::Abs => write!(f, "{:?}.abs()", expr),
                MathFn::ToString => write!(f, "{:?}.to_string()", expr),
                MathFn::Pow { exponent } => write!(f, "{:?}.pow({:?})", expr, exponent),
                MathFn::Log { base } => write!(f, "{:?}.log(base={:?})", expr, base),
                MathFn::Sqrt => write!(f, "{:?}.sqrt()", expr),
                MathFn::Sin => write!(f, "{:?}.sin()", expr),
                MathFn::Cos => write!(f, "{:?}.cos()", expr),
                MathFn::Tan => write!(f, "{:?}.tan()", expr),
                MathFn::Asin => write!(f, "{:?}.asin()", expr),
                MathFn::Acos => write!(f, "{:?}.acos()", expr),
                MathFn::Atan => write!(f, "{:?}.atan()", expr),
                MathFn::IsNan => write!(f, "{:?}.is_nan()", expr),
                MathFn::IsInfinite => write!(f, "{:?}.is_infinite()", expr),
            },
            Expr::ListFn { list, func } => match func.as_ref() {
                ListFn::Len => write!(f, "{:?}.list.len()", list),
                ListFn::Get { index } => write!(f, "{:?}.list.get({:?})", list, index),
                ListFn::Contains { item: expr } => {
                    write!(f, "{:?}.list.contains({:?})", list, expr)
                }
                ListFn::HasNull => write!(f, "{:?}.list.hasnull()", list),
                ListFn::Sum => write!(f, "{:?}.list.sum()", list),
                ListFn::Min => write!(f, "{:?}.list.min()", list),
                ListFn::Max => write!(f, "{:?}.list.max()", list),
                ListFn::All => write!(f, "{:?}.list.all()", list),
                ListFn::Any => write!(f, "{:?}.list.any()", list),
                ListFn::Mean => write!(f, "{:?}.list.mean()", list),
                ListFn::Filter { var, predicate } => {
                    write!(f, "{:?}.list.filter({:?}, {:?})", list, var, predicate)
                }
                ListFn::Map { var, func } => {
                    write!(f, "{:?}.list.map({:?}, {:?})", list, var, func)
                }
            },
            Expr::DictFn { dict, func } => match func.as_ref() {
                DictFn::Len => write!(f, "{:?}.dict.len()", dict),
                DictFn::Get { key, default } => {
                    write!(f, "{:?}.dict.get({:?}, {:?})", dict, key, default)
                }
                DictFn::Contains { key } => write!(f, "{:?}.dict.contains({:?})", dict, key),
            },
            Expr::StructFn { struct_, func } => match func.as_ref() {
                StructFn::Get { field } => write!(f, "{:?}.{}", struct_, field),
            },
            Expr::StringFn { func, expr } => match func.as_ref() {
                StringFn::Len => write!(f, "{:?}.str.len()", expr),
                StringFn::ToInt => write!(f, "{:?}.str.to_int()", expr),
                StringFn::ToLower => write!(f, "{:?}.str.lower()", expr),
                StringFn::ToUpper => write!(f, "{:?}.str.upper()", expr),
                StringFn::Contains { key } => write!(f, "{:?}.str.contains({:?})", expr, key),
                StringFn::StartsWith { key } => write!(f, "{:?}.str.starts_with({:?})", expr, key),
                StringFn::EndsWith { key } => write!(f, "{:?}.str.ends_with({:?})", expr, key),
                StringFn::Concat { other } => write!(f, "{:?}.str.concat({:?})", expr, other),
                StringFn::Strptime { format, timezone } => match timezone {
                    None => write!(f, "{:?}.str.parse_datetime({:?})", expr, format),
                    Some(tz) => write!(
                        f,
                        "{:?}.str.parse_datetime({:?}, timezone={:?})",
                        expr, format, tz
                    ),
                },
                StringFn::JsonDecode { dtype } => {
                    write!(f, "{:?}.str.json_decode({:?})", expr, dtype)
                }
                StringFn::Split { sep } => write!(f, "{:?}.str.split({:?})", expr, sep),
                StringFn::JsonExtract { path } => {
                    write!(f, "{:?}.str.json_extract({:?})", expr, path)
                }
            },
            Expr::DatetimeFn { func, expr } => match func.as_ref() {
                DateTimeFn::SinceEpoch { unit } => {
                    write!(f, "{:?}.dt.since_epoch(unit=\"{:?}\")", expr, unit)
                }
                DateTimeFn::Since { other, unit } => {
                    write!(f, "{:?}.dt.since({:?}, unit=\"{:?}\")", expr, other, unit)
                }
                DateTimeFn::Part { unit, timezone } => {
                    let tzstr = match timezone {
                        None => "".to_string(),
                        Some(tz) => format!("timezone=\"{}\"", tz),
                    };
                    match unit {
                        TimeUnit::Year => write!(f, "{:?}.dt.year({})", expr, tzstr),
                        TimeUnit::Month => write!(f, "{:?}.dt.month({})", expr, tzstr),
                        TimeUnit::Week => write!(f, "{:?}.dt.week({})", expr, tzstr),
                        TimeUnit::Day => write!(f, "{:?}.dt.day({})", expr, tzstr),
                        TimeUnit::Hour => write!(f, "{:?}.dt.hour({})", expr, tzstr),
                        TimeUnit::Minute => write!(f, "{:?}.dt.minute({})", expr, tzstr),
                        TimeUnit::Second => write!(f, "{:?}.dt.second({})", expr, tzstr),
                        TimeUnit::Millisecond => write!(f, "{:?}.dt.millisecond({})", expr, tzstr),
                        TimeUnit::Microsecond => write!(f, "{:?}.dt.microsecond({})", expr, tzstr),
                    }
                }
                DateTimeFn::Stftime { format, timezone } => {
                    let tzstr = match timezone {
                        None => "".to_string(),
                        Some(tz) => format!(", timezone=\"{}\"", tz),
                    };
                    write!(f, "{:?}.dt.strftime({:?}{})", expr, format, tzstr)
                }
            },
        }
    }
}

impl fmt::Display for BinOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinOp::Add => write!(f, "+"),
            BinOp::Sub => write!(f, "-"),
            BinOp::Mul => write!(f, "*"),
            BinOp::Div => write!(f, "/"),
            BinOp::Mod => write!(f, "%"),
            BinOp::Eq => write!(f, "=="),
            BinOp::Neq => write!(f, "!="),
            BinOp::Gt => write!(f, ">"),
            BinOp::Gte => write!(f, ">="),
            BinOp::Lt => write!(f, "<"),
            BinOp::Lte => write!(f, "<="),
            BinOp::And => write!(f, "and"),
            BinOp::Or => write!(f, "or"),
            BinOp::FloorDiv => write!(f, "//"),
        }
    }
}

impl fmt::Display for UnOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnOp::Neg => write!(f, "-"),
            UnOp::Not => write!(f, "!"),
        }
    }
}

fn none_as(dtype: &Type) -> Expr {
    Expr::Cast {
        expr: Box::new(Expr::Lit { value: Value::None }),
        dtype: dtype.clone(),
    }
}

/// Given a value, return the simplest natural type that can hold it
/// returning None for Value::None
fn natural_type(value: &Value) -> Type {
    match value {
        Value::None => Type::Null,
        Value::Int(_) => Type::Int,
        Value::Float(_) => Type::Float,
        Value::Bool(_) => Type::Bool,
        Value::String(_) => Type::String,
        Value::Bytes(_) => Type::Bytes,
        Value::Timestamp(_) => Type::Timestamp,
        Value::Embedding(e) => Type::Embedding(e.len()),
        Value::Struct(s) => {
            let fields = s
                .fields()
                .map(|(n, v)| Field::new(n.clone(), natural_type(v)))
                .collect();
            Type::Struct(Box::new(StructType::new("anon".into(), fields).unwrap()))
        }
        Value::Decimal(v) => Type::Decimal(DecimalType::new(v.scale()).unwrap()),
        Value::Date(_) => Type::Date,
        Value::List(l) => Type::List(Box::new(l.dtype().clone())),
        Value::Map(m) => Type::Map(Box::new(m.dtype().clone())),
    }
}
pub mod builders {
    use super::*;

    pub fn lit(x: impl Into<Value>) -> Expr {
        Expr::Lit { value: x.into() }
    }

    pub fn dt_lit(
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        microsecond: u32,
    ) -> Expr {
        Expr::DateTimeLiteral {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            timezone: None,
        }
    }
    pub fn dt_lit_tz(
        year: u32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
        microsecond: u32,
        timezone: &str,
    ) -> Expr {
        Expr::DateTimeLiteral {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            timezone: Some(timezone.to_string()),
        }
    }

    pub fn struct_(struct_type: StructType, fields: &[(&str, Expr)]) -> Expr {
        let fields = fields
            .iter()
            .map(|(name, expr)| (name.to_string(), Box::new(expr.clone())))
            .collect_vec();
        Expr::MakeStruct {
            struct_type,
            fields,
        }
    }

    pub fn zip(struct_type: &StructType, fields: &[(&str, Expr)]) -> Expr {
        Expr::Zip {
            struct_type: struct_type.clone(),
            fields: fields
                .iter()
                .map(|(name, expr)| (name.to_string(), Box::new(expr.clone())))
                .collect_vec(),
        }
    }

    pub fn from_epoch(expr: Expr, unit: &str) -> Expr {
        Expr::FromEpoch {
            expr: Box::new(expr),
            unit: TimeUnit::try_from(unit).unwrap(),
        }
    }

    pub fn floor(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Floor,
            expr: Box::new(e),
        }
    }

    pub fn to_string(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::ToString,
            expr: Box::new(e),
        }
    }
    pub fn sin(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Sin,
            expr: Box::new(e),
        }
    }

    pub fn cos(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Cos,
            expr: Box::new(e),
        }
    }

    pub fn tan(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Tan,
            expr: Box::new(e),
        }
    }

    pub fn asin(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Asin,
            expr: Box::new(e),
        }
    }

    pub fn acos(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Acos,
            expr: Box::new(e),
        }
    }

    pub fn atan(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Atan,
            expr: Box::new(e),
        }
    }

    pub fn ceil(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Ceil,
            expr: Box::new(e),
        }
    }

    pub fn roundto(e: Expr, precision: u32) -> Expr {
        Expr::MathFn {
            func: MathFn::Round { precision },
            expr: Box::new(e),
        }
    }

    pub fn round(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Round { precision: 0 },
            expr: Box::new(e),
        }
    }

    pub fn abs(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::Abs,
            expr: Box::new(e),
        }
    }

    pub fn col(name: &str) -> Expr {
        Expr::Ref {
            name: name.to_string(),
        }
    }

    pub fn now() -> Expr {
        Expr::Now
    }

    pub fn var(name: &str) -> Expr {
        Expr::Var {
            name: name.to_string(),
        }
    }

    pub fn isnull(expr: Expr) -> Expr {
        Expr::IsNull {
            expr: Box::new(expr),
        }
    }

    pub fn fillnull(expr: Expr, default: Expr) -> Expr {
        Expr::FillNull {
            expr: Box::new(expr),
            default: Box::new(default),
        }
    }

    pub fn binary(left: impl Into<Box<Expr>>, op: BinOp, right: impl Into<Box<Expr>>) -> Expr {
        Expr::Binary {
            op,
            left: left.into(),
            right: right.into(),
        }
    }

    pub fn cast(expr: Expr, dtype: Type) -> Expr {
        Expr::Cast {
            expr: Box::new(expr),
            dtype,
        }
    }

    pub fn unary(op: UnOp, expr: Expr) -> Expr {
        Expr::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    pub fn is_nan(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::IsNan,
            expr: Box::new(e),
        }
    }

    pub fn is_infinite(e: Expr) -> Expr {
        Expr::MathFn {
            func: MathFn::IsInfinite,
            expr: Box::new(e),
        }
    }

    pub fn not(expr: Expr) -> Expr {
        unary(UnOp::Not, expr)
    }

    pub fn neg(expr: Expr) -> Expr {
        unary(UnOp::Neg, expr)
    }

    impl TryFrom<&str> for TimeUnit {
        type Error = anyhow::Error;
        fn try_from(value: &str) -> Result<Self, Self::Error> {
            match value {
                "year" => Ok(TimeUnit::Year),
                "month" => Ok(TimeUnit::Month),
                "week" => Ok(TimeUnit::Week),
                "day" => Ok(TimeUnit::Day),
                "hour" => Ok(TimeUnit::Hour),
                "minute" => Ok(TimeUnit::Minute),
                "second" => Ok(TimeUnit::Second),
                "millis" => Ok(TimeUnit::Millisecond),
                "micros" => Ok(TimeUnit::Microsecond),
                _ => bail!("invalid time unit: {}", value),
            }
        }
    }

    impl Expr {
        pub fn dt_part(self, unit: &str) -> Expr {
            Expr::DatetimeFn {
                func: Box::new(DateTimeFn::Part {
                    unit: TimeUnit::try_from(unit).unwrap(),
                    timezone: None,
                }),
                expr: Box::new(self),
            }
        }
        pub fn dt_part_tz(self, unit: &str, timezone: &str) -> Expr {
            Expr::DatetimeFn {
                func: Box::new(DateTimeFn::Part {
                    unit: TimeUnit::try_from(unit).unwrap(),
                    timezone: Some(timezone.to_string()),
                }),
                expr: Box::new(self),
            }
        }
        pub fn repeat(self, count: Expr) -> Expr {
            Expr::Repeat {
                expr: Box::new(self),
                count: Box::new(count),
            }
        }
        pub fn pow(self, exponent: Expr) -> Expr {
            Expr::MathFn {
                func: MathFn::Pow {
                    exponent: Box::new(exponent),
                },
                expr: Box::new(self),
            }
        }
        pub fn sqrt(self) -> Expr {
            Expr::MathFn {
                func: MathFn::Sqrt,
                expr: Box::new(self),
            }
        }
        pub fn log(self, base: f64) -> Expr {
            Expr::MathFn {
                func: MathFn::Log { base },
                expr: Box::new(self),
            }
        }

        pub fn since_epoch(self, unit: &str) -> super::Expr {
            let unit = super::TimeUnit::try_from(unit).unwrap();
            super::Expr::DatetimeFn {
                func: Box::new(super::DateTimeFn::SinceEpoch { unit }),
                expr: Box::new(self),
            }
        }
        pub fn since(self, other: super::Expr, unit: &str) -> super::Expr {
            let unit = super::TimeUnit::try_from(unit).unwrap();
            super::Expr::DatetimeFn {
                func: Box::new(super::DateTimeFn::Since {
                    other: Box::new(other),
                    unit,
                }),
                expr: Box::new(self),
            }
        }
        pub fn part(self, unit: &str) -> super::Expr {
            let unit = super::TimeUnit::try_from(unit).unwrap();
            super::Expr::DatetimeFn {
                func: Box::new(super::DateTimeFn::Part {
                    unit,
                    timezone: None,
                }),
                expr: Box::new(self),
            }
        }

        pub fn part_tz(self, unit: &str, timezone: &str) -> super::Expr {
            let unit = super::TimeUnit::try_from(unit).unwrap();
            super::Expr::DatetimeFn {
                func: Box::new(super::DateTimeFn::Part {
                    unit,
                    timezone: Some(timezone.to_string()),
                }),
                expr: Box::new(self),
            }
        }

        pub fn strftime(self, format: &str) -> super::Expr {
            super::Expr::DatetimeFn {
                func: Box::new(super::DateTimeFn::Stftime {
                    format: format.to_string(),
                    timezone: None,
                }),
                expr: Box::new(self),
            }
        }
        pub fn split(self, by: &str) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::Split {
                    sep: by.to_string(),
                }),
            }
        }
        pub fn to_int(self) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::ToInt),
            }
        }

        pub fn json_extract(self, path: &str) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::JsonExtract {
                    path: path.to_string(),
                }),
            }
        }

        pub fn strftime_tz(self, format: &str, timezone: &str) -> Expr {
            Expr::DatetimeFn {
                func: Box::new(DateTimeFn::Stftime {
                    format: format.to_string(),
                    timezone: Some(timezone.to_string()),
                }),
                expr: Box::new(self),
            }
        }

        pub fn dot(self, field: &str) -> Expr {
            Expr::StructFn {
                struct_: Box::new(self),
                func: Box::new(StructFn::Get {
                    field: field.to_string(),
                }),
            }
        }
        pub fn str_len(self) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::Len),
            }
        }
        pub fn str_to_upper(self) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::ToUpper),
            }
        }
        pub fn str_to_lower(self) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::ToLower),
            }
        }
        pub fn str_contains(self, key: Expr) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::Contains { key }),
            }
        }
        pub fn str_json_decode(self, dtype: impl AsRef<Type>) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::JsonDecode {
                    dtype: dtype.as_ref().clone(),
                }),
            }
        }

        pub fn str_starts_with(self, key: Expr) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::StartsWith { key }),
            }
        }
        pub fn str_ends_with(self, key: Expr) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::EndsWith { key }),
            }
        }
        pub fn str_concat(self, other: Expr) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::Concat { other }),
            }
        }
        pub fn strptime(self, format: &str) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::Strptime {
                    format: format.to_string(),
                    timezone: None,
                }),
            }
        }

        pub fn strptime_tz(self, format: &str, timezone: &str) -> Expr {
            Expr::StringFn {
                expr: Box::new(self),
                func: Box::new(StringFn::Strptime {
                    format: format.to_string(),
                    timezone: Some(timezone.to_string()),
                }),
            }
        }

        pub fn modulo(self, other: Expr) -> Expr {
            binary(self, BinOp::Mod, other)
        }

        pub fn list_at(self, index: Expr) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Get { index }),
            }
        }
        pub fn list_len(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Len),
            }
        }
        pub fn dict_get(self, key: Expr, default: Option<Expr>) -> Expr {
            Expr::DictFn {
                dict: Box::new(self),
                func: Box::new(DictFn::Get { key, default }),
            }
        }
        pub fn dict_len(self) -> Expr {
            Expr::DictFn {
                dict: Box::new(self),
                func: Box::new(DictFn::Len),
            }
        }
        pub fn list_has(self, item: Expr) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Contains { item }),
            }
        }
        pub fn list_min(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Min),
            }
        }
        pub fn list_any(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Any),
            }
        }

        pub fn list_all(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::All),
            }
        }

        pub fn list_max(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Max),
            }
        }
        pub fn list_mean(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Mean),
            }
        }
        pub fn list_sum(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Sum),
            }
        }
        pub fn list_map(self, var: &str, func: Expr) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Map {
                    var: var.to_string(),
                    func,
                }),
            }
        }
        pub fn list_filter(self, var: &str, predicate: Expr) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::Filter {
                    var: var.to_string(),
                    predicate,
                }),
            }
        }

        pub fn list_has_null(self) -> Expr {
            Expr::ListFn {
                list: Box::new(self),
                func: Box::new(ListFn::HasNull),
            }
        }
        pub fn floordiv(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::FloorDiv, Box::new(other))
        }

        pub fn and(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::And, Box::new(other))
        }
        pub fn or(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Or, Box::new(other))
        }
        pub fn sub(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Sub, Box::new(other))
        }
        pub fn add(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Add, Box::new(other))
        }
        pub fn mul(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Mul, Box::new(other))
        }
        pub fn div(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Div, Box::new(other))
        }
        pub fn eq(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Eq, Box::new(other))
        }
        pub fn neq(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Neq, Box::new(other))
        }
        pub fn gt(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Gt, Box::new(other))
        }
        pub fn gte(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Gte, Box::new(other))
        }
        pub fn lt(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Lt, Box::new(other))
        }
        pub fn lte(self, other: Expr) -> Expr {
            binary(Box::new(self), BinOp::Lte, Box::new(other))
        }
        pub fn when(self, expr: Expr, then: Expr) -> Expr {
            match self {
                Expr::Case {
                    when_thens,
                    otherwise,
                } => {
                    let mut when_thens = when_thens;
                    when_thens.push((expr, then));
                    Expr::Case {
                        when_thens,
                        otherwise,
                    }
                }
                _ => panic!("expected CASE expression"),
            }
        }
        pub fn otherwise(self, expr: Expr) -> Expr {
            match self {
                Expr::Case {
                    when_thens,
                    otherwise: None,
                } => Expr::Case {
                    when_thens,
                    otherwise: Some(Box::new(expr)),
                },
                _ => panic!("expected CASE expression"),
            }
        }
    }
    pub fn when(expr: Expr, then: Expr) -> Expr {
        Expr::Case {
            when_thens: vec![(expr, then)],
            otherwise: None,
        }
    }
    impl std::ops::Mul<Expr> for Expr {
        type Output = Expr;

        fn mul(self, rhs: Expr) -> Expr {
            self.mul(rhs)
        }
    }

    impl std::ops::Div<Expr> for Expr {
        type Output = Expr;

        fn div(self, rhs: Expr) -> Expr {
            self.div(rhs)
        }
    }

    impl std::ops::Add<Expr> for Expr {
        type Output = Expr;

        fn add(self, rhs: Expr) -> Expr {
            self.add(rhs)
        }
    }
    impl std::ops::Sub<Expr> for Expr {
        type Output = Expr;

        fn sub(self, rhs: Expr) -> Expr {
            self.sub(rhs)
        }
    }
    impl std::ops::Rem<Expr> for Expr {
        type Output = Expr;

        fn rem(self, rhs: Expr) -> Expr {
            binary(self, BinOp::Mod, rhs)
        }
    }
}

/// Proto conversion code

impl From<TimeUnit> for eproto::TimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::Year => eproto::TimeUnit::Year,
            TimeUnit::Month => eproto::TimeUnit::Month,
            TimeUnit::Week => eproto::TimeUnit::Week,
            TimeUnit::Day => eproto::TimeUnit::Day,
            TimeUnit::Hour => eproto::TimeUnit::Hour,
            TimeUnit::Minute => eproto::TimeUnit::Minute,
            TimeUnit::Second => eproto::TimeUnit::Second,
            TimeUnit::Millisecond => eproto::TimeUnit::Millisecond,
            TimeUnit::Microsecond => eproto::TimeUnit::Microsecond,
        }
    }
}

impl TryFrom<ProtoTimeUnit> for TimeUnit {
    type Error = anyhow::Error;
    fn try_from(value: ProtoTimeUnit) -> Result<Self> {
        match value {
            ProtoTimeUnit::Year => Ok(TimeUnit::Year),
            ProtoTimeUnit::Month => Ok(TimeUnit::Month),
            ProtoTimeUnit::Week => Ok(TimeUnit::Week),
            ProtoTimeUnit::Day => Ok(TimeUnit::Day),
            ProtoTimeUnit::Hour => Ok(TimeUnit::Hour),
            ProtoTimeUnit::Minute => Ok(TimeUnit::Minute),
            ProtoTimeUnit::Second => Ok(TimeUnit::Second),
            ProtoTimeUnit::Millisecond => Ok(TimeUnit::Millisecond),
            ProtoTimeUnit::Microsecond => Ok(TimeUnit::Microsecond),
            ProtoTimeUnit::Unknown => bail!("invalid time unit"),
        }
    }
}

impl TryFrom<&ProtoExpr> for Expr {
    type Error = anyhow::Error;

    fn try_from(value: &ProtoExpr) -> std::result::Result<Self, Self::Error> {
        value.clone().try_into()
    }
}

impl TryFrom<ProtoExpr> for Expr {
    type Error = anyhow::Error;

    fn try_from(value: ProtoExpr) -> Result<Self> {
        let expr = match value.node {
            Some(eproto::expr::Node::Ref(r)) => Expr::Ref { name: r.name },
            Some(eproto::expr::Node::Var(v)) => Expr::Var { name: v.name },
            Some(eproto::expr::Node::Now(_)) => Expr::Now,
            Some(eproto::expr::Node::JsonLiteral(l)) => {
                let dtype = l
                    .dtype
                    .ok_or(anyhow!("missing json literal dtype"))?
                    .try_into()?;
                Expr::Lit {
                    value: Value::from_json(&dtype, &l.literal)?,
                }
            }
            Some(eproto::expr::Node::DatetimeLiteral(d)) => {
                let timezone = d.timezone.map(|tz| tz.timezone);
                Expr::DateTimeLiteral {
                    year: d.year as u32,
                    month: d.month as u32,
                    day: d.day as u32,
                    hour: d.hour as u32,
                    minute: d.minute as u32,
                    second: d.second as u32,
                    microsecond: d.microsecond as u32,
                    timezone,
                }
            }
            Some(eproto::expr::Node::FromEpoch(f)) => Expr::FromEpoch {
                expr: Box::new(
                    f.duration
                        .ok_or(anyhow!("missing fromepoch expr"))?
                        .as_ref()
                        .try_into()?,
                ),
                unit: ProtoTimeUnit::try_from(f.unit)?.try_into()?,
            },
            Some(eproto::expr::Node::Repeat(r)) => Expr::Repeat {
                expr: Box::new(
                    r.expr
                        .ok_or(anyhow!("missing repeat expr"))?
                        .as_ref()
                        .try_into()?,
                ),
                count: Box::new(
                    r.count
                        .ok_or(anyhow!("missing repeat count"))?
                        .as_ref()
                        .try_into()?,
                ),
            },
            Some(eproto::expr::Node::Zip(z)) => Expr::Zip {
                struct_type: z
                    .struct_type
                    .ok_or(anyhow!("missing zip struct type"))?
                    .try_into()?,
                fields: z
                    .fields
                    .into_iter()
                    .sorted_by_key(|(name, _)| name.clone())
                    .map(|(name, expr)| Ok((name, Box::new(expr.try_into()?))))
                    .collect::<Result<Vec<_>>>()?,
            },
            Some(eproto::expr::Node::MakeStruct(s)) => Expr::MakeStruct {
                struct_type: s
                    .struct_type
                    .ok_or(anyhow!("missing struct type"))?
                    .try_into()?,
                fields: s
                    .fields
                    .into_iter()
                    .sorted_by_key(|(name, _)| name.clone())
                    .map(|(name, expr)| Ok((name, Box::new(expr.try_into()?))))
                    .collect::<Result<Vec<_>>>()?,
            },
            Some(eproto::expr::Node::Binary(b)) => Expr::Binary {
                op: b.op.try_into()?,
                left: Box::new(
                    b.left
                        .ok_or(anyhow!("missing binary left"))?
                        .as_ref()
                        .try_into()?,
                ),
                right: Box::new(
                    b.right
                        .ok_or(anyhow!("missing binary right"))?
                        .as_ref()
                        .try_into()?,
                ),
            },
            Some(eproto::expr::Node::Unary(u)) => Expr::Unary {
                op: u.op.try_into()?,
                expr: Box::new(
                    u.operand
                        .ok_or(anyhow!("missing unary operand"))?
                        .as_ref()
                        .try_into()?,
                ),
            },
            Some(eproto::expr::Node::MathFn(m)) => {
                if m.r#fn.is_none() {
                    bail!("missing mathfn func");
                }
                Expr::MathFn {
                    func: match m.r#fn.unwrap().fn_type {
                        Some(eproto::math_op::FnType::Abs(_)) => MathFn::Abs,
                        Some(eproto::math_op::FnType::Ceil(_)) => MathFn::Ceil,
                        Some(eproto::math_op::FnType::Floor(_)) => MathFn::Floor,
                        Some(eproto::math_op::FnType::Round(r)) => MathFn::Round {
                            precision: r.precision as u32,
                        },
                        Some(eproto::math_op::FnType::ToString(_)) => MathFn::ToString,
                        Some(eproto::math_op::FnType::Pow(p)) => MathFn::Pow {
                            exponent: Box::new(
                                p.exponent
                                    .ok_or(anyhow!("missing pow exponent"))?
                                    .as_ref()
                                    .try_into()?,
                            ),
                        },
                        Some(eproto::math_op::FnType::Log(l)) => MathFn::Log { base: l.base },
                        Some(eproto::math_op::FnType::Sqrt(_)) => MathFn::Sqrt,
                        Some(eproto::math_op::FnType::Sin(_)) => MathFn::Sin,
                        Some(eproto::math_op::FnType::Cos(_)) => MathFn::Cos,
                        Some(eproto::math_op::FnType::Tan(_)) => MathFn::Tan,
                        Some(eproto::math_op::FnType::Asin(_)) => MathFn::Asin,
                        Some(eproto::math_op::FnType::Acos(_)) => MathFn::Acos,
                        Some(eproto::math_op::FnType::Atan(_)) => MathFn::Atan,
                        Some(eproto::math_op::FnType::IsNan(_)) => MathFn::IsNan,
                        Some(eproto::math_op::FnType::IsInfinite(_)) => MathFn::IsInfinite,
                        None => bail!("missing mathfn func"),
                    },
                    expr: Box::new(
                        m.operand
                            .ok_or(anyhow!("missing mathfn operand"))?
                            .as_ref()
                            .try_into()?,
                    ),
                }
            }
            Some(eproto::expr::Node::DatetimeFn(d)) => {
                if d.r#fn.is_none() {
                    bail!("missing datetimefn func");
                }
                Expr::DatetimeFn {
                    func: Box::new(match d.r#fn.unwrap().fn_type {
                        Some(eproto::date_time_op::FnType::SinceEpoch(s)) => {
                            DateTimeFn::SinceEpoch {
                                unit: ProtoTimeUnit::try_from(s.unit)?.try_into()?,
                            }
                        }
                        Some(eproto::date_time_op::FnType::Since(s)) => DateTimeFn::Since {
                            other: Box::new(
                                s.other
                                    .ok_or(anyhow!("missing since other"))?
                                    .as_ref()
                                    .try_into()?,
                            ),
                            unit: ProtoTimeUnit::try_from(s.unit)?.try_into()?,
                        },
                        Some(eproto::date_time_op::FnType::Part(p)) => DateTimeFn::Part {
                            unit: ProtoTimeUnit::try_from(p.unit)?.try_into()?,
                            timezone: p.timezone.map(|tz| tz.timezone),
                        },
                        Some(eproto::date_time_op::FnType::Strftime(s)) => DateTimeFn::Stftime {
                            format: s.format,
                            timezone: s.timezone.map(|tz| tz.timezone),
                        },
                        None => bail!("missing datetimefn func"),
                    }),
                    expr: Box::new(
                        d.datetime
                            .ok_or(anyhow!("missing datetimefn operand"))?
                            .as_ref()
                            .try_into()?,
                    ),
                }
            }
            Some(eproto::expr::Node::StringFn(s)) => {
                if s.r#fn.is_none() {
                    bail!("missing stringfn func");
                }
                Expr::StringFn {
                    func: match s.r#fn.unwrap().fn_type {
                        Some(eproto::string_op::FnType::Len(_)) => Box::new(StringFn::Len),
                        Some(eproto::string_op::FnType::Tolower(_)) => Box::new(StringFn::ToLower),
                        Some(eproto::string_op::FnType::Toupper(_)) => Box::new(StringFn::ToUpper),
                        Some(eproto::string_op::FnType::ToInt(_)) => Box::new(StringFn::ToInt),
                        Some(eproto::string_op::FnType::Contains(c)) => {
                            Box::new(StringFn::Contains {
                                key: c
                                    .element
                                    .ok_or(anyhow!("missing contains key"))?
                                    .as_ref()
                                    .try_into()?,
                            })
                        }
                        Some(eproto::string_op::FnType::Startswith(c)) => {
                            Box::new(StringFn::StartsWith {
                                key: c
                                    .key
                                    .ok_or(anyhow!("missing startswith key"))?
                                    .as_ref()
                                    .try_into()?,
                            })
                        }
                        Some(eproto::string_op::FnType::Endswith(c)) => {
                            Box::new(StringFn::EndsWith {
                                key: c
                                    .key
                                    .ok_or(anyhow!("missing endswith key"))?
                                    .as_ref()
                                    .try_into()?,
                            })
                        }
                        Some(eproto::string_op::FnType::Concat(c)) => Box::new(StringFn::Concat {
                            other: c
                                .other
                                .ok_or(anyhow!("missing concat other"))?
                                .as_ref()
                                .try_into()?,
                        }),
                        Some(eproto::string_op::FnType::Strptime(s)) => match s.timezone {
                            Some(tz) => Box::new(StringFn::Strptime {
                                format: s.format,
                                timezone: Some(tz.timezone),
                            }),
                            None => Box::new(StringFn::Strptime {
                                format: s.format,
                                timezone: None,
                            }),
                        },
                        Some(eproto::string_op::FnType::JsonDecode(j)) => {
                            Box::new(StringFn::JsonDecode {
                                dtype: j
                                    .dtype
                                    .ok_or(anyhow!("missing jsondecode dtype"))?
                                    .try_into()?,
                            })
                        }
                        Some(eproto::string_op::FnType::Split(s)) => {
                            Box::new(StringFn::Split { sep: s.sep })
                        }
                        Some(eproto::string_op::FnType::JsonExtract(j)) => {
                            Box::new(StringFn::JsonExtract { path: j.path })
                        }
                        None => bail!("missing stringfn func"),
                    },
                    expr: Box::new(
                        s.string
                            .ok_or(anyhow!("missing stringfn expr"))?
                            .as_ref()
                            .try_into()?,
                    ),
                }
            }
            Some(eproto::expr::Node::DictFn(d)) => {
                if d.r#fn.is_none() {
                    bail!("missing dictfn func");
                }

                Expr::DictFn {
                    dict: Box::new(
                        d.dict
                            .ok_or(anyhow!("missing dictfn dict"))?
                            .as_ref()
                            .try_into()?,
                    ),
                    func: match d.r#fn.unwrap().fn_type {
                        Some(eproto::dict_op::FnType::Len(_)) => Box::new(DictFn::Len),
                        Some(eproto::dict_op::FnType::Get(g)) => {
                            if g.field.is_none() {
                                bail!("missing dictfn get field in dictfn");
                            }

                            Box::new(DictFn::Get {
                                key: g.field.unwrap().as_ref().try_into()?,
                                default: g
                                    .default_value
                                    .map(|x| x.as_ref().try_into())
                                    .transpose()?,
                            })
                        }
                        Some(eproto::dict_op::FnType::Contains(c)) => Box::new(DictFn::Contains {
                            key: c
                                .element
                                .ok_or(anyhow!("missing dictfn contains key"))?
                                .as_ref()
                                .try_into()?,
                        }),
                        None => bail!("missing dictfn func"),
                    },
                }
            }
            Some(eproto::expr::Node::StructFn(s)) => {
                if s.r#fn.is_none() {
                    bail!("missing structfn func");
                }

                Expr::StructFn {
                    struct_: Box::new(
                        s.r#struct
                            .ok_or(anyhow!("missing structfn struct"))?
                            .as_ref()
                            .try_into()?,
                    ),
                    func: match s.r#fn.unwrap().fn_type {
                        Some(eproto::struct_op::FnType::Field(field)) => {
                            Box::new(StructFn::Get { field })
                        }
                        None => bail!("missing structfn func"),
                    },
                }
            }
            Some(eproto::expr::Node::ListFn(l)) => {
                if l.r#fn.is_none() {
                    bail!("missing listfn func");
                }

                Expr::ListFn {
                    list: Box::new(
                        l.list
                            .ok_or(anyhow!("missing listfn list"))?
                            .as_ref()
                            .try_into()?,
                    ),
                    func: match l.r#fn.unwrap().fn_type {
                        Some(eproto::list_op::FnType::Len(_)) => Box::new(ListFn::Len),
                        Some(eproto::list_op::FnType::Get(index)) => Box::new(ListFn::Get {
                            index: index.as_ref().try_into()?,
                        }),
                        Some(eproto::list_op::FnType::Contains(c)) => Box::new(ListFn::Contains {
                            item: c
                                .element
                                .ok_or(anyhow!("missing listfn contains item"))?
                                .as_ref()
                                .try_into()?,
                        }),
                        Some(eproto::list_op::FnType::HasNull(_)) => Box::new(ListFn::HasNull),
                        Some(eproto::list_op::FnType::Sum(_)) => Box::new(ListFn::Sum),
                        Some(eproto::list_op::FnType::Mean(_)) => Box::new(ListFn::Mean),
                        Some(eproto::list_op::FnType::Min(_)) => Box::new(ListFn::Min),
                        Some(eproto::list_op::FnType::Max(_)) => Box::new(ListFn::Max),
                        Some(eproto::list_op::FnType::All(_)) => Box::new(ListFn::All),
                        Some(eproto::list_op::FnType::Any(_)) => Box::new(ListFn::Any),
                        Some(eproto::list_op::FnType::Filter(f)) => Box::new(ListFn::Filter {
                            var: f.var,
                            predicate: f
                                .predicate
                                .ok_or(anyhow!("missing listfn filter predicate"))?
                                .as_ref()
                                .try_into()?,
                        }),
                        Some(eproto::list_op::FnType::Map(m)) => Box::new(ListFn::Map {
                            var: m.var,
                            func: m
                                .map_expr
                                .ok_or(anyhow!("missing listfn map expr"))?
                                .as_ref()
                                .try_into()?,
                        }),
                        None => bail!("missing listfn func"),
                    },
                }
            }
            Some(eproto::expr::Node::Case(c)) => {
                let mut when_thens = vec![];
                for w in c.when_then {
                    when_thens.push((
                        w.when.ok_or(anyhow!("missing case when"))?.try_into()?,
                        w.then.ok_or(anyhow!("missing case then"))?.try_into()?,
                    ));
                }
                Expr::Case {
                    when_thens,
                    otherwise: match c.otherwise {
                        None => None,
                        Some(o) => Some(Box::new(o.as_ref().try_into()?)),
                    },
                }
            }
            Some(eproto::expr::Node::Isnull(i)) => Expr::IsNull {
                expr: Box::new(
                    i.operand
                        .ok_or(anyhow!("missing isnull expr"))?
                        .as_ref()
                        .try_into()?,
                ),
            },
            Some(eproto::expr::Node::Fillnull(f)) => Expr::FillNull {
                expr: Box::new(
                    f.operand
                        .ok_or(anyhow!("missing fillnull expr"))?
                        .as_ref()
                        .try_into()?,
                ),
                default: Box::new(
                    f.fill
                        .ok_or(anyhow!("missing fillnull default"))?
                        .as_ref()
                        .try_into()?,
                ),
            },
            None => bail!("missing expr node"),
        };
        Ok(expr)
    }
}

impl TryFrom<i32> for BinOp {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            x if x == crate::schema_proto::expression::BinOp::Add as i32 => Ok(BinOp::Add),
            x if x == crate::schema_proto::expression::BinOp::Sub as i32 => Ok(BinOp::Sub),
            x if x == crate::schema_proto::expression::BinOp::Mul as i32 => Ok(BinOp::Mul),
            x if x == crate::schema_proto::expression::BinOp::Div as i32 => Ok(BinOp::Div),
            x if x == crate::schema_proto::expression::BinOp::Mod as i32 => Ok(BinOp::Mod),
            x if x == crate::schema_proto::expression::BinOp::FloorDiv as i32 => {
                Ok(BinOp::FloorDiv)
            }
            x if x == crate::schema_proto::expression::BinOp::Eq as i32 => Ok(BinOp::Eq),
            x if x == crate::schema_proto::expression::BinOp::Ne as i32 => Ok(BinOp::Neq),
            x if x == crate::schema_proto::expression::BinOp::Gt as i32 => Ok(BinOp::Gt),
            x if x == crate::schema_proto::expression::BinOp::Gte as i32 => Ok(BinOp::Gte),
            x if x == crate::schema_proto::expression::BinOp::Lt as i32 => Ok(BinOp::Lt),
            x if x == crate::schema_proto::expression::BinOp::Lte as i32 => Ok(BinOp::Lte),
            x if x == crate::schema_proto::expression::BinOp::And as i32 => Ok(BinOp::And),
            x if x == crate::schema_proto::expression::BinOp::Or as i32 => Ok(BinOp::Or),
            _ => bail!("invalid binop"),
        }
    }
}

impl TryFrom<i32> for UnOp {
    type Error = anyhow::Error;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            x if x == crate::schema_proto::expression::UnaryOp::Neg as i32 => Ok(UnOp::Neg),
            x if x == crate::schema_proto::expression::UnaryOp::Not as i32 => Ok(UnOp::Not),
            _ => bail!("invalid unop"),
        }
    }
}

impl From<Expr> for ProtoExpr {
    fn from(expr: Expr) -> Self {
        let node = match expr {
            Expr::Var { name } => Some(eproto::expr::Node::Var(eproto::Var { name })),
            Expr::Now => Some(eproto::expr::Node::Now(eproto::Now {})),
            Expr::Ref { name } => Some(eproto::expr::Node::Ref(eproto::Ref { name })),
            Expr::Lit { value } => {
                let literal = value.to_json();
                let dtype = natural_type(&value);
                Some(eproto::expr::Node::JsonLiteral(eproto::JsonLiteral {
                    literal: literal.to_string(),
                    dtype: Some((&dtype).into()),
                }))
            }
            Expr::DateTimeLiteral {
                year,
                month,
                day,
                hour,
                minute,
                second,
                microsecond,
                timezone,
            } => Some(eproto::expr::Node::DatetimeLiteral(
                eproto::DatetimeLiteral {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    microsecond,
                    timezone: timezone.map(|tz| ProtoTimezone { timezone: tz }),
                },
            )),
            Expr::MakeStruct {
                struct_type,
                fields,
            } => Some(eproto::expr::Node::MakeStruct(eproto::MakeStruct {
                struct_type: Some((&struct_type).into()),
                fields: fields
                    .into_iter()
                    .map(|(name, expr)| (name, (*expr).into()))
                    .collect::<HashMap<_, _>>(),
            })),
            Expr::Repeat { expr, count } => {
                Some(eproto::expr::Node::Repeat(Box::new(eproto::Repeat {
                    expr: Some(Box::new((*expr).into())),
                    count: Some(Box::new((*count).into())),
                })))
            }
            Expr::Zip {
                struct_type,
                fields,
            } => Some(eproto::expr::Node::Zip(eproto::Zip {
                struct_type: Some((&struct_type).into()),
                fields: fields
                    .into_iter()
                    .map(|(name, expr)| (name, (*expr).into()))
                    .collect::<HashMap<_, _>>(),
            })),
            Expr::FromEpoch { expr, unit } => {
                Some(eproto::expr::Node::FromEpoch(Box::new(eproto::FromEpoch {
                    duration: Some(Box::new((*expr).into())),
                    unit: ProtoTimeUnit::from(unit).into(),
                })))
            }
            Expr::Unary { op, expr } => Some(eproto::expr::Node::Unary(Box::new(eproto::Unary {
                op: op.into(),
                operand: Some(Box::new((*expr).into())),
            }))),
            Expr::Binary { op, left, right } => {
                Some(eproto::expr::Node::Binary(Box::new(eproto::Binary {
                    op: op.into(),
                    left: Some(Box::new((*left).into())),
                    right: Some(Box::new((*right).into())),
                })))
            }
            Expr::Case {
                when_thens,
                otherwise,
            } => Some(eproto::expr::Node::Case(Box::new(eproto::Case {
                when_then: when_thens
                    .into_iter()
                    .map(|(when, then)| eproto::WhenThen {
                        when: Some(when.into()),
                        then: Some(then.into()),
                    })
                    .collect::<Vec<_>>(),
                otherwise: otherwise.map(|o| Box::new((*o).into())),
            }))),
            Expr::IsNull { expr } => Some(eproto::expr::Node::Isnull(Box::new(eproto::IsNull {
                operand: Some(Box::new((*expr).into())),
            }))),
            Expr::FillNull { expr, default } => {
                Some(eproto::expr::Node::Fillnull(Box::new(eproto::FillNull {
                    operand: Some(Box::new((*expr).into())),
                    fill: Some(Box::new((*default).into())),
                })))
            }
            Expr::MathFn { func, expr } => {
                let math_fn = match func {
                    MathFn::Abs => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Abs(eproto::Abs {})),
                    },
                    MathFn::Ceil => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Ceil(eproto::Ceil {})),
                    },
                    MathFn::Floor => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Floor(eproto::Floor {})),
                    },
                    MathFn::Round { precision } => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Round(eproto::Round {
                            precision: precision as i32,
                        })),
                    },
                    MathFn::ToString => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::ToString(eproto::ToString {})),
                    },
                    MathFn::Pow { exponent } => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Pow(Box::new(eproto::Pow {
                            exponent: Some(Box::new((*exponent).into())),
                        }))),
                    },
                    MathFn::Log { base } => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Log(eproto::Log { base })),
                    },
                    MathFn::Sqrt => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Sqrt(eproto::Sqrt {})),
                    },
                    MathFn::Sin => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Sin(eproto::Sin {})),
                    },
                    MathFn::Cos => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Cos(eproto::Cos {})),
                    },
                    MathFn::Tan => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Tan(eproto::Tan {})),
                    },
                    MathFn::Asin => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Asin(eproto::Asin {})),
                    },
                    MathFn::Acos => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Acos(eproto::Acos {})),
                    },
                    MathFn::Atan => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::Atan(eproto::Atan {})),
                    },
                    MathFn::IsNan => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::IsNan(eproto::IsNan {})),
                    },
                    MathFn::IsInfinite => eproto::MathOp {
                        fn_type: Some(eproto::math_op::FnType::IsInfinite(eproto::IsInfinite {})),
                    },
                };
                Some(eproto::expr::Node::MathFn(Box::new(eproto::MathFn {
                    operand: Some(Box::new((*expr).into())),
                    r#fn: Some(Box::new(math_fn)),
                })))
            }
            Expr::DatetimeFn { func, expr } => {
                let datetime_fn = match *func {
                    DateTimeFn::Since { other, unit } => eproto::DateTimeOp {
                        fn_type: Some(eproto::date_time_op::FnType::Since(Box::new(
                            eproto::Since {
                                other: Some(Box::new((*other).into())),
                                unit: ProtoTimeUnit::from(unit).into(),
                            },
                        ))),
                    },
                    DateTimeFn::SinceEpoch { unit } => eproto::DateTimeOp {
                        fn_type: Some(eproto::date_time_op::FnType::SinceEpoch(
                            eproto::SinceEpoch {
                                unit: ProtoTimeUnit::from(unit).into(),
                            },
                        )),
                    },
                    DateTimeFn::Part { unit, timezone } => eproto::DateTimeOp {
                        fn_type: Some(eproto::date_time_op::FnType::Part(eproto::Part {
                            unit: ProtoTimeUnit::from(unit).into(),
                            timezone: timezone.map(|tz| ProtoTimezone { timezone: tz }),
                        })),
                    },
                    DateTimeFn::Stftime { format, timezone } => eproto::DateTimeOp {
                        fn_type: Some(eproto::date_time_op::FnType::Strftime(eproto::Strftime {
                            format,
                            timezone: timezone.map(|tz| ProtoTimezone { timezone: tz }),
                        })),
                    },
                };
                Some(eproto::expr::Node::DatetimeFn(Box::new(
                    eproto::DateTimeFn {
                        datetime: Some(Box::new((*expr).into())),
                        r#fn: Some(Box::new(datetime_fn)),
                    },
                )))
            }
            Expr::StringFn { func, expr } => {
                let string_fn = match *func {
                    StringFn::Len => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Len(eproto::Len {})),
                    },
                    StringFn::ToLower => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Tolower(eproto::ToLower {})),
                    },
                    StringFn::ToUpper => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Toupper(eproto::ToUpper {})),
                    },
                    StringFn::ToInt => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::ToInt(eproto::ToInt {})),
                    },
                    StringFn::Contains { ref key } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Contains(Box::new(
                            eproto::Contains {
                                element: Some(Box::new(key.clone().into())),
                            },
                        ))),
                    },
                    StringFn::StartsWith { ref key } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Startswith(Box::new(
                            eproto::StartsWith {
                                key: Some(Box::new(key.clone().into())),
                            },
                        ))),
                    },
                    StringFn::EndsWith { ref key } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Endswith(Box::new(
                            eproto::EndsWith {
                                key: Some(Box::new(key.clone().into())),
                            },
                        ))),
                    },
                    StringFn::Concat { ref other } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Concat(Box::new(
                            eproto::Concat {
                                other: Some(Box::new(other.clone().into())),
                            },
                        ))),
                    },
                    StringFn::Strptime { format, timezone } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Strptime(eproto::Strptime {
                            format,
                            timezone: timezone.map(|tz| ProtoTimezone { timezone: tz }),
                        })),
                    },
                    StringFn::JsonDecode { dtype } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::JsonDecode(eproto::JsonDecode {
                            dtype: Some((&dtype).into()),
                        })),
                    },
                    StringFn::Split { ref sep } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::Split(eproto::Split {
                            sep: sep.clone(),
                        })),
                    },
                    StringFn::JsonExtract { ref path } => eproto::StringOp {
                        fn_type: Some(eproto::string_op::FnType::JsonExtract(
                            eproto::JsonExtract { path: path.clone() },
                        )),
                    },
                };
                Some(eproto::expr::Node::StringFn(Box::new(eproto::StringFn {
                    string: Some(Box::new((*expr).into())),
                    r#fn: Some(Box::new(string_fn)),
                })))
            }
            Expr::DictFn { dict, func } => {
                let dict_fn = match *func {
                    DictFn::Len => eproto::DictOp {
                        fn_type: Some(eproto::dict_op::FnType::Len(eproto::Len {})),
                    },
                    DictFn::Contains { ref key } => eproto::DictOp {
                        fn_type: Some(eproto::dict_op::FnType::Contains(Box::new(
                            eproto::Contains {
                                element: Some(Box::new(key.clone().into())),
                            },
                        ))),
                    },
                    DictFn::Get {
                        ref key,
                        ref default,
                    } => {
                        let default_value = match default.clone() {
                            Some(d) => Some(Box::new(d.into())),
                            None => None,
                        };

                        eproto::DictOp {
                            fn_type: Some(eproto::dict_op::FnType::Get(Box::new(
                                eproto::DictGet {
                                    field: Some(Box::new(key.clone().into())),
                                    default_value,
                                },
                            ))),
                        }
                    }
                };
                Some(eproto::expr::Node::DictFn(Box::new(eproto::DictFn {
                    dict: Some(Box::new((*dict).into())),
                    r#fn: Some(Box::new(dict_fn)),
                })))
            }
            Expr::StructFn { struct_, func } => {
                let struct_fn = match *func {
                    StructFn::Get { ref field } => eproto::StructOp {
                        fn_type: Some(eproto::struct_op::FnType::Field(field.clone())),
                    },
                };
                Some(eproto::expr::Node::StructFn(Box::new(eproto::StructFn {
                    r#struct: Some(Box::new((*struct_).into())),
                    r#fn: Some(struct_fn),
                })))
            }
            Expr::ListFn { list, func } => {
                let list_fn = match *func {
                    ListFn::Len => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Len(eproto::Len {})),
                    },
                    ListFn::Contains { ref item } => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Contains(Box::new(
                            eproto::Contains {
                                element: Some(Box::new(item.clone().into())),
                            },
                        ))),
                    },
                    ListFn::Get { ref index } => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Get(Box::new(index.clone().into()))),
                    },
                    ListFn::HasNull => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::HasNull(eproto::HasNull {})),
                    },
                    ListFn::Sum => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Sum(eproto::ListSum {})),
                    },
                    ListFn::Min => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Min(eproto::ListMin {})),
                    },
                    ListFn::Max => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Max(eproto::ListMax {})),
                    },
                    ListFn::All => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::All(eproto::ListAll {})),
                    },
                    ListFn::Any => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Any(eproto::ListAny {})),
                    },
                    ListFn::Mean => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Mean(eproto::ListMean {})),
                    },
                    ListFn::Filter { var, predicate } => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Filter(Box::new(
                            eproto::ListFilter {
                                var,
                                predicate: Some(Box::new(predicate.into())),
                            },
                        ))),
                    },
                    ListFn::Map { var, func } => eproto::ListOp {
                        fn_type: Some(eproto::list_op::FnType::Map(Box::new(eproto::ListMap {
                            var,
                            map_expr: Some(Box::new(func.into())),
                        }))),
                    },
                };
                Some(eproto::expr::Node::ListFn(Box::new(eproto::ListFn {
                    list: Some(Box::new((*list).into())),
                    r#fn: Some(Box::new(list_fn)),
                })))
            }
            Expr::Cast { .. } => panic!("Cast should not be serialized"),
        };
        ProtoExpr { node }
    }
}

// Conversion for BinOp
impl From<BinOp> for i32 {
    fn from(op: BinOp) -> Self {
        match op {
            BinOp::Add => crate::schema_proto::expression::BinOp::Add as i32,
            BinOp::Sub => crate::schema_proto::expression::BinOp::Sub as i32,
            BinOp::Mul => crate::schema_proto::expression::BinOp::Mul as i32,
            BinOp::Div => crate::schema_proto::expression::BinOp::Div as i32,
            BinOp::Mod => crate::schema_proto::expression::BinOp::Mod as i32,
            BinOp::FloorDiv => crate::schema_proto::expression::BinOp::FloorDiv as i32,
            BinOp::Eq => crate::schema_proto::expression::BinOp::Eq as i32,
            BinOp::Neq => crate::schema_proto::expression::BinOp::Ne as i32,
            BinOp::Gt => crate::schema_proto::expression::BinOp::Gt as i32,
            BinOp::Gte => crate::schema_proto::expression::BinOp::Gte as i32,
            BinOp::Lt => crate::schema_proto::expression::BinOp::Lt as i32,
            BinOp::Lte => crate::schema_proto::expression::BinOp::Lte as i32,
            BinOp::And => crate::schema_proto::expression::BinOp::And as i32,
            BinOp::Or => crate::schema_proto::expression::BinOp::Or as i32,
        }
    }
}

// Conversion for UnOp
impl From<UnOp> for i32 {
    fn from(op: UnOp) -> Self {
        match op {
            UnOp::Neg => crate::schema_proto::expression::UnaryOp::Neg as i32,
            UnOp::Not => crate::schema_proto::expression::UnaryOp::Not as i32,
        }
    }
}

// Conversion for ExprContext
impl TryFrom<ProtoEvalContext> for EvalContext {
    type Error = anyhow::Error;

    fn try_from(value: ProtoEvalContext) -> Result<Self> {
        Ok(Self {
            now_col_name: value.now_col_name,
            index_col_name: value.index_col_name,
        })
    }
}

impl From<EvalContext> for ProtoEvalContext {
    fn from(ctx: EvalContext) -> Self {
        Self {
            now_col_name: ctx.now_col_name,
            index_col_name: ctx.index_col_name,
        }
    }
}

fn validate_timezone(tz: &str, expr: &Expr) -> Result<()> {
    match tz.parse::<chrono_tz::Tz>() {
        Ok(_) => Ok(()),
        Err(_) => Err(anyhow::anyhow!(
            "invalid timezone: `{}` in expression: `{:?}`\n\
            Note: Fennel does not currently support parsing strings with timezone offsets. \
            Consider using a timezone name (e.g. 'America/New_York') instead of an offset.",
            tz,
            expr
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::builders::*;
    use super::*;
    use crate::df::Dataframe;
    use crate::rowcol::Col;
    use crate::schema::Field;
    use crate::types;
    use crate::value;
    use crate::value::Value;
    use crate::UTCTimestamp;
    use itertools::Itertools;
    use std::sync::Arc;
    use Type::*;

    #[derive(Debug, Clone)]
    struct Case {
        expr: Expr,
        valid: Option<bool>,
        matches: Vec<(Type, Vec<Value>)>,
        mismatches: Vec<Type>,
        produces: Option<(Type, Vec<Value>)>,
        errors: Option<bool>,
        eval_ctx: Option<EvalContext>,
    }

    fn check(df: Dataframe, cases: impl IntoIterator<Item = Case>) {
        for case in cases {
            // first verify that case itself is well formed
            case.check();
            let schema = df.schema();
            let compiled = case.expr.compile(schema.clone());
            match case.valid {
                None => panic!("invalid test case: validity not set"),
                Some(false) => {
                    assert!(compiled.is_err(), "expected {:?} to be invalid", case.expr);
                    continue;
                }
                Some(true) => {
                    assert!(
                        compiled.is_ok(),
                        "expected {:?} to be valid but got {:?}",
                        case.expr,
                        compiled
                    );
                    let compiled = compiled.unwrap();
                    // verify all mismatches
                    for dtype in &case.mismatches {
                        assert!(
                            !compiled.matches(dtype),
                            "expected {:?} to not match for expr: {} of type {:?}",
                            dtype,
                            compiled,
                            compiled.dtype()
                        );
                    }
                    // if matches is set, verify that it matches
                    for (dtype, expected) in &case.matches {
                        assert!(
                            compiled.matches(dtype),
                            "expected {:?} to match for expr: {} of type {:?}",
                            dtype,
                            compiled,
                            compiled.dtype()
                        );
                        let found = df
                            .eval(&compiled, dtype.clone(), "result", case.eval_ctx.clone())
                            .unwrap();
                        let expected = Col::from("result", dtype.clone(), expected.clone());
                        assert!(
                            expected.is_ok(),
                            "could not create col for expr: {}, error: {:?}",
                            compiled,
                            expected
                        );
                        assert_eq!(found, expected.unwrap(), "mismatch for expr: {}", compiled);
                    }
                    // if errors is set, verify that it errors
                    if let Some(true) = case.errors {
                        let result = df.eval(
                            &compiled,
                            compiled.dtype().clone(),
                            "result",
                            case.eval_ctx.clone(),
                        );
                        assert!(
                            result.is_err(),
                            "expected {:?} to error but got {:?}",
                            compiled,
                            result
                        );
                    }
                    // if produces is set, verify that it produces
                    if let Some((dtype, expected)) = &case.produces {
                        assert!(
                            compiled.dtype() == dtype,
                            "expression {:?}, expected to produce type {:?} but got {:?}",
                            compiled,
                            dtype,
                            compiled.dtype()
                        );
                        let expected = Col::from("result", dtype.clone(), expected.clone());
                        let found =
                            df.eval(&compiled, dtype.clone(), "result", case.eval_ctx.clone());
                        assert!(
                            found.is_ok(),
                            "could not eval expr: {}, error: {:?}",
                            compiled,
                            found.unwrap_err()
                        );
                        assert_eq!(
                            found.unwrap(),
                            expected.unwrap(),
                            "mismatch for expr: {}",
                            compiled
                        );
                    }
                }
            }
        }
    }
    impl Case {
        fn new(expr: Expr) -> Self {
            Self {
                expr,
                valid: None,
                mismatches: vec![],
                matches: vec![],
                produces: None,
                errors: None,
                eval_ctx: None,
            }
        }
        fn errors(mut self) -> Self {
            self.valid = Some(true);
            self.errors = Some(true);
            self
        }

        fn invalid(mut self) -> Self {
            self.valid = Some(false);
            self
        }

        fn eval_ctx(mut self, ctx: EvalContext) -> Self {
            self.eval_ctx = Some(ctx);
            self
        }

        fn produces<T: Into<Value>>(
            mut self,
            dtype: Type,
            values: impl IntoIterator<Item = T>,
        ) -> Self {
            self.valid = Some(true);
            let values = values.into_iter().map(Into::into).collect::<Vec<_>>();
            self.produces = Some((dtype, values));
            self
        }
        fn try_produces<T: TryInto<Value, Error = anyhow::Error>>(
            mut self,
            dtype: Type,
            values: impl IntoIterator<Item = T>,
        ) -> Self {
            self.valid = Some(true);
            let values = values
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>>>()
                .unwrap();
            self.produces = Some((dtype, values));
            self
        }
        fn matches<T: Into<Value>>(
            mut self,
            dtype: Type,
            values: impl IntoIterator<Item = T>,
        ) -> Self {
            self.valid = Some(true);
            let expected =
                Col::from("result", dtype.clone(), values.into_iter().map(Into::into)).unwrap();
            let expected = expected.values().to_vec();
            self.matches.push((dtype, expected));
            self
        }

        fn mismatches(mut self, dtypes: impl IntoIterator<Item = Type>) -> Self {
            self.mismatches.extend(dtypes);
            self
        }

        fn check(&self) {
            // case is well formed: either it's invalid or matches
            if self.valid.is_none() {
                assert!(!self.matches.is_empty(), "no matches set");
            }
        }
    }

    fn dict_df() -> Dataframe {
        Dataframe::new(vec![Col::from(
            "dict",
            Type::optional(Type::Map(Box::new(Int))),
            vec![
                Some(Value::Map(Arc::new(
                    value::Map::new(
                        Type::Int,
                        &[
                            ("a".to_string(), Value::Int(1)),
                            ("b".to_string(), Value::Int(2)),
                            ("c".to_string(), Value::Int(3)),
                        ],
                    )
                    .unwrap(),
                ))),
                None,
                Some(Value::Map(Arc::new(
                    value::Map::new(Type::Int, &[]).unwrap(),
                ))),
                Some(Value::Map(Arc::new(
                    value::Map::new(
                        Type::Int,
                        &[
                            ("a".to_string(), Value::Int(8)),
                            ("i".to_string(), Value::Int(9)),
                        ],
                    )
                    .unwrap(),
                ))),
            ],
        )
        .unwrap()])
        .unwrap()
    }

    fn list_df() -> Dataframe {
        // create customer df - with one list of int column, one optional list
        // of int, one list of optional int, and one non-list int column
        let ltype1 = List(Box::new(Int));
        let ltype2 = Type::optional(ltype1.clone());
        let ltype3 = List(Box::new(Type::optional(Int)));
        let ltype4 = Type::optional(List(Box::new(Float)));
        let ltype5 = Type::optional(List(Box::new(Type::optional(Int))));
        let df = Dataframe::new(vec![
            Col::new(
                Arc::new(Field::new("a", ltype1.clone())),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                    )),
                    Value::List(Arc::new(value::List::new(Int, &[Value::Int(3)]).unwrap())),
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(4), Value::Int(5)]).unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b", ltype2.clone())),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                    )),
                    Value::None,
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(4), Value::Int(5)]).unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("c", ltype3.clone())),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(1), Value::None, Value::Int(2)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(11), Value::Int(4), Value::Int(2)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(3), Value::None, Value::Int(6)],
                        )
                        .unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("d", Int)),
                Arc::new(vec![Value::Int(0), Value::Int(1), Value::Int(2)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("e", ltype4.clone())),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(Float, &[Value::Float(1.0), Value::Float(2.0)]).unwrap(),
                    )),
                    Value::None,
                    Value::List(Arc::new(
                        value::List::new(Float, &[Value::Float(5.0), Value::Float(6.0)]).unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("f", ltype5.clone())),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(1), Value::None, Value::Int(2)],
                        )
                        .unwrap(),
                    )),
                    Value::None,
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(3), Value::None, Value::Int(6)],
                        )
                        .unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("basic_float", List(Box::new(Float)))),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(Float, &[Value::Float(1.0), Value::Float(2.0)]).unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(Float, &[Value::Float(3.0)]).unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(Float, &[Value::Float(5.0), Value::Float(6.0)]).unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("empty_float", List(Box::new(Float)))),
                Arc::new(vec![
                    Value::List(Arc::new(value::List::new(Float, &[]).unwrap())),
                    Value::List(Arc::new(value::List::new(Float, &[]).unwrap())),
                    Value::List(Arc::new(
                        value::List::new(Float, &[Value::Float(2.1)]).unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("empty_optional", ltype5.clone())),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(Type::optional(Int), &[]).unwrap(),
                    )),
                    Value::None,
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(3), Value::None, Value::Int(6)],
                        )
                        .unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("str_list", List(Box::new(String)))),
                Arc::new(vec![
                    Value::List(Arc::new(
                        value::List::new(
                            String,
                            &[
                                Value::String(Arc::new("a".to_string())),
                                Value::String(Arc::new("b".to_string())),
                            ],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            String,
                            &[
                                Value::String(Arc::new("c".to_string())),
                                Value::String(Arc::new("d".to_string())),
                                Value::String(Arc::new("e".to_string())),
                            ],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(value::List::new(String, &[]).unwrap())),
                ]),
            )
            .unwrap(),
        ])
        .unwrap();
        df
    }

    fn default_df() -> Dataframe {
        Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13]).unwrap(),
            Col::from("c", Type::Float, vec![1.0, 2.0, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["1", "1.0", "Hi"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from("now_col", Type::Timestamp, vec![10, 12, 14]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
            Col::from("h", Type::Float, vec![-1.0, 2.0, -3.1]).unwrap(),
            Col::from("i", Type::Timestamp, vec![4, 5, 6]).unwrap(),
            Col::from(
                "null_ints",
                Type::Optional(Box::new(Type::Int)),
                vec![None::<Value>, None, None],
            )
            .unwrap(),
            Col::from(
                "null_bools",
                Type::Optional(Box::new(Type::Bool)),
                vec![None::<Value>, None, None],
            )
            .unwrap(),
            Col::from(
                "null_floats",
                Type::Optional(Box::new(Type::Float)),
                vec![None::<Value>, None, None],
            )
            .unwrap(),
            Col::from(
                "null_strings",
                Type::Optional(Box::new(Type::String)),
                vec![Some("Hi"), None, Some("Bye")],
            )
            .unwrap(),
            Col::from(
                "null_dt_strings",
                Type::Optional(Box::new(Type::String)),
                vec![Some("2021-01-01"), None, Some("2021-01-02")],
            )
            .unwrap(),
            Col::from(
                "nan_floats",
                Type::Float,
                vec![f64::NAN, f64::INFINITY, f64::NEG_INFINITY],
            )
            .unwrap(),
            Col::from(
                "null_nan_floats",
                Type::Optional(Box::new(Type::Float)),
                vec![None::<f64>, Some(f64::NAN), Some(f64::INFINITY)],
            )
            .unwrap(),
        ])
        .unwrap()
    }

    #[test]
    fn test_unary_not() {
        use Type::*;
        let df = Dataframe::new(vec![
            Col::from("a", Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", String, vec!["a", "b", "c"]).unwrap(),
            Col::from("c", Bool, vec![true, false, true]).unwrap(),
            Col::from("d", Float, vec![1.1, 2.2, 3.3]).unwrap(),
            Col::from(
                "e",
                Type::optional(Bool),
                vec![Some(true), None, Some(false)],
            )
            .unwrap(),
            Col::from("f", Type::optional(Int), vec![Some(1), None, Some(3)]).unwrap(),
        ])
        .unwrap();

        let cases = [
            // valid literals
            Case::new(not(lit(true))).matches(Bool, [false, false, false]),
            Case::new(not(lit(false))).matches(Bool, [true, true, true]),
            Case::new(not(lit(Value::None)))
                .matches(Type::optional(Bool), [None::<Value>, None, None]),
            // invalid literals
            Case::new(not(lit(1))).invalid(),
            Case::new(not(lit("hi"))).invalid(),
            Case::new(not(lit(1.0))).invalid(),
            // col refs
            Case::new(not(col("a"))).invalid(),
            Case::new(not(col("b"))).invalid(),
            Case::new(not(col("c"))).matches(Bool, [false, true, false]),
            Case::new(not(col("d"))).invalid(),
            Case::new(not(col("e")))
                .mismatches([Bool])
                .matches(Type::optional(Bool), [Some(false), None, Some(true)]),
            Case::new(not(col("f"))).invalid(),
            // composite - not of not
            Case::new(not(not(col("c")))).matches(Bool, [true, false, true]),
            Case::new(not(not(col("e"))))
                .matches(Type::optional(Bool), [Some(true), None, Some(false)]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_lit() {
        use crate::types::{Between, OneOf};
        let df = Dataframe::new(vec![Col::from("a", Int, vec![1, 2, 3]).unwrap()]).unwrap();
        let cases = [
            Case::new(lit(1))
                .matches(Int, [1, 1, 1])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .matches(
                    Between(Between::new(Int, 0.into(), 10.into(), false, false).unwrap()),
                    [1, 1, 1],
                )
                .matches(
                    OneOf(OneOf::new(Int, vec![1.into(), 2.into()]).unwrap()),
                    [1, 1, 1],
                )
                .mismatches([Bool, String, Type::List(Box::new(Int))]),
            Case::new(lit(1.0))
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([
                    Int,
                    Bool,
                    String,
                    Type::optional(Int),
                    List(Box::new(Float)),
                ]),
            Case::new(lit(true))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, List(Box::new(Bool))]),
            Case::new(lit("hi"))
                .matches(String, ["hi", "hi", "hi"])
                .matches(Type::optional(String), [Some("hi"), Some("hi"), Some("hi")])
                .mismatches([Int, Float, Bool, List(Box::new(String))]),
            Case::new(lit(Value::None))
                .matches(Type::Null, [Value::None, Value::None, Value::None])
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .matches(Type::optional(Bool), [None::<Value>, None, None])
                .matches(Type::optional(String), [None::<Value>, None, None])
                .matches(Type::optional(Date), [None::<Value>, None, None])
                .matches(Type::optional(Timestamp), [None::<Value>, None, None])
                .mismatches([Int, Float, Bool, String, Date, Timestamp])
                .mismatches([Type::List(Box::new(Int))])
                .mismatches([Struct(Box::new(
                    types::StructType::new(
                        "somename".into(),
                        vec![Field::new("a".to_string(), Int)],
                    )
                    .unwrap(),
                ))])
                .mismatches([Decimal(types::DecimalType::new(5).unwrap())]),
            Case::new(lit(Value::List(Arc::new(
                value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
            ))))
            .produces(
                List(Box::new(Int)),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                    ));
                    3
                ],
            ),
            // can also do 2d lists
            Case::new(lit(Value::List(Arc::new(
                value::List::new(
                    Type::List(Box::new(Int)),
                    &[Value::List(Arc::new(
                        value::List::new(Type::Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                    ))],
                )
                .unwrap(),
            ))))
            .produces(
                List(Box::new(List(Box::new(Int)))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::List(Box::new(Int)),
                            &[Value::List(Arc::new(
                                value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                            ))],
                        )
                        .unwrap(),
                    ));
                    3
                ],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_ref_matches() {
        use crate::value::List;
        let list = Col::from(
            "r",
            Type::List(Box::new(Type::Int)),
            [
                Value::List(Arc::new(
                    List::new(Type::Int, &[1.into(), 2.into(), 3.into()]).unwrap(),
                )),
                Value::List(Arc::new(
                    List::new(Type::Int, &[4.into(), 5.into()]).unwrap(),
                )),
                Value::List(Arc::new(List::new(Type::Int, &[6.into()]).unwrap())),
            ],
        )
        .unwrap();
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::String, vec!["a", "b", "c"]).unwrap(),
            Col::from("c", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("d", Type::Float, vec![1.1, 2.2, 3.3]).unwrap(),
            Col::from("e", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "f",
                Type::List(Box::new(Type::Int)),
                list.values().iter().map(|v| v.clone()).collect_vec(),
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            Case::new(col("a"))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([
                    Bool,
                    String,
                    Timestamp,
                    Date,
                    List(Box::new(Int)),
                    Struct(Box::new(
                        types::StructType::new(
                            "somename".into(),
                            vec![Field::new("a".to_string(), Int)],
                        )
                        .unwrap(),
                    )),
                ]),
            Case::new(col("b"))
                .matches(String, ["a", "b", "c"])
                .matches(Type::optional(String), [Some("a"), Some("b"), Some("c")])
                .mismatches([
                    Int,
                    Float,
                    Bool,
                    Timestamp,
                    Date,
                    List(Box::new(String)),
                    Struct(Box::new(
                        types::StructType::new(
                            "somename".into(),
                            vec![Field::new("a".to_string(), String)],
                        )
                        .unwrap(),
                    )),
                ]),
            Case::new(col("c"))
                .matches(Bool, [true, false, true])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(true)])
                .mismatches([
                    Int,
                    Float,
                    String,
                    Timestamp,
                    Date,
                    List(Box::new(Bool)),
                    Struct(Box::new(
                        types::StructType::new(
                            "somename".into(),
                            vec![Field::new("a".to_string(), Bool)],
                        )
                        .unwrap(),
                    )),
                ]),
            Case::new(col("d"))
                .matches(Float, [1.1, 2.2, 3.3])
                .matches(Type::optional(Float), [Some(1.1), Some(2.2), Some(3.3)])
                .mismatches([
                    Int,
                    Bool,
                    String,
                    Timestamp,
                    Date,
                    Type::optional(Int),
                    List(Box::new(Float)),
                    Struct(Box::new(
                        types::StructType::new(
                            "somename".into(),
                            vec![Field::new("a".to_string(), Float)],
                        )
                        .unwrap(),
                    )),
                ]),
            Case::new(col("e"))
                .matches(Timestamp, [1, 2, 3])
                .matches(Type::optional(Timestamp), [Some(1), Some(2), Some(3)])
                .mismatches([
                    Int,
                    Bool,
                    String,
                    Float,
                    Date,
                    List(Box::new(Timestamp)),
                    Struct(Box::new(
                        types::StructType::new(
                            "somename".into(),
                            vec![Field::new("a".to_string(), Timestamp)],
                        )
                        .unwrap(),
                    )),
                ]),
            Case::new(col("f"))
                .matches(
                    List(Box::new(Int)),
                    list.values().iter().map(|v| v.clone()).collect_vec(),
                )
                .matches(
                    Type::optional(List(Box::new(Int))),
                    list.values().iter().map(|v| Some(v.clone())).collect_vec(),
                )
                .mismatches([
                    Int,
                    Bool,
                    String,
                    Float,
                    Date,
                    List(Box::new(Float)),
                    Struct(Box::new(
                        types::StructType::new(
                            "somename".into(),
                            vec![Field::new("a".to_string(), List(Box::new(Int)))],
                        )
                        .unwrap(),
                    )),
                ]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_unary_neg() {
        let df = Dataframe::new(vec![
            Col::from("a", Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", String, vec!["a", "b", "c"]).unwrap(),
            Col::from("c", Bool, vec![true, false, true]).unwrap(),
            Col::from("d", Float, vec![1.1, 2.2, 3.3]).unwrap(),
            Col::from("e", Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from("f", Optional(Box::new(Int)), vec![Some(1), None, Some(3)]).unwrap(),
        ])
        .unwrap();
        let cases = [
            // some invalid cases
            Case::new(neg(lit(true))).invalid(),
            Case::new(neg(lit("hi"))).invalid(),
            Case::new(neg(col("b"))).invalid(),
            Case::new(neg(col("c"))).invalid(),
            Case::new(neg(col("e"))).invalid(),
            // negation of literals
            Case::new(neg(lit(1)))
                .matches(Int, [-1, -1, -1])
                .matches(Float, [-1.0, -1.0, -1.0])
                .matches(Type::optional(Int), [Some(-1), Some(-1), Some(-1)])
                .matches(Type::optional(Float), [Some(-1.0), Some(-1.0), Some(-1.0)])
                .mismatches([String, Bool, Timestamp, Date])
                .mismatches([List(Box::new(Int)), Map(Box::new(Int))]),
            Case::new(neg(lit(1.1)))
                .matches(Float, [-1.1, -1.1, -1.1])
                .matches(Type::optional(Float), [Some(-1.1), Some(-1.1), Some(-1.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)])
                .mismatches([List(Box::new(Float)), Map(Box::new(Float))]),
            // negation of column refs
            Case::new(neg(col("a")))
                .matches(Int, [-1, -2, -3])
                .matches(Float, [-1.0, -2.0, -3.0])
                .matches(Type::optional(Int), [Some(-1), Some(-2), Some(-3)])
                .matches(Type::optional(Float), [Some(-1.0), Some(-2.0), Some(-3.0)])
                .mismatches([String, Bool, Timestamp])
                .mismatches([List(Box::new(Int)), Map(Box::new(Int))]),
            Case::new(neg(col("d")))
                .matches(Float, [-1.1, -2.2, -3.3])
                .matches(Type::optional(Float), [Some(-1.1), Some(-2.2), Some(-3.3)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)])
                .mismatches([List(Box::new(Float)), Map(Box::new(Float))]),
            Case::new(neg(col("f")))
                .matches(Type::optional(Int), [Some(-1), None, Some(-3)])
                .matches(Type::optional(Float), [Some(-1.0), None, Some(-3.0)])
                .mismatches([Int, Float, String, Bool, Timestamp, List(Box::new(Int))])
                .mismatches([List(Box::new(Float)), Map(Box::new(Float))]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_arithmetic_sub() {
        // same as test_arithmetic_add but with subtraction
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13]).unwrap(),
            Col::from("c", Type::Float, vec![1.1, 2.1, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["hi", "bye", "something"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            // invalid subtraction using literals
            Case::new(lit("hi") - lit("bye")).invalid(),
            Case::new(lit(1) - lit("bye")).invalid(),
            Case::new(lit(true) - lit(1)).invalid(),
            Case::new(lit(1) - lit(true)).invalid(),
            Case::new(lit(true) - lit(false)).invalid(),
            // invalid subtraction using refs
            Case::new(col("a") - lit("bye")).invalid(),
            Case::new(col("a") - col("d")).invalid(),
            Case::new(col("a") - col("e")).invalid(),
            Case::new(col("a") - col("f")).invalid(),
            // subtraction of two integers or two floats
            Case::new(lit(1) - lit(2))
                .matches(Int, [-1, -1, -1])
                .matches(Float, [-1.0, -1.0, -1.0])
                .matches(Type::optional(Int), [Some(-1), Some(-1), Some(-1)])
                .matches(Type::optional(Float), [Some(-1.0), Some(-1.0), Some(-1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(lit(1.0) - lit(2.0))
                .matches(Float, [-1.0, -1.0, -1.0])
                .matches(Type::optional(Float), [Some(-1.0), Some(-1.0), Some(-1.0)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            Case::new(col("a") - lit(2))
                .matches(Int, [-1, 0, 1])
                .matches(Float, [-1.0, 0.0, 1.0])
                .matches(Type::optional(Int), [Some(-1), Some(0), Some(1)])
                .matches(Type::optional(Float), [Some(-1.0), Some(0.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            // here
            Case::new(col("a") - col("a"))
                .matches(Int, [0, 0, 0])
                .matches(Float, [0.0, 0.0, 0.0])
                .matches(Type::optional(Int), [Some(0), Some(0), Some(0)])
                .matches(Type::optional(Float), [Some(0.0), Some(0.0), Some(0.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(col("a") - col("b"))
                .matches(Int, [-10, -10, -10])
                .matches(Float, [-10.0, -10.0, -10.0])
                .matches(Type::optional(Int), [Some(-10), Some(-10), Some(-10)])
                .matches(
                    Type::optional(Float),
                    [Some(-10.0), Some(-10.0), Some(-10.0)],
                )
                .mismatches([String, Bool, Timestamp]),
            Case::new(lit(1.5) - col("c"))
                .matches(Float, [0.4, -0.6, -1.6])
                .matches(Type::optional(Float), [Some(0.4), Some(-0.6), Some(-1.6)])
                .mismatches([Int, String, Bool, Timestamp]),
            // mix of int and float
            Case::new(lit(1) - lit(2.1))
                .matches(Float, [-1.1, -1.1, -1.1])
                .matches(Type::optional(Float), [Some(-1.1), Some(-1.1), Some(-1.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            Case::new(col("a") - col("c"))
                .matches(Float, [-0.1, -0.1, -0.1])
                .matches(Type::optional(Float), [Some(-0.1), Some(-0.1), Some(-0.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            // mix of int and float with optional
            Case::new(col("a") - col("g"))
                .matches(Optional(Box::new(Int)), [Some(0), None, Some(0)])
                .matches(Optional(Box::new(Float)), [Some(0.0), None, Some(0.0)])
                .mismatches([Float, String, Bool, Timestamp, Int]),
            Case::new(col("c") - col("g"))
                .matches(Optional(Box::new(Float)), [Some(0.1), None, Some(0.1)])
                .mismatches([Int, Float, String, Bool, Timestamp, Type::optional(Int)]),
            // some literal None
            Case::new(col("a") - lit(Value::None))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, Bool, String, Timestamp, Date]),
            Case::new(lit(Value::None) - lit(Value::None))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([
                    Int,
                    Float,
                    Bool,
                    String,
                    Timestamp,
                    Date,
                    Type::optional(Bool),
                    Type::optional(String),
                    Type::optional(Timestamp),
                    Type::optional(Date),
                ]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_arithmetic_mul() {
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13]).unwrap(),
            Col::from("c", Type::Float, vec![1.1, 2.1, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["hi", "bye", "something"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            // invalid multiplication using literals
            Case::new(lit("hi") * lit("bye")).invalid(),
            Case::new(lit(1) * lit("bye")).invalid(),
            Case::new(lit(true) * lit(1)).invalid(),
            Case::new(lit(1) * lit(true)).invalid(),
            Case::new(lit(true) * lit(false)).invalid(),
            // invalid multiplication using refs
            Case::new(col("a") * lit("bye")).invalid(),
            Case::new(col("a") * col("d")).invalid(),
            Case::new(col("a") * col("e")).invalid(),
            Case::new(col("a") * col("f")).invalid(),
            // multiplication of two integers or two floats
            Case::new(lit(1) * lit(2))
                .matches(Int, [2, 2, 2])
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Int), [Some(2), Some(2), Some(2)])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(lit(1.0) * lit(2.0))
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            Case::new(col("a") * lit(2))
                .matches(Int, [2, 4, 6])
                .matches(Float, [2.0, 4.0, 6.0])
                .matches(Type::optional(Int), [Some(2), Some(4), Some(6)])
                .matches(Type::optional(Float), [Some(2.0), Some(4.0), Some(6.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(col("a") * col("a"))
                .matches(Int, [1, 4, 9])
                .matches(Float, [1.0, 4.0, 9.0])
                .matches(Type::optional(Int), [Some(1), Some(4), Some(9)])
                .matches(Type::optional(Float), [Some(1.0), Some(4.0), Some(9.0)])
                .mismatches([String, Bool, Timestamp]),
            // mix of int and float
            Case::new(lit(1) * lit(2.1))
                .matches(Float, [2.1, 2.1, 2.1])
                .matches(Type::optional(Float), [Some(2.1), Some(2.1), Some(2.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            // mix of int and float with optional
            Case::new(col("a") * col("g"))
                .matches(Optional(Box::new(Int)), [Some(1), None, Some(9)])
                .matches(Optional(Box::new(Float)), [Some(1.0), None, Some(9.0)])
                .mismatches([Float, String, Bool, Timestamp, Int]),
            Case::new(col("c") * col("g"))
                .matches(Optional(Box::new(Float)), [Some(1.1), None, Some(9.3)])
                .mismatches([Int, Float, String, Bool, Timestamp, Type::optional(Int)]),
            Case::new(col("a") * lit(Value::None))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, Bool, String, Timestamp, Date]),
            Case::new(lit(Value::None) * lit(Value::None))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([
                    Int,
                    Float,
                    Bool,
                    String,
                    Timestamp,
                    Date,
                    Type::optional(Bool),
                    Type::optional(String),
                ]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_div() {
        let df = default_df();
        let cases = [
            // invalid division using literals
            Case::new(lit("hi") / lit("bye")).invalid(),
            Case::new(lit(1) / lit("bye")).invalid(),
            Case::new(lit(true) / lit(1)).invalid(),
            Case::new(lit(1) / lit(true)).invalid(),
            Case::new(lit(true) / lit(false)).invalid(),
            // division of two integers or two floats
            Case::new(lit(1) / lit(2)).produces(Float, [0.5; 3]),
            Case::new(lit(1.0) / lit(2.0)).produces(Float, [0.5; 3]),
            Case::new(lit(1.0) / lit(2)).produces(Float, [0.5; 3]),
            Case::new(lit(1) / lit(2.0)).produces(Float, [0.5; 3]),
            // check division by zero
            Case::new(lit(1) / lit(0)).produces(Float, [f64::INFINITY; 3]),
            Case::new(lit(0) / lit(0)).produces(Float, [f64::NAN; 3]),
            // check negative numbers
            Case::new(lit(-1) / lit(2)).produces(Float, [-0.5; 3]),
            Case::new(lit(1) / lit(-2)).produces(Float, [-0.5; 3]),
            // one is optional
            Case::new(lit(1) / col("g")).produces(
                Optional(Box::new(Float)),
                [Some(1.0), None, Some(0.33333333)],
            ),
            Case::new(col("a") / lit(2)).produces(Float, [0.5, 1.0, 1.5]),
            Case::new(col("g") / lit(2))
                .produces(Type::optional(Float), [Some(0.5), None, Some(1.5)]),
            // both are optional
            Case::new(col("g") / col("g"))
                .produces(Optional(Box::new(Float)), [Some(1.0), None, Some(1.0)]),
            // one is none
            Case::new(col("a") / lit(Value::None))
                .produces(Optional(Box::new(Float)), [None::<Value>, None, None]),
            Case::new(lit(Value::None) / lit(Value::None))
                .produces(Optional(Box::new(Float)), [None::<Value>, None, None]),
            Case::new(lit(Value::None) / col("a"))
                .produces(Optional(Box::new(Float)), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_floordiv() {
        let df = default_df();
        let cases = [
            // first some invalid cases
            Case::new(lit("hi").floordiv(lit("bye"))).invalid(),
            Case::new(lit(1).floordiv(lit("bye"))).invalid(),
            Case::new(lit(true).floordiv(lit(1))).invalid(),
            Case::new(lit(1).floordiv(lit(true))).invalid(),
            Case::new(lit(true).floordiv(lit(false))).invalid(),
            // division of two integers or two floats
            Case::new(lit(1).floordiv(lit(2))).produces(Int, [0; 3]),
            Case::new(lit(2).floordiv(lit(1))).produces(Int, [2; 3]),
            Case::new(lit(1.0).floordiv(lit(2.0))).produces(Float, [0.0; 3]),
            Case::new(lit(2.0).floordiv(lit(1.0))).produces(Float, [2.0; 3]),
            Case::new(lit(1.0).floordiv(lit(2))).produces(Float, [0.0; 3]),
            Case::new(lit(2).floordiv(lit(1.0))).produces(Float, [2.0; 3]),
            // negative numbers
            Case::new(lit(-1).floordiv(lit(2))).produces(Int, [-1; 3]),
            Case::new(lit(1).floordiv(lit(-2))).produces(Int, [-1; 3]),
            Case::new(lit(-1).floordiv(lit(-2))).produces(Int, [0; 3]),
            // division by zero
            Case::new(lit(1).floordiv(lit(0))).produces(Int, [0; 3]),
            // one is optional
            Case::new(lit(1).floordiv(col("g")))
                .produces(Optional(Box::new(Int)), [Some(1), None, Some(0)]),
            Case::new(col("a").floordiv(lit(2))).produces(Int, [0, 1, 1]),
            Case::new(col("g").floordiv(lit(2)))
                .produces(Optional(Box::new(Int)), [Some(0), None, Some(1)]),
            // both are optional
            Case::new(col("g").floordiv(col("g")))
                .produces(Optional(Box::new(Int)), [Some(1), None, Some(1)]),
            // one is none
            Case::new(col("a").floordiv(lit(Value::None)))
                .produces(Optional(Box::new(Int)), [None::<Value>, None, None]),
            Case::new(lit(Value::None).floordiv(lit(Value::None)))
                .produces(Optional(Box::new(Int)), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    // TODO: write tests for modulo
    // TODO: write tests for comparison operators
    #[test]
    fn test_arithmetic_add() {
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13]).unwrap(),
            Col::from("c", Type::Float, vec![1.1, 2.1, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["hi", "bye", "something"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            // invalid addition using literals
            Case::new(lit("hi") + lit("bye")).invalid(),
            Case::new(lit(1) + lit("bye")).invalid(),
            Case::new(lit(true) + lit(1)).invalid(),
            Case::new(lit(1) + lit(true)).invalid(),
            Case::new(lit(true) + lit(false)).invalid(),
            // invalid addition using refs
            Case::new(col("a") + lit("bye")).invalid(),
            Case::new(col("a") + col("d")).invalid(),
            Case::new(col("a") + col("e")).invalid(),
            Case::new(col("a") + col("f")).invalid(),
            // addition of two integers or two floats
            Case::new(lit(1) + lit(2))
                .matches(Int, [3, 3, 3])
                .matches(Float, [3.0, 3.0, 3.0])
                .matches(Type::optional(Int), [Some(3), Some(3), Some(3)])
                .matches(Type::optional(Float), [Some(3.0), Some(3.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp, Date])
                .mismatches([Struct(Box::new(
                    types::StructType::new(
                        "somename".into(),
                        vec![Field::new("a".to_string(), Int)],
                    )
                    .unwrap(),
                ))])
                .mismatches([Decimal(types::DecimalType::new(5).unwrap())]),
            Case::new(lit(1.0) + lit(2.0))
                .matches(Float, [3.0, 3.0, 3.0])
                .matches(Type::optional(Float), [Some(3.0), Some(3.0), Some(3.0)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            Case::new(col("a") + lit(2))
                .matches(Int, [3, 4, 5])
                .matches(Float, [3.0, 4.0, 5.0])
                .matches(Type::optional(Int), [Some(3), Some(4), Some(5)])
                .matches(Type::optional(Float), [Some(3.0), Some(4.0), Some(5.0)])
                .mismatches([String, Bool, Timestamp, Date]),
            Case::new(col("a") + col("a"))
                .matches(Int, [2, 4, 6])
                .matches(Float, [2.0, 4.0, 6.0])
                .matches(Type::optional(Int), [Some(2), Some(4), Some(6)])
                .matches(Type::optional(Float), [Some(2.0), Some(4.0), Some(6.0)])
                .mismatches([String, Bool, Timestamp, Date]),
            Case::new(col("a") + col("b"))
                .matches(Int, [12, 14, 16])
                .matches(Type::optional(Int), [Some(12), Some(14), Some(16)])
                .matches(Float, [12.0, 14.0, 16.0])
                .matches(Type::optional(Float), [Some(12.0), Some(14.0), Some(16.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(lit(1.5) + col("c"))
                .matches(Float, [2.6, 3.6, 4.6])
                .matches(Type::optional(Float), [Some(2.6), Some(3.6), Some(4.6)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            // mix of int and float
            Case::new(lit(1) + lit(2.1))
                .matches(Float, [3.1, 3.1, 3.1])
                .matches(Type::optional(Float), [Some(3.1), Some(3.1), Some(3.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            Case::new(col("a") + col("c"))
                .matches(Float, [2.1, 4.1, 6.1])
                .matches(Type::optional(Float), [Some(2.1), Some(4.1), Some(6.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int)]),
            // mix of int and float with optional
            Case::new(col("a") + col("g"))
                .matches(Optional(Box::new(Int)), [Some(2), None, Some(6)])
                .matches(Optional(Box::new(Float)), [Some(2.0), None, Some(6.0)])
                .mismatches([Float, String, Bool, Timestamp, Int]),
            Case::new(col("c") + col("g"))
                .matches(Optional(Box::new(Float)), [Some(2.1), None, Some(6.1)])
                .mismatches([Int, String, Bool, Timestamp, Type::optional(Int), Float]),
            // cases involving none literal
            Case::new(col("a") + lit(Value::None))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, Bool, String, Timestamp, Date]),
            Case::new(lit(Value::None) + lit(Value::None))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([
                    Int,
                    Float,
                    Bool,
                    String,
                    Timestamp,
                    Date,
                    Type::optional(Bool),
                    Type::optional(String),
                ]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_logical_and_or() {
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("c", Type::Float, vec![1.1, 2.1, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["hi", "bye", "something"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Bool)),
                vec![Some(true), None, Some(false)],
            )
            .unwrap(),
        ])
        .unwrap();
        let and_cases = [
            //invalids ands
            Case::new(col("a").and(col("c"))).invalid(),
            Case::new(col("a").and(col("d"))).invalid(),
            Case::new(col("a").and(col("e"))).invalid(),
            Case::new(col("a").and(col("f"))).invalid(),
            Case::new(col("a").and(col("g"))).invalid(),
            // valid ands
            Case::new(col("e").and(lit(true)))
                .matches(Bool, [true, false, true])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(col("e").and(lit(false)))
                .matches(Bool, [false, false, false])
                .matches(
                    Type::optional(Bool),
                    [Some(false), Some(false), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(col("e").and(lit(true)))
                .matches(Bool, [true, false, true])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
            // involving optional
            Case::new(lit(Value::None).and(lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(col("e").and(lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [None, Some(false), None])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(col("e").and(col("g")))
                .matches(
                    Optional(Box::new(Bool)),
                    [Some(true), Some(false), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(not(col("e")).and(col("g")))
                .matches(Optional(Box::new(Bool)), [Some(false), None, Some(false)])
                .mismatches([Int, Float, String, Timestamp, Bool]),
        ];
        let or_cases = [
            //invalids ors
            Case::new(col("a").or(col("c"))).invalid(),
            Case::new(col("a").or(col("d"))).invalid(),
            Case::new(col("a").or(col("e"))).invalid(),
            Case::new(col("a").or(col("f"))).invalid(),
            Case::new(col("a").or(col("g"))).invalid(),
            // valid ors
            Case::new(col("e").or(lit(true)))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(col("e").or(lit(false)))
                .matches(Bool, [true, false, true])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(col("e").or(col("e")))
                .matches(Bool, [true, false, true])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
            // involving optional
            Case::new(lit(Value::None).or(lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(col("e").or(lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [Some(true), None, Some(true)])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(col("e").or(col("g")))
                .matches(Optional(Box::new(Bool)), [Some(true), None, Some(true)])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(not(col("e")).or(col("g")))
                .matches(
                    Optional(Box::new(Bool)),
                    [Some(true), Some(true), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp, Bool]),
        ];
        let cases = and_cases
            .iter()
            .chain(or_cases.iter())
            .cloned()
            .collect_vec();
        check(df, cases);
    }

    #[test]
    fn test_complex_expr() {
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13]).unwrap(),
            Col::from("c", Type::Float, vec![1.1, 2.1, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["hi", "bye", "something"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            Case::new(col("a").and(col("b")).or(col("c"))).invalid(),
            Case::new(col("a").neq(col("g")).and(lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [Some(false), None, Some(false)])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(col("a").add(lit(10)).eq(col("b")))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(col("b").sub(col("c").add(col("g"))))
                .matches(Type::optional(Float), [Some(8.9), None, Some(6.9)])
                .mismatches([Int, Bool, Timestamp, Float, Type::optional(Int)]),
            Case::new(col("a").gt(col("b").sub(col("a"))))
                .matches(Bool, [false, false, false])
                .matches(
                    Type::optional(Bool),
                    [Some(false), Some(false), Some(false)],
                )
                .mismatches([Float, String, Timestamp]),
            Case::new(col("a") + col("b") - col("g"))
                .matches(
                    Type::optional(Int),
                    [Some(1 + 11 - 1), None, Some(3 + 13 - 3)],
                )
                .matches(
                    Type::optional(Float),
                    [Some(1.0 + 11.0 - 1.0), None, Some(3.0 + 13.0 - 3.0)],
                )
                .mismatches([Float, String, Bool, Timestamp, Int]),
            Case::new(lit(Value::None).gte(lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Timestamp, Bool]),
            Case::new(lit(Value::None).gte(lit(Value::None) + lit(Value::None)))
                .matches(Optional(Box::new(Bool)), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Timestamp, Bool]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_eq_neq() {
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13]).unwrap(),
            Col::from("c", Type::Float, vec![1.0, 2.0, 3.1]).unwrap(),
            Col::from("d", Type::String, vec!["1", "1.0", "hi"]).unwrap(),
            Col::from("e", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("f", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "g",
                Type::Optional(Box::new(Type::Int)),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();
        let b = binary;
        use BinOp::*;
        // we will now rewrite/improve cases below to use Case2
        let eq_cases = [
            // invalid equality
            Case::new(b(lit(1), Eq, lit("1"))).invalid(),
            Case::new(b(col("a"), Eq, col("d"))).invalid(),
            Case::new(b(col("a"), Eq, col("e"))).invalid(),
            Case::new(b(col("d"), Eq, col("e"))).invalid(),
            // basic equality in same type
            Case::new(b(lit(1), Eq, lit(Value::None)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(b(lit(Value::None), Eq, lit(Value::None)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(b(lit(1), Eq, lit(1)))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(lit(1), Eq, lit(2)))
                .matches(Bool, [false, false, false])
                .matches(
                    Type::optional(Bool),
                    [Some(false), Some(false), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(col("a"), Eq, lit(1)))
                .matches(Bool, [true, false, false])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(false)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(lit(3.1), Eq, col("c")))
                .matches(Bool, [false, false, true])
                .matches(Type::optional(Bool), [Some(false), Some(false), Some(true)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(lit("1"), Eq, col("d")))
                .matches(Bool, [true, false, false])
                .matches(Type::optional(Bool), [Some(true), Some(false), Some(false)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(lit(false), Eq, col("e")))
                .matches(Bool, [false, true, false])
                .matches(Type::optional(Bool), [Some(false), Some(true), Some(false)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(col("a"), Eq, col("a")))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            Case::new(b(col("a"), Eq, col("b")))
                .matches(Bool, [false, false, false])
                .matches(
                    Type::optional(Bool),
                    [Some(false), Some(false), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp, Type::optional(Int)]),
            // int / float equality
            Case::new(b(lit(1), Eq, lit(1.0)))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Float)]),
            Case::new(b(col("a"), Eq, col("c")))
                .matches(Bool, [true, true, false])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(false)])
                .mismatches([Int, Float, String, Timestamp, Type::optional(Float)]),
            // optional cases
            Case::new(b(col("a"), Eq, col("g")))
                .matches(Optional(Box::new(Bool)), [Some(true), None, Some(true)])
                .matches(Optional(Box::new(Bool)), [Some(true), None, Some(true)])
                .mismatches([Int, Float, String, Timestamp, Bool, Type::optional(Int)]),
            Case::new(b(col("c"), Eq, col("g")))
                .matches(Optional(Box::new(Bool)), [Some(true), None, Some(false)])
                .mismatches([Int, Float, String, Timestamp, Bool])
                .mismatches([Type::optional(Int)]),
        ];

        let mut cases = vec![];
        for case in eq_cases {
            cases.push(case.clone());
            // now convert to neq case
            // expr changes from Eq op to Neq op and matches change from true to false
            let mut new = case.clone();
            new.expr = match new.expr {
                Expr::Binary { left, right, .. } => Expr::Binary {
                    op: Neq,
                    left,
                    right,
                },
                _ => unreachable!("expected binary expr"),
            };
            for (_, values) in new.matches.iter_mut() {
                for v in values.iter_mut() {
                    match v {
                        Value::Bool(b) => *b = !*b,
                        _ => {}
                    }
                }
            }
            cases.push(new);
        }
        check(df, cases);
    }

    #[test]
    fn test_isnull() {
        let df = default_df();
        let cases = [
            // never invalid, even if the column is not nullable
            Case::new(isnull(lit(1)))
                .matches(Bool, [false, false, false])
                .matches(
                    Type::optional(Bool),
                    [Some(false), Some(false), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(isnull(col("a")))
                .matches(Bool, [false, false, false])
                .matches(
                    Type::optional(Bool),
                    [Some(false), Some(false), Some(false)],
                )
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(isnull(col("g")))
                .matches(Bool, [false, true, false])
                .matches(Type::optional(Bool), [Some(false), Some(true), Some(false)])
                .mismatches([Int, Float, String, Timestamp]),
            Case::new(isnull(lit(Value::None)))
                .matches(Bool, [true, true, true])
                .matches(Type::optional(Bool), [Some(true), Some(true), Some(true)])
                .mismatches([Int, Float, String, Timestamp]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_fillnull() {
        let df = default_df();
        let cases = [
            // invalid when expr/default aren't compatible
            Case::new(fillnull(lit(1), lit("1"))).invalid(),
            Case::new(fillnull(col("a"), lit("1"))).invalid(),
            Case::new(fillnull(lit(1), col("d"))).invalid(),
            // valid cases
            Case::new(fillnull(lit(1), lit(2)))
                .matches(Int, [1, 1, 1])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(fillnull(col("a"), lit(2)))
                .matches(Int, [1, 2, 3])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(fillnull(col("g"), col("a")))
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(fillnull(col("g"), lit(3.3)))
                .matches(Float, [1.0, 3.3, 3.0])
                .matches(Type::optional(Float), [Some(1.0), Some(3.3), Some(3.0)])
                .mismatches([Int, String, Bool, Timestamp]),
            Case::new(fillnull(lit(Value::None), lit(1)))
                .matches(Int, [1, 1, 1])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(fillnull(col("g"), lit(Value::None)))
                .matches(Optional(Box::new(Int)), [Some(1), None, Some(3)])
                .matches(Optional(Box::new(Float)), [Some(1.0), None, Some(3.0)])
                .mismatches([Float, Bool, Timestamp, Int, Type::optional(Bool)]),
            Case::new(fillnull(lit(Value::None), lit(Value::None)))
                .matches(Optional(Box::new(Int)), [None::<Value>, None, None])
                .matches(Optional(Box::new(Float)), [None::<Value>, None, None])
                .matches(Optional(Box::new(Bool)), [None::<Value>, None, None])
                .mismatches([String, Bool, Timestamp, Int]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_when() {
        let df = default_df();
        let cases = [
            // some invalid cases
            // when doesn't evaluate to bool
            Case::new(when(lit(1), lit("one"))).invalid(),
            Case::new(when(col("a"), lit("one"))).invalid(),
            // two whens, oen of them invalid
            Case::new(when(col("a").eq(lit(1)), lit("one")).when(lit(1), lit("one"))).invalid(),
            // then/otherwise can not be promoted to the same type
            Case::new(when(col("a").eq(lit(1)), lit("one")).otherwise(lit(1))).invalid(),
            // valid cases
            Case::new(when(col("a").eq(lit(1)), lit("one")).otherwise(lit("not one")))
                .matches(String, ["one", "not one", "not one"])
                .matches(
                    Type::optional(String),
                    [Some("one"), Some("not one"), Some("not one")],
                )
                .mismatches([Int, Float, Bool, Timestamp]),
            Case::new(when(lit(true), col("d")).otherwise(lit("not one")))
                .matches(String, ["1", "1.0", "Hi"])
                .mismatches([Int, Float, Bool, Timestamp]),
            Case::new(when(lit(Value::None), lit(1)).otherwise(lit(2.0)))
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([Int, String, Bool, Timestamp]),
            // without otherwise
            Case::new(when(col("a").eq(lit(1)), lit("one")))
                .matches(Type::optional(String), [Some("one"), None, None])
                .mismatches([String, Int, Float, Bool, Timestamp]),
            // multiple when
            Case::new(
                when(col("a").eq(lit(1)), lit("one"))
                    .when(col("a").eq(lit(2)), lit("two"))
                    .otherwise(lit("not one or two")),
            )
            .matches(String, ["one", "two", "not one or two"])
            .matches(
                Type::optional(String),
                [Some("one"), Some("two"), Some("not one or two")],
            )
            .mismatches([Int, Float, Bool, Timestamp]),
            // two thens, both with nones
            Case::new(
                when(col("a").eq(lit(1)), lit(Value::None))
                    .when(col("a").eq(lit(2)), lit(Value::None)),
            )
            .matches(Type::optional(Int), [None::<Value>, None, None]),
            // two thens, one int, one float without otherwise
            Case::new(when(col("a").eq(lit(1)), lit(1)).when(col("a").eq(lit(2)), lit(2.0)))
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), None])
                .mismatches([Int, String, Bool, Float]),
            // three whens + otherwise, each having condition on different column
            Case::new(
                when(col("a").eq(lit(1)), lit("one"))
                    .when(col("b").eq(lit(12)), lit("twelve"))
                    .when(col("c").eq(lit(3.1)), lit("three point one"))
                    .otherwise(lit(Value::None)),
            )
            .matches(
                Type::optional(String),
                [Some("one"), Some("twelve"), Some("three point one")],
            )
            .mismatches([Int, Float, Bool, String]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_lca() {
        let cases = [
            (Int, Int, Some(Int)),
            (Int, Float, Some(Float)),
            (Int, String, None),
            (Int, Type::optional(Int), Some(Type::optional(Int))),
            (Int, Type::optional(Float), Some(Type::optional(Float))),
            (Type::optional(Int), Float, Some(Type::optional(Float))),
            (Int, Type::Optional(Box::new(String)), None),
            (Int, Bool, None),
            (Null, Int, Some(Type::optional(Int))),
            (Null, Float, Some(Type::optional(Float))),
            (Null, Type::optional(Int), Some(Type::optional(Int))),
            (Null, Type::optional(Float), Some(Type::optional(Float))),
            (Null, String, Some(Type::optional(String))),
            (Null, Type::optional(String), Some(Type::optional(String))),
            (Null, Bool, Some(Type::optional(Bool))),
            (Null, Type::optional(Bool), Some(Type::optional(Bool))),
            (Null, Null, Some(Null)),
            (
                Type::Struct(Box::new(
                    types::StructType::new(
                        "somename".into(),
                        vec![Field::new("a".to_string(), Int)],
                    )
                    .unwrap(),
                )),
                Type::Struct(Box::new(
                    types::StructType::new("anon".into(), vec![Field::new("a".to_string(), Int)])
                        .unwrap(),
                )),
                Some(Type::Struct(Box::new(
                    types::StructType::new("anon".into(), vec![Field::new("a".to_string(), Int)])
                        .unwrap(),
                ))),
            ),
        ];
        for (t1, t2, expected) in cases {
            let t1 = Type::from(t1);
            let t2 = Type::from(t2);
            let expected = expected.map(Type::from);
            assert_eq!(lca(&t1, &t2), expected, "lca({}, {}) failed", t1, t2);
            assert_eq!(lca(&t2, &t1), expected, "lca({}, {}) failed", t2, t1);
        }
    }

    #[test]
    fn test_struct_type_promotion() {
        use itertools::iproduct;

        let s1 = Type::Struct(Box::new(
            types::StructType::new(
                "s1".into(),
                vec![
                    Field::new("a".to_string(), Int),
                    Field::new("b".to_string(), Float),
                ],
            )
            .unwrap(),
        ));
        let s2 = Type::Struct(Box::new(
            types::StructType::new(
                "s2".into(),
                vec![
                    Field::new("a".to_string(), Int),
                    Field::new("b".to_string(), Float),
                ],
            )
            .unwrap(),
        ));
        // Different set of fields
        let s3 = Type::Struct(Box::new(
            types::StructType::new(
                "s3".into(),
                vec![
                    Field::new("a".to_string(), Int),
                    Field::new("b".to_string(), Float),
                    Field::new("c".to_string(), String),
                ],
            )
            .unwrap(),
        ));
        // Inverse order of fields
        let s4 = Type::Struct(Box::new(
            types::StructType::new(
                "s4".into(),
                vec![
                    Field::new("b".to_string(), Float),
                    Field::new("a".to_string(), Int),
                ],
            )
            .unwrap(),
        ));
        let similar_structs = vec![s1.clone(), s2.clone(), s4.clone()];
        let different_structs = vec![s3.clone()];
        for (a, b) in iproduct!(similar_structs.iter(), similar_structs.iter()) {
            assert!(promotable(a, b));
        }

        for (a, b) in iproduct!(similar_structs.iter(), different_structs.iter()) {
            assert!(!promotable(a, b));
        }
    }

    #[test]
    fn test_math_to_string() {
        let df = default_df();
        let cases = [
            Case::new(to_string(lit(1))).produces(String, ["1", "1", "1"]),
            // TODO: this test fails - debug and enable
            // Case::new(to_string(lit(1.232))).produces(String, ["1.232", "1.232", "1.232"]),
            Case::new(to_string(lit(Value::None)))
                .produces(Type::optional(String), [None::<Value>, None, None]),
            Case::new(to_string(col("a"))).produces(Type::String, ["1", "2", "3"]),
            Case::new(to_string(col("b"))).produces(Type::String, ["11", "12", "13"]),
            Case::new(to_string(col("c"))).produces(Type::String, ["1.0", "2.0", "3.1"]),
            Case::new(to_string(col("g")))
                .produces(Type::optional(Type::String), [Some("1"), None, Some("3")]),
            Case::new(to_string(col("h"))).produces(Type::String, ["-1.0", "2.0", "-3.1"]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_floor_ceil() {
        let df = default_df();
        let cases = [
            Case::new(floor(col("a")))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(ceil(col("a")))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(floor(lit(1.1)))
                .matches(Int, [1, 1, 1])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(ceil(lit(1.1)))
                .matches(Int, [2, 2, 2])
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Int), [Some(2), Some(2), Some(2)])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(floor(lit(2)))
                .matches(Int, [2, 2, 2])
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Int), [Some(2), Some(2), Some(2)])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(ceil(lit(2)))
                .matches(Int, [2, 2, 2])
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Int), [Some(2), Some(2), Some(2)])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(floor(lit(Value::None)))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            Case::new(ceil(lit(Value::None)))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            Case::new(floor(lit(1.3) + lit(1.1) + lit(Value::None)))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            Case::new(ceil(lit(1.3) + lit(1.1) + lit(Value::None)))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            Case::new(floor(lit(2.1) - lit(5)))
                .matches(Int, [-3, -3, -3])
                .matches(Float, [-3.0, -3.0, -3.0])
                .matches(Type::optional(Int), [Some(-3), Some(-3), Some(-3)])
                .matches(Type::optional(Float), [Some(-3.0), Some(-3.0), Some(-3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(ceil(lit(2.1) - lit(5)))
                .matches(Int, [-2, -2, -2])
                .matches(Float, [-2.0, -2.0, -2.0])
                .matches(Type::optional(Int), [Some(-2), Some(-2), Some(-2)])
                .matches(Type::optional(Float), [Some(-2.0), Some(-2.0), Some(-2.0)])
                .mismatches([String, Bool, Timestamp]),
            // some columns
            Case::new(floor(col("c")))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(ceil(col("c")))
                .matches(Int, [1, 2, 4])
                .matches(Float, [1.0, 2.0, 4.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(4)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(4.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(floor(col("c") + col("a")))
                .matches(Int, [2, 4, 6])
                .matches(Float, [2.0, 4.0, 6.0])
                .matches(Type::optional(Int), [Some(2), Some(4), Some(6)])
                .matches(Type::optional(Float), [Some(2.0), Some(4.0), Some(6.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(ceil(col("c") + col("a")))
                .matches(Int, [2, 4, 7])
                .matches(Float, [2.0, 4.0, 7.0])
                .matches(Type::optional(Int), [Some(2), Some(4), Some(7)])
                .matches(Type::optional(Float), [Some(2.0), Some(4.0), Some(7.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(floor(col("c") + col("g")))
                .matches(Optional(Box::new(Int)), [Some(2), None, Some(6)])
                .matches(Optional(Box::new(Float)), [Some(2.0), None, Some(6.0)])
                .mismatches([String, Bool, Timestamp, Int, Float]),
            Case::new(ceil(col("c") + col("g")))
                .matches(Optional(Box::new(Int)), [Some(2), None, Some(7)])
                .matches(Optional(Box::new(Float)), [Some(2.0), None, Some(7.0)])
                .mismatches([String, Bool, Timestamp, Int, Float]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_round() {
        let df = default_df();
        let cases = [
            Case::new(round(lit(1.1)))
                .matches(Int, [1, 1, 1])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(roundto(lit(1.1), 0))
                .matches(Int, [1, 1, 1])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(roundto(lit(1.1), 1))
                .matches(Float, [1.1, 1.1, 1.1])
                .matches(Type::optional(Float), [Some(1.1), Some(1.1), Some(1.1)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(round(lit(1.5)))
                .matches(Int, [2, 2, 2])
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Int), [Some(2), Some(2), Some(2)])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(roundto(lit(1.5), 0))
                .matches(Int, [2, 2, 2])
                .matches(Float, [2.0, 2.0, 2.0])
                .matches(Type::optional(Int), [Some(2), Some(2), Some(2)])
                .matches(Type::optional(Float), [Some(2.0), Some(2.0), Some(2.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(roundto(lit(1.5), 1))
                .matches(Float, [1.5, 1.5, 1.5])
                .matches(Type::optional(Float), [Some(1.5), Some(1.5), Some(1.5)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(round(lit(Value::None)))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            Case::new(roundto(lit(Value::None), 1))
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            // some columns
            Case::new(round(col("a")))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(roundto(col("a"), 1))
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(round(col("c")))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(roundto(col("c"), 1))
                .matches(Float, [1.0, 2.0, 3.1])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.1)])
                .mismatches([String, Bool, Timestamp]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_abs() {
        let df = default_df();
        let cases = [
            Case::new(abs(lit(1)))
                .matches(Int, [1, 1, 1])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(abs(lit(-1)))
                .matches(Int, [1, 1, 1])
                .matches(Float, [1.0, 1.0, 1.0])
                .matches(Type::optional(Int), [Some(1), Some(1), Some(1)])
                .matches(Type::optional(Float), [Some(1.0), Some(1.0), Some(1.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(abs(lit(1.1)))
                .matches(Float, [1.1, 1.1, 1.1])
                .matches(Type::optional(Float), [Some(1.1), Some(1.1), Some(1.1)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(abs(lit(-1.1)))
                .matches(Float, [1.1, 1.1, 1.1])
                .matches(Type::optional(Float), [Some(1.1), Some(1.1), Some(1.1)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(abs(lit(Value::None)))
                .matches(Type::optional(Int), [None::<Value>, None, None])
                .matches(Type::optional(Float), [None::<Value>, None, None])
                .mismatches([Int, Float, String, Bool, Timestamp]),
            // some columns
            Case::new(abs(col("a")))
                .matches(Int, [1, 2, 3])
                .matches(Float, [1.0, 2.0, 3.0])
                .matches(Type::optional(Int), [Some(1), Some(2), Some(3)])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.0)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(abs(col("c")))
                .matches(Float, [1.0, 2.0, 3.1])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.1)])
                .mismatches([String, Bool, Timestamp]),
            Case::new(abs(col("h")))
                .matches(Float, [1.0, 2.0, 3.1])
                .matches(Type::optional(Float), [Some(1.0), Some(2.0), Some(3.1)])
                .mismatches([String, Bool, Timestamp]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_struct_field_access_advanced() {
        // create customer df - with one struct column, one optional
        // struct column. Both structs should have an optional field
        let stype1 = Struct(Box::new(
            types::StructType::new(
                "stype1".into(),
                vec![
                    Field::new("int", Int),
                    Field::new("float", Float),
                    Field::new("string", String),
                    Field::new("optional_int", Type::optional(Int)),
                ],
            )
            .unwrap(),
        ));
        let stype2 = Type::optional(stype1.clone());
        let df = Dataframe::new(vec![
            Col::new(
                Arc::new(Field::new("a", stype1.clone())),
                Arc::new(vec![
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("int".into(), Value::Int(1)),
                            ("float".into(), Value::Float(1.0)),
                            ("string".into(), Value::String(Arc::new("hi".to_string()))),
                            ("optional_int".into(), Value::None),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("int".into(), Value::Int(2)),
                            ("float".into(), Value::Float(2.0)),
                            ("string".into(), Value::String(Arc::new("bye".to_string()))),
                            ("optional_int".into(), Value::Int(3)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("int".into(), Value::Int(3)),
                            ("float".into(), Value::Float(3.0)),
                            (
                                "string".into(),
                                Value::String(Arc::new("something".to_string())),
                            ),
                            ("optional_int".into(), Value::Int(4)),
                        ])
                        .unwrap(),
                    )),
                ]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b", stype2.clone())),
                Arc::new(vec![
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("int".into(), Value::Int(1)),
                            ("float".into(), Value::Float(1.0)),
                            ("string".into(), Value::String(Arc::new("hi".to_string()))),
                            ("optional_int".into(), Value::None),
                        ])
                        .unwrap(),
                    )),
                    Value::None,
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("int".into(), Value::Int(3)),
                            ("float".into(), Value::Float(3.0)),
                            (
                                "string".into(),
                                Value::String(Arc::new("something".to_string())),
                            ),
                            ("optional_int".into(), Value::Int(4)),
                        ])
                        .unwrap(),
                    )),
                ]),
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            Case::new(col("a").dot("int")).produces(Int, [1, 2, 3]),
            Case::new(col("a").dot("float")).produces(Float, [1.0, 2.0, 3.0]),
            Case::new(col("a").dot("string")).produces(String, ["hi", "bye", "something"]),
            Case::new(col("a").dot("optional_int"))
                .produces(Type::optional(Int), [None, Some(3), Some(4)]),
            Case::new(col("b").dot("int")).produces(Type::optional(Int), [Some(1), None, Some(3)]),
            Case::new(col("b").dot("float"))
                .produces(Type::optional(Float), [Some(1.0), None, Some(3.0)]),
            Case::new(col("b").dot("string")).produces(
                Type::optional(String),
                [Some("hi"), None, Some("something")],
            ),
            Case::new(col("b").dot("optional_int"))
                .produces(Type::optional(Int), [None, None, Some(4)]),
            // invalid with missing field
            Case::new(col("a").dot("random")).invalid(),
            Case::new(col("b").dot("random")).invalid(),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_len() {
        let df = list_df();
        let cases = [
            Case::new(col("a").list_len()).produces(Int, [2, 1, 2]),
            Case::new(col("b").list_len()).produces(Type::optional(Int), [Some(2), None, Some(2)]),
            Case::new(col("c").list_len()).produces(Int, [3, 3, 3]),
            Case::new(col("d").list_len()).invalid(),
            Case::new(col("random").list_len()).invalid(),
            Case::new(lit(Value::None).list_len())
                .produces(Type::optional(Int), [None::<Value>, None, None]),
            Case::new(lit("hi").list_len()).invalid(),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_contains() {
        let df = list_df();
        let cases = [
            Case::new(col("a").list_has(lit(1))).produces(Bool, [true, false, false]),
            Case::new(col("a").list_has(lit(3))).produces(Bool, [false, true, false]),
            Case::new(col("a").list_has(lit(1.0))).invalid(),
            Case::new(col("a").list_has(lit("hi"))).invalid(),
            Case::new(col("a").list_has(lit(Value::None))).invalid(),
            Case::new(col("b").list_has(lit(1)))
                .produces(Type::optional(Bool), [Some(true), None, Some(false)]),
            Case::new(col("b").list_has(lit(Value::None))).invalid(),
            Case::new(col("c").list_has(lit(1)))
                .produces(Type::optional(Bool), [Some(true), Some(false), None]),
            Case::new(col("c").list_has(lit(Value::None)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(col("e").list_has(lit(1)))
                .produces(Type::optional(Bool), [Some(true), None, Some(false)]),
            Case::new(lit(Value::None).list_has(lit(1)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(col("empty_optional").list_has(lit(Value::None)))
                .produces(Type::optional(Bool), [Some(false), None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_index_access() {
        let df = list_df();
        let cases = [
            Case::new(col("a").list_at(lit(Value::None))).invalid(),
            Case::new(col("a").list_at(lit(1.0))).invalid(),
            Case::new(col("a").list_at(lit(0)))
                .produces(Type::optional(Int), [Some(1), Some(3), Some(4)]),
            Case::new(col("a").list_at(lit(1)))
                .produces(Type::optional(Int), [Some(2), None, Some(5)]),
            Case::new(col("b").list_at(lit(0)))
                .produces(Type::optional(Int), [Some(1), None, Some(4)]),
            Case::new(col("b").list_at(lit(1)))
                .produces(Type::optional(Int), [Some(2), None, Some(5)]),
            Case::new(col("c").list_at(lit(0)))
                .produces(Type::optional(Int), [Some(1), Some(11), Some(3)]),
            Case::new(col("c").list_at(lit(1)))
                .produces(Type::optional(Int), [None, Some(Value::Int(4)), None]),
            Case::new(col("d").list_at(lit(0))).invalid(),
            Case::new(col("d").list_at(lit(1))).invalid(),
            Case::new(col("b").list_at(col("d")))
                .produces(Type::optional(Int), [Some(1), None, None]),
            // invalid with missing field
            Case::new(col("random").list_at(lit(2))).invalid(),
            Case::new(lit(Value::None).list_at(lit(2))).invalid(),
        ];
        check(df, cases);
    }

    #[test]
    fn test_struct_field_access() {
        let struct_dtype = StructType::new(
            "struct1".into(),
            vec![
                Field::new("a", Int),
                Field::new("b", Float),
                Field::new("c", String),
                Field::new("d", Bool),
                Field::new("none", Type::optional(Int)),
            ],
        )
        .unwrap();
        let struct_lit1 = Value::Struct(Arc::new(
            value::Struct::new(vec![
                ("a".into(), Value::Int(1)),
                ("b".into(), Value::Float(1.0)),
                ("c".into(), Value::String(Arc::new("one".to_string()))),
                ("d".into(), Value::Bool(true)),
                ("none".into(), Value::None),
            ])
            .unwrap(),
        ));
        let struct_lit2 = Value::Struct(Arc::new(
            value::Struct::new(vec![
                ("a".into(), Value::Int(2)),
                ("b".into(), Value::Float(2.0)),
                ("c".into(), Value::String(Arc::new("two".to_string()))),
                ("d".into(), Value::Bool(false)),
                ("none".into(), Value::Int(2)),
            ])
            .unwrap(),
        ));
        let struct_lit3 = Value::Struct(Arc::new(
            value::Struct::new(vec![
                ("a".into(), Value::Int(3)),
                ("b".into(), Value::Float(3.0)),
                ("c".into(), Value::String(Arc::new("three".to_string()))),
                ("d".into(), Value::Bool(false)),
                ("none".into(), Value::None),
            ])
            .unwrap(),
        ));
        let df = Dataframe::new(vec![
            Col::from("int", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("float", Type::Float, vec![1.0, 2.0, 3.1]).unwrap(),
            Col::from("str", Type::String, vec!["1", "1.0", "Hi"]).unwrap(),
            Col::from("bool", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("decimal", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "struct",
                Type::Struct(Box::new(struct_dtype.clone())),
                vec![
                    struct_lit1.clone(),
                    struct_lit2.clone(),
                    struct_lit3.clone(),
                ],
            )
            .unwrap(),
            Col::from(
                "opt_struct",
                Type::optional(Type::Struct(Box::new(struct_dtype.clone()))),
                vec![struct_lit1.clone(), Value::None, Value::None],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            Case::new(lit(1).dot("a")).invalid(),
            Case::new(lit("hi").dot("a")).invalid(),
            Case::new(lit(Value::None).dot("a")).invalid(),
            Case::new(lit(true).dot("a")).invalid(),
            Case::new(lit(struct_lit1.clone()).dot("a")).produces(Int, [1, 1, 1]),
            Case::new(lit(struct_lit1.clone()).dot("b")).produces(Float, [1.0, 1.0, 1.0]),
            Case::new(lit(struct_lit1.clone()).dot("c")).produces(String, ["one", "one", "one"]),
            Case::new(lit(struct_lit1.clone()).dot("d")).produces(Bool, [true, true, true]),
            Case::new(lit(struct_lit1.clone()).dot("none"))
                .produces(Null, [None::<Value>, None, None]),
            // now with some columns
            Case::new(col("a").dot("int")).invalid(),
            Case::new(col("b").dot("int")).invalid(),
            Case::new(col("c").dot("int")).invalid(),
            Case::new(col("d").dot("int")).invalid(),
            Case::new(col("e").dot("int")).invalid(),
            // now some valid cases
            Case::new(col("struct").dot("a")).produces(Int, [1, 2, 3]),
            Case::new(col("struct").dot("b")).produces(Float, [1.0, 2.0, 3.0]),
            Case::new(col("struct").dot("c")).produces(String, ["one", "two", "three"]),
            Case::new(col("struct").dot("d")).produces(Bool, [true, false, false]),
            Case::new(col("struct").dot("none"))
                .produces(Type::optional(Int), [None, Some(2), None]),
            Case::new(col("struct").dot("random")).invalid(),
            // complex expression with multiple fields
            Case::new(col("struct").dot("a") + col("struct").dot("b"))
                .produces(Float, [2.0, 4.0, 6.0]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_has_null() {
        let df = list_df();
        let cases = [
            Case::new(col("a").list_has_null()).produces(Bool, [false, false, false]),
            Case::new(col("b").list_has_null())
                .produces(Type::optional(Bool), [Some(false), None, Some(false)]),
            Case::new(col("c").list_has_null()).produces(Bool, [true, false, true]),
            Case::new(col("e").list_has_null())
                .produces(Type::optional(Bool), [Some(false), None, Some(false)]),
            Case::new(col("f").list_has_null())
                .produces(Type::optional(Bool), [Some(true), None, Some(true)]),
            // list can only be list, not null
            Case::new(lit(Value::None).list_has_null())
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(lit("hi").list_has_null()).invalid(),
            Case::new(col("d").list_has_null()).invalid(),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_length() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_len()).invalid(),
            Case::new(lit(1.0).str_len()).invalid(),
            Case::new(lit(true).str_len()).invalid(),
            Case::new(lit("hi").str_len()).produces(Int, [2, 2, 2]),
            Case::new(col("a").str_len()).invalid(),
            Case::new(col("d").str_len()).produces(Int, [1, 3, 2]),
            Case::new(col("null_strings").str_len())
                .produces(Type::optional(Int), [Some(2), None, Some(3)]),
            Case::new(lit(Value::None).str_len())
                .produces(Type::optional(Int), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_to_upper() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_to_upper()).invalid(),
            Case::new(lit(1.0).str_to_upper()).invalid(),
            Case::new(lit(true).str_to_upper()).invalid(),
            Case::new(lit("hi").str_to_upper()).produces(String, ["HI", "HI", "HI"]),
            Case::new(col("a").str_to_upper()).invalid(),
            Case::new(col("d").str_to_upper()).produces(String, ["1", "1.0", "HI"]),
            Case::new(col("null_strings").str_to_upper())
                .produces(Type::optional(String), [Some("HI"), None, Some("BYE")]),
            Case::new(lit(Value::None).str_to_upper())
                .produces(Type::optional(String), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_to_lower() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_to_lower()).invalid(),
            Case::new(lit(1.0).str_to_lower()).invalid(),
            Case::new(lit(true).str_to_lower()).invalid(),
            Case::new(lit("Hi").str_to_lower()).produces(String, ["hi", "hi", "hi"]),
            Case::new(col("a").str_to_lower()).invalid(),
            Case::new(col("d").str_to_lower()).produces(String, ["1", "1.0", "hi"]),
            Case::new(col("null_strings").str_to_lower())
                .produces(Type::optional(String), [Some("hi"), None, Some("bye")]),
            Case::new(lit(Value::None).str_to_lower())
                .produces(Type::optional(String), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_contains() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_contains(lit("1"))).invalid(),
            Case::new(lit(1.0).str_contains(lit("1"))).invalid(),
            Case::new(lit(true).str_contains(lit("1"))).invalid(),
            Case::new(lit("1").str_contains(lit(1))).invalid(),
            Case::new(lit("Hello").str_contains(lit("ello"))).produces(Bool, [true, true, true]),
            Case::new(lit("Hello").str_contains(lit("yellow")))
                .produces(Bool, [false, false, false]),
            Case::new(lit("Hello").str_contains(lit(""))).produces(Bool, [true, true, true]),
            Case::new(lit("").str_contains(lit("yellow"))).produces(Bool, [false, false, false]),
            Case::new(lit("").str_contains(lit(""))).produces(Bool, [true, true, true]),
            Case::new(lit("hello").str_contains(lit(Value::None)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(col("a").str_contains(lit("i"))).invalid(),
            Case::new(col("d").str_contains(lit("i"))).produces(Bool, [false, false, true]),
            Case::new(col("d").str_contains(lit("1"))).produces(Bool, [true, true, false]),
            Case::new(col("null_strings").str_contains(lit("i")))
                .produces(Type::optional(Bool), [Some(true), None, Some(false)]),
            Case::new(lit(Value::None).str_contains(lit("")))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_starts_with() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_starts_with(lit("1"))).invalid(),
            Case::new(lit(1.0).str_starts_with(lit("1"))).invalid(),
            Case::new(lit(true).str_starts_with(lit("1"))).invalid(),
            Case::new(lit("1").str_starts_with(lit(1))).invalid(),
            Case::new(lit("Hello").str_starts_with(lit("Hell"))).produces(Bool, [true, true, true]),
            Case::new(lit("Hello").str_starts_with(lit("Help")))
                .produces(Bool, [false, false, false]),
            Case::new(lit("Hello").str_starts_with(lit(""))).produces(Bool, [true, true, true]),
            Case::new(lit("").str_starts_with(lit("Help"))).produces(Bool, [false, false, false]),
            Case::new(lit("").str_starts_with(lit(""))).produces(Bool, [true, true, true]),
            Case::new(col("a").str_starts_with(lit("H"))).invalid(),
            Case::new(col("d").str_starts_with(lit("H"))).produces(Bool, [false, false, true]),
            Case::new(col("d").str_starts_with(lit("1"))).produces(Bool, [true, true, false]),
            Case::new(col("d").str_starts_with(lit(Value::None)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(col("null_strings").str_starts_with(lit("H")))
                .produces(Type::optional(Bool), [Some(true), None, Some(false)]),
            Case::new(lit("hello").str_starts_with(col("null_strings")))
                .produces(Type::optional(Bool), [Some(false), None, Some(false)]),
            Case::new(lit(Value::None).str_starts_with(lit("")))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_ends_with() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_ends_with(lit("1"))).invalid(),
            Case::new(lit(1.0).str_ends_with(lit("1"))).invalid(),
            Case::new(lit(true).str_ends_with(lit("1"))).invalid(),
            Case::new(lit("1").str_ends_with(lit(1))).invalid(),
            Case::new(lit("Hello").str_ends_with(lit("lo"))).produces(Bool, [true, true, true]),
            Case::new(lit("Hello").str_ends_with(lit("low"))).produces(Bool, [false, false, false]),
            Case::new(lit("Hello").str_ends_with(lit(""))).produces(Bool, [true, true, true]),
            Case::new(lit("").str_ends_with(lit("low"))).produces(Bool, [false, false, false]),
            Case::new(lit("").str_ends_with(lit(""))).produces(Bool, [true, true, true]),
            Case::new(col("a").str_ends_with(lit("i"))).invalid(),
            Case::new(col("d").str_ends_with(lit("i"))).produces(Bool, [false, false, true]),
            Case::new(col("d").str_ends_with(lit("1"))).produces(Bool, [true, false, false]),
            Case::new(col("d").str_ends_with(lit(Value::None)))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(col("null_strings").str_ends_with(lit("i")))
                .produces(Type::optional(Bool), [Some(true), None, Some(false)]),
            Case::new(lit("hello").str_ends_with(col("null_strings")))
                .produces(Type::optional(Bool), [Some(false), None, Some(false)]),
            Case::new(lit(Value::None).str_ends_with(lit("")))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_strptime() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).strptime("%Y")).invalid(),
            Case::new(lit(1.0).strptime("%Y-%m-%d")).invalid(),
            Case::new(lit(true).strptime("%Y-%m-%d")).invalid(),
            // 1) on invalid format string, error is raised
            Case::new(lit("2019-01-01").strptime("%Y-%L")).invalid(),
            Case::new(lit("2019-01-01").strptime_tz("%Y-%m-%d", "random_tz")).invalid(),
            // as of now, we don't support parsing with timezone offsets
            Case::new(lit("2019-01-01").strptime_tz("%Y-%m-%d", "+05:00")).invalid(),
            // 2) on invalid timezone, error is raised
            Case::new(lit("2019-01-01").strptime("%Y-%m-%d")).produces(
                Timestamp,
                [
                    1546300800 * 1000_000,
                    1546300800 * 1000_000,
                    1546300800 * 1000_000,
                ],
            ),
            Case::new(lit("2019-01-01").strptime_tz("%Y-%m-%d", "UTC"))
                .produces(Timestamp, [1546300800 * 1000_000; 3]),
            // also test with timezone
            Case::new(lit("2019-01-01").strptime_tz("%Y-%m-%d", "America/New_York"))
                .produces(Timestamp, [1546300800 * 1000_000 + 5 * 3600 * 1000_000; 3]),
            Case::new(lit("2019-01-01T00:00:00Z").strptime("%Y-%m-%dT%H:%M:%SZ")).produces(
                Timestamp,
                [
                    1546300800 * 1000_000,
                    1546300800 * 1000_000,
                    1546300800 * 1000_000,
                ],
            ),
            Case::new(lit("2019-01-01T00:00:00+05:00").strptime("%Y-%m-%dT%H:%M:%S%:z"))
                .produces(Timestamp, [1546282800000000; 3]),
            // if provided timezone differs from the one in the string, one in the string is used
            Case::new(
                lit("2019-01-01T00:00:00+05:00")
                    .strptime_tz("%Y-%m-%-dT%H:%M:%S%:z", "Asia/Calcutta"),
            )
            .produces(Timestamp, [1546282800000000; 3]),
            // if string is null, result is null
            Case::new(lit(Value::None).strptime("%Y-%m-%d"))
                .produces(Type::optional(Timestamp), [None::<Value>, None, None]),
            // if string is optional, result is optional
            Case::new(col("null_dt_strings").strptime("%Y-%m-%d")).produces(
                Type::optional(Timestamp),
                [Some(1609459200000000), None, Some(1609545600000000)],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_concat() {
        let df = default_df();
        let cases = [
            Case::new(lit(1).str_concat(lit("1"))).invalid(),
            Case::new(lit(1.0).str_concat(lit("1"))).invalid(),
            Case::new(lit(true).str_concat(lit("1"))).invalid(),
            Case::new(lit("1").str_concat(lit(1))).invalid(),
            Case::new(lit("foo").str_concat(lit(Value::None)))
                .produces(Type::optional(String), [None::<Value>, None, None]),
            Case::new(lit("Hel").str_concat(lit("lo")))
                .produces(String, ["Hello", "Hello", "Hello"]),
            Case::new(lit("Hell").str_concat(lit(""))).produces(String, ["Hell", "Hell", "Hell"]),
            Case::new(lit("").str_concat(lit("low"))).produces(String, ["low", "low", "low"]),
            Case::new(lit(Value::None).str_concat(lit("bar")))
                .produces(Type::optional(String), [None::<Value>, None, None]),
            Case::new(col("a").str_concat(lit("??"))).invalid(),
            Case::new(col("d").str_concat(lit("??"))).produces(String, ["1??", "1.0??", "Hi??"]),
            Case::new(col("d").str_concat(col("null_strings")))
                .produces(Type::optional(String), [Some("1Hi"), None, Some("HiBye")]),
            Case::new(col("null_strings").str_concat(lit("!!")))
                .produces(Type::optional(String), [Some("Hi!!"), None, Some("Bye!!")]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_str_json_decode() {
        let df = Dataframe::new(vec![
            Col::from("int_strings", Type::String, vec!["1", "2", "3"]).unwrap(),
            Col::from("float_strings", Type::String, vec!["1.0", "2.0", "3.1"]).unwrap(),
            Col::from("str_strings", Type::String, vec!["\"1\"", "\"\"", "\"Hi\""]).unwrap(),
            Col::from("bool_strings", Type::String, vec!["true", "false", "true"]).unwrap(),
            Col::from(
                "ts_strings",
                Type::String,
                vec!["1609459200", "1609545600", "1609632000"],
            )
            .unwrap(),
            Col::from(
                "opt_int_strings",
                Type::optional(String),
                vec![Some("1"), None, Some("3")],
            )
            .unwrap(),
            Col::from(
                "opt_float_strings",
                Type::optional(String),
                vec![Some("1.0"), None, Some("3.1")],
            )
            .unwrap(),
            Col::from(
                "opt_str_strings",
                Type::optional(String),
                vec![Some("\"1\""), None, Some("\"Hi\"")],
            )
            .unwrap(),
            Col::from(
                "opt_bool_strings",
                Type::optional(String),
                vec![Some("true"), None, Some("true")],
            )
            .unwrap(),
            Col::from(
                "opt_ts_strings",
                Type::optional(String),
                vec![Some("1609459200"), None, Some("1609632000")],
            )
            .unwrap(),
            Col::from(
                "list_strings",
                String,
                vec![r#"[1, 2, 3]"#, r#"[4, 5]"#, r#"[]"#],
            )
            .unwrap(),
            Col::from(
                "opt_list_strings",
                Type::optional(String),
                vec![Some(r#"[1, 2, 3]"#), None, Some(r#"[]"#)],
            )
            .unwrap(),
            Col::from(
                "struct_strings",
                Type::String,
                vec![
                    r#"{"a": 1, "b": 2}"#,
                    r#"{"a": 3, "b": 4}"#,
                    r#"{"a": 5, "b": 6}"#,
                ],
            )
            .unwrap(),
            Col::from(
                "opt_struct_strings",
                Type::optional(String),
                vec![
                    Some(r#"{"a": 1, "b": 2}"#),
                    None,
                    Some(r#"{"a": 5, "b": 6}"#),
                ],
            )
            .unwrap(),
            Col::from(
                "nested_struct_strings",
                Type::String,
                vec![
                    r#"{"a": 1, "b": {"c": 2}}"#,
                    r#"{"a": 3, "b": {"c": 4}}"#,
                    r#"{"a": 5, "b": {"c": 6}}"#,
                ],
            )
            .unwrap(),
        ])
        .unwrap();
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "struct1".into(),
                vec![Field::new("a", Int), Field::new("b", Int)],
            )
            .unwrap(),
        ));
        let nested_struct_type = Type::Struct(Box::new(
            StructType::new(
                "struct1".into(),
                vec![
                    Field::new("a", Int),
                    Field::new(
                        "b",
                        Type::Struct(Box::new(
                            StructType::new("struct2".into(), vec![Field::new("c", Int)]).unwrap(),
                        )),
                    ),
                ],
            )
            .unwrap(),
        ));

        let cases = [
            // invalid strings
            Case::new(lit(1).str_json_decode(Int)).invalid(),
            Case::new(lit(1.0).str_json_decode(Int)).invalid(),
            Case::new(lit(true).str_json_decode(Int)).invalid(),
            Case::new(lit(Value::Timestamp(UTCTimestamp::from(1))).str_json_decode(Int)).invalid(),
            // basic valid cases
            Case::new(lit(r#"1"#).str_json_decode(Int)).produces(Int, [1, 1, 1]),
            Case::new(lit(r#"1"#).str_json_decode(Timestamp)).produces(Timestamp, [1, 1, 1]),
            Case::new(lit(r#"1.0"#).str_json_decode(Float)).produces(Float, [1.0, 1.0, 1.0]),
            Case::new(lit(r#""hi""#).str_json_decode(String)).produces(String, ["hi", "hi", "hi"]),
            Case::new(lit(r#"true"#).str_json_decode(Bool)).produces(Bool, [true, true, true]),
            // can do int -> float promotions as well as float -> int (if it's a whole number)
            Case::new(lit(r#"1"#).str_json_decode(Float)).produces(Float, [1.0, 1.0, 1.0]),
            Case::new(lit(r#"1.0"#).str_json_decode(Int)).produces(Int, [1, 1, 1]),
            Case::new(lit(r#"1.1"#).str_json_decode(Int)).produces(Int, [1, 1, 1]),
            // and truncates for negative numbers
            Case::new(lit(r#"-1.1"#).str_json_decode(Int)).produces(Int, [-1, -1, -1]),
            Case::new(lit(r#"True"#).str_json_decode(Bool)).errors(),
            // can parse from columns - optional or not
            Case::new(col("int_strings").str_json_decode(Int)).produces(Int, [1, 2, 3]),
            Case::new(col("float_strings").str_json_decode(Float)).produces(Float, [1.0, 2.0, 3.1]),
            Case::new(col("str_strings").str_json_decode(String)).produces(String, ["1", "", "Hi"]),
            Case::new(col("bool_strings").str_json_decode(Bool))
                .produces(Bool, [true, false, true]),
            Case::new(col("ts_strings").str_json_decode(Timestamp))
                .produces(Timestamp, [1609459200, 1609545600, 1609632000]),
            Case::new(col("opt_int_strings").str_json_decode(Int))
                .produces(Type::optional(Int), [Some(1), None, Some(3)]),
            Case::new(col("opt_float_strings").str_json_decode(Float))
                .produces(Type::optional(Float), [Some(1.0), None, Some(3.1)]),
            Case::new(col("opt_str_strings").str_json_decode(String))
                .produces(Type::optional(String), [Some("1"), None, Some("Hi")]),
            Case::new(col("opt_bool_strings").str_json_decode(Bool))
                .produces(Type::optional(Bool), [Some(true), None, Some(true)]),
            Case::new(col("opt_ts_strings").str_json_decode(Timestamp)).produces(
                Type::optional(Timestamp),
                [Some(1609459200), None, Some(1609632000)],
            ),
            Case::new(col("list_strings").str_json_decode(List(Box::new(Int))))
                .try_produces(List(Box::new(Int)), [vec![1, 2, 3], vec![4, 5], vec![]]),
            Case::new(col("opt_list_strings").str_json_decode(Type::optional(List(Box::new(Int)))))
                .try_produces(
                    Type::optional(List(Box::new(Int))),
                    [Some(vec![1, 2, 3]), None, Some(vec![])],
                ),
            Case::new(col("struct_strings").str_json_decode(struct_type.clone())).produces(
                struct_type.clone(),
                [
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Int(2)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(3)),
                            ("b".into(), Value::Int(4)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(5)),
                            ("b".into(), Value::Int(6)),
                        ])
                        .unwrap(),
                    )),
                ],
            ),
            Case::new(
                col("opt_struct_strings").str_json_decode(Type::optional(struct_type.clone())),
            )
            .produces(
                Type::optional(struct_type.clone()),
                [
                    Some(Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Int(2)),
                        ])
                        .unwrap(),
                    ))),
                    None,
                    Some(Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(5)),
                            ("b".into(), Value::Int(6)),
                        ])
                        .unwrap(),
                    ))),
                ],
            ),
            Case::new(col("nested_struct_strings").str_json_decode(nested_struct_type.clone()))
                .produces(
                    nested_struct_type.clone(),
                    [
                        Value::Struct(Arc::new(
                            value::Struct::new(vec![
                                ("a".into(), Value::Int(1)),
                                (
                                    "b".into(),
                                    Value::Struct(Arc::new(
                                        value::Struct::new(vec![("c".into(), Value::Int(2))])
                                            .unwrap(),
                                    )),
                                ),
                            ])
                            .unwrap(),
                        )),
                        Value::Struct(Arc::new(
                            value::Struct::new(vec![
                                ("a".into(), Value::Int(3)),
                                (
                                    "b".into(),
                                    Value::Struct(Arc::new(
                                        value::Struct::new(vec![("c".into(), Value::Int(4))])
                                            .unwrap(),
                                    )),
                                ),
                            ])
                            .unwrap(),
                        )),
                        Value::Struct(Arc::new(
                            value::Struct::new(vec![
                                ("a".into(), Value::Int(5)),
                                (
                                    "b".into(),
                                    Value::Struct(Arc::new(
                                        value::Struct::new(vec![("c".into(), Value::Int(6))])
                                            .unwrap(),
                                    )),
                                ),
                            ])
                            .unwrap(),
                        )),
                    ],
                ),
            Case::new(lit(Value::None).str_json_decode(Int))
                .produces(Type::optional(Int), [None::<Value>, None, None]),
            Case::new(lit(Value::None).str_json_decode(Float))
                .produces(Type::optional(Float), [None::<Value>, None, None]),
            Case::new(lit(Value::None).str_json_decode(String))
                .produces(Type::optional(String), [None::<Value>, None, None]),
            Case::new(lit(Value::None).str_json_decode(Bool))
                .produces(Type::optional(Bool), [None::<Value>, None, None]),
            Case::new(lit(Value::None).str_json_decode(Timestamp))
                .produces(Type::optional(Timestamp), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_dt_parts() {
        let df = default_df();
        let cases = [
            // some invalid strings
            Case::new(lit(1).dt_part("year")).invalid(),
            Case::new(lit(1.0).dt_part("year")).invalid(),
            Case::new(lit(true).dt_part("year")).invalid(),
            Case::new(lit("hi").dt_part("year")).invalid(),
            Case::new(
                lit("2019-02-03")
                    .strptime("%Y-%m-%d")
                    .dt_part_tz("year", "random_time_zone"),
            )
            .invalid(),
            // simple valid cases
            Case::new(lit("2019-02-03").strptime("%Y-%m-%d").dt_part("year"))
                .produces(Int, [2019, 2019, 2019]),
            Case::new(lit("2019-02-03").strptime("%Y-%m-%d").dt_part("month"))
                .produces(Int, [2, 2, 2]),
            Case::new(lit("2019-02-03").strptime("%Y-%m-%d").dt_part("week"))
                .produces(Int, [5, 5, 5]),
            Case::new(lit("2019-02-03").strptime("%Y-%m-%d").dt_part("day"))
                .produces(Int, [3, 3, 3]),
            Case::new(
                lit("2019-02-03")
                    .strptime_tz("%Y-%m-%d", "Asia/Calcutta")
                    .dt_part_tz("day", "Asia/Calcutta"),
            )
            .produces(Int, [3, 3, 3]),
            Case::new(
                lit("2019-02-03 13:03:21")
                    .strptime("%Y-%m-%d %H:%M:%S")
                    .dt_part("hour"),
            )
            .produces(Int, [13, 13, 13]),
            // do the same but with timezone
            Case::new(
                lit("2019-02-03 13:03:21")
                    .strptime_tz("%Y-%m-%d %H:%M:%S", "America/New_York")
                    .dt_part_tz("hour", "Asia/Calcutta"),
            )
            .produces(Int, [23, 23, 23]),
            Case::new(
                lit("2019-02-03 13:03:21")
                    .strptime("%Y-%m-%d %H:%M:%S")
                    .dt_part("minute"),
            )
            .produces(Int, [3, 3, 3]),
            Case::new(
                lit("2019-02-03 13:03:21")
                    .strptime_tz("%Y-%m-%d %H:%M:%S", "America/New_York")
                    .dt_part_tz("minute", "Asia/Calcutta"),
            )
            .produces(Int, [33, 33, 33]),
            Case::new(
                lit("2019-02-03 13:03:21")
                    .strptime("%Y-%m-%d %H:%M:%S")
                    .dt_part("second"),
            )
            .produces(Int, [21, 21, 21]),
            // works when string is optional
            Case::new(col("null_dt_strings").strptime("%Y-%m-%d").dt_part("year"))
                .produces(Type::optional(Int), [Some(2021), None, Some(2021)]),
            // string is None
            Case::new(lit(Value::None).dt_part("year"))
                .produces(Type::optional(Int), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_from_epoch() {
        let df = default_df();
        let cases = [
            // invalids
            Case::new(from_epoch(lit("hi"), "second")).invalid(),
            Case::new(from_epoch(lit(true), "second")).invalid(),
            // even floats are invalid
            Case::new(from_epoch(lit(1.0), "second")).invalid(),
            // wrong units
            Case::new(from_epoch(lit(1), "month")).invalid(),
            Case::new(from_epoch(lit(1), "second"))
                .produces(Timestamp, [1000_000, 1000_000, 1000_000]),
            Case::new(from_epoch(lit(1), "millis")).produces(Timestamp, [1000, 1000, 1000]),
            Case::new(from_epoch(lit(1), "micros")).produces(Timestamp, [1, 1, 1]),
            Case::new(from_epoch(col("g"), "millis"))
                .produces(Type::optional(Timestamp), [Some(1000), None, Some(3000)]),
            Case::new(from_epoch(lit(Value::None), "second"))
                .produces(Type::optional(Timestamp), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_since_epoch() {
        let df = Dataframe::new(vec![
            Col::from("int", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("float", Type::Float, vec![1.0, 2.0, 3.0]).unwrap(),
            Col::from("str", Type::String, vec!["1", "1.0", "Hi"]).unwrap(),
            Col::from("bool", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("ts", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "opt_ts",
                Type::optional(Type::Timestamp),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = [
            Case::new(lit(1).since_epoch("second")).invalid(),
            Case::new(from_epoch(lit(123123), "millis").since_epoch("second"))
                .produces(Int, [123; 3]),
            Case::new(lit("hi").since_epoch("second")).invalid(),
            Case::new(col("a").since_epoch("second")).invalid(),
            Case::new(col("ts").since_epoch("micros")).produces(Int, [1, 2, 3]),
            Case::new(col("ts").since_epoch("second")).produces(Int, [0, 0, 0]),
            Case::new(col("opt_ts").since_epoch("micros"))
                .produces(Type::optional(Int), [Some(1), None, Some(3)]),
            // wrong units
            Case::new(col("ts").since_epoch("month")).invalid(),
            Case::new(lit(Value::None).since_epoch("day"))
                .produces(Type::optional(Int), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_dt_since() {
        let df = Dataframe::new(vec![
            Col::from("int", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("float", Type::Float, vec![1.0, 2.0, 3.0]).unwrap(),
            Col::from("str", Type::String, vec!["1", "1.0", "Hi"]).unwrap(),
            Col::from("bool", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("ts", Type::Timestamp, vec![1, 2, 3]).unwrap(),
            Col::from(
                "opt_ts",
                Type::optional(Type::Timestamp),
                vec![Some(1), None, Some(3)],
            )
            .unwrap(),
        ])
        .unwrap();

        let cases = [
            // invalids
            Case::new(lit(1).since(lit(2), "second")).invalid(),
            Case::new(lit("hi").since(from_epoch(lit(1), "second"), "second")).invalid(),
            Case::new(col("a").since(col("b"), "second")).invalid(),
            Case::new(col("a").since(from_epoch(lit(1), "second"), "second")).invalid(),
            // invalid units
            Case::new(from_epoch(lit(1), "second").since(from_epoch(lit(1), "second"), "month"))
                .invalid(),
            Case::new(
                from_epoch(lit(1000), "second").since(from_epoch(lit(1), "second"), "second"),
            )
            .produces(Int, [999, 999, 999]),
            Case::new(
                from_epoch(lit(1000), "second").since(from_epoch(lit(1), "second"), "millis"),
            )
            .produces(Int, [999_000, 999_000, 999_000]),
            Case::new(
                from_epoch(lit(1000), "second").since(from_epoch(lit(1), "second"), "micros"),
            )
            .produces(Int, [999_000_000, 999_000_000, 999_000_000]),
            Case::new(
                from_epoch(lit(1000), "second").since(from_epoch(lit(1), "second"), "minute"),
            )
            .produces(Int, [16, 16, 16]),
            Case::new(from_epoch(lit(1000), "second").since(from_epoch(lit(1), "second"), "hour"))
                .produces(Int, [0, 0, 0]),
            Case::new(from_epoch(lit(1000), "second").since(from_epoch(lit(1), "second"), "day"))
                .produces(Int, [0, 0, 0]),
            // what if first arg is before second arg
            Case::new(
                from_epoch(lit(1), "second").since(from_epoch(lit(1000), "second"), "second"),
            )
            .produces(Int, [-999, -999, -999]),
            // one or both args are optional
            Case::new(col("opt_ts").since(from_epoch(lit(1), "micros"), "micros"))
                .produces(Type::optional(Int), [Some(0), None, Some(2)]),
            Case::new(from_epoch(lit(2), "micros").since(col("opt_ts"), "micros"))
                .produces(Type::optional(Int), [Some(1), None, Some(-1)]),
            Case::new(col("opt_ts").since(col("opt_ts"), "micros"))
                .produces(Type::optional(Int), [Some(0), None, Some(0)]),
            // what if one or both args are None
            Case::new(lit(Value::None).since(from_epoch(lit(1), "second"), "second"))
                .produces(Type::optional(Int), [None::<Value>, None, None]),
            Case::new(from_epoch(lit(1), "second").since(lit(Value::None), "second"))
                .produces(Type::optional(Int), [None::<Value>, None, None]),
            Case::new(lit(Value::None).since(lit(Value::None), "second"))
                .produces(Type::optional(Int), [None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_dt_strftime() {
        let day = 24 * 60 * 60 * 1_000_000;
        let df = Dataframe::new(vec![
            Col::from("int", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("float", Type::Float, vec![1.0, 2.0, 3.0]).unwrap(),
            Col::from("str", Type::String, vec!["1", "1.0", "Hi"]).unwrap(),
            Col::from("bool", Type::Bool, vec![true, false, true]).unwrap(),
            Col::from("ts", Type::Timestamp, vec![1000_000, 2000_000, 3000_000]).unwrap(),
            Col::from(
                "opt_ts",
                Type::optional(Type::Timestamp),
                vec![Some(day), None, Some(3 * day)],
            )
            .unwrap(),
        ])
        .unwrap();

        let cases = [
            // invalids
            Case::new(lit(1).strftime("%Y-%m-%d")).invalid(),
            Case::new(lit("hi").strftime("%Y-%m-%d")).invalid(),
            Case::new(col("a").strftime("%Y-%m-%d")).invalid(),
            //invalid timezone
            Case::new(col("ts").strftime_tz("%Y-%m-%d %H:%M:%S %Z", "random_timezone")).invalid(),
            // valids - basic days
            Case::new(col("ts").strftime("%Y-%m-%d"))
                .produces(String, ["1970-01-01", "1970-01-01", "1970-01-01"]),
            Case::new(col("opt_ts").strftime("%Y-%m-%d")).produces(
                Type::optional(String),
                [Some("1970-01-02"), None, Some("1970-01-04")],
            ),
            Case::new(lit(Value::None).strftime("%Y-%m-%d"))
                .produces(Type::optional(String), [None::<Value>, None, None]),
            // now add more complexity in the format - hours, minutes, seconds, timezone etc.
            Case::new(col("ts").strftime("%Y-%m-%d %H:%M:%S %Z")).produces(
                String,
                [
                    "1970-01-01 00:00:01 UTC",
                    "1970-01-01 00:00:02 UTC",
                    "1970-01-01 00:00:03 UTC",
                ],
            ),
            Case::new(col("ts").strftime_tz("%Y-%m-%d %H:%M:%S %Z", "America/New_York")).produces(
                String,
                [
                    "1969-12-31 19:00:01 EST",
                    "1969-12-31 19:00:02 EST",
                    "1969-12-31 19:00:03 EST",
                ],
            ),
            Case::new(col("opt_ts").strftime_tz("%Y-%m-%d %H:%M:%S %Z", "America/New_York"))
                .produces(
                    Type::optional(String),
                    [
                        Some("1970-01-01 19:00:00 EST"),
                        None,
                        Some("1970-01-03 19:00:00 EST"),
                    ],
                ),
            Case::new(col("opt_ts").strftime_tz("%Y-%m-%d %H:%M:%S %Z", "UTC")).produces(
                Type::optional(String),
                [
                    Some("1970-01-02 00:00:00 UTC"),
                    None,
                    Some("1970-01-04 00:00:00 UTC"),
                ],
            ),
            // only get day of the week
            Case::new(col("opt_ts").strftime("%A")).produces(
                Type::optional(String),
                [Some("Friday"), None, Some("Sunday")],
            ),
            // get hour/time with AM/PM
            Case::new(col("ts").strftime("%I %p")).produces(String, ["12 AM", "12 AM", "12 AM"]),
            // hour/mins/secs with am/pm
            Case::new(col("ts").strftime("%I:%M:%S %p"))
                .produces(String, ["12:00:01 AM", "12:00:02 AM", "12:00:03 AM"]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_dt_literal() {
        let df = default_df();
        let cases = [
            // invalid cases - month is < 1 or > 12, or day is < 1 or > 31 etc.
            Case::new(dt_lit(2020, 0, 1, 0, 0, 0, 0)).invalid(),
            Case::new(dt_lit(2021, 13, 1, 0, 0, 0, 0)).invalid(),
            Case::new(dt_lit(2022, 1, 0, 0, 0, 0, 0)).invalid(),
            Case::new(dt_lit(2023, 1, 32, 0, 0, 0, 0)).invalid(),
            Case::new(dt_lit(2024, 1, 1, 24, 0, 0, 0)).invalid(),
            Case::new(dt_lit(2025, 1, 1, 0, 60, 0, 0)).invalid(),
            Case::new(dt_lit(2026, 1, 1, 0, 0, 60, 0)).invalid(),
            Case::new(dt_lit(2027, 1, 1, 0, 0, 0, 1_000_000)).invalid(),
            // invalid due to wrong timezone
            Case::new(dt_lit_tz(2028, 1, 1, 0, 0, 0, 0, "random_tz")).invalid(),
            Case::new(dt_lit(2021, 1, 12, 21, 32, 47, 0))
                .produces(Timestamp, [1610487167000000; 3]),
            Case::new(dt_lit_tz(2021, 1, 12, 21, 32, 47, 0, "UTC"))
                .produces(Timestamp, [1610487167000000; 3]),
            Case::new(dt_lit_tz(2021, 1, 12, 21, 32, 47, 0, "America/New_York"))
                .produces(Timestamp, [1610505167000000; 3]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_make_struct() {
        let struct_type1 = StructType::new(
            "struct1".into(),
            vec![Field::new("a", Int), Field::new("b", Float)],
        )
        .unwrap();
        let struct_type2 = StructType::new(
            "struct2".into(),
            vec![
                Field::new("a", Int),
                Field::new("b", Type::Struct(Box::new(struct_type1.clone()))),
            ],
        )
        .unwrap();
        let struct_type3 = StructType::new(
            "struct3".into(),
            vec![
                Field::new("a", Type::optional(Int)),
                Field::new("b", Type::optional(Type::Float)),
            ],
        )
        .unwrap();
        let df = default_df();
        let cases = [
            // some invalids - fields missing and/or wrong types
            Case::new(struct_(struct_type1.clone(), &[])).invalid(),
            Case::new(struct_(struct_type1.clone(), &[("a", lit(1))])).invalid(),
            Case::new(struct_(
                struct_type1.clone(),
                &[("a", lit(1)), ("b", lit("hi"))],
            ))
            .invalid(),
            // extra fields are also invalid
            Case::new(struct_(
                struct_type1.clone(),
                &[("a", lit(1)), ("b", lit(2.1)), ("c", lit(3))],
            ))
            .invalid(),
            // duplicate fields are invalid
            Case::new(struct_(
                struct_type1.clone(),
                &[("a", lit(1)), ("b", lit(2.1)), ("a", lit(3))],
            ))
            .invalid(),
            // basic valid case
            Case::new(struct_(
                struct_type1.clone(),
                &[("a", lit(1)), ("b", lit(2.1))],
            ))
            .produces(
                Type::Struct(Box::new(struct_type1.clone())),
                [
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.1)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.1)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.1)),
                        ])
                        .unwrap(),
                    )),
                ],
            ),
            // more advanced case - nested structs
            Case::new(struct_(
                struct_type2.clone(),
                &[
                    ("a", lit(1)),
                    (
                        "b",
                        struct_(struct_type1.clone(), &[("a", lit(2)), ("b", lit(3.1))]),
                    ),
                ],
            ))
            .produces(
                Type::Struct(Box::new(struct_type2.clone())),
                [
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            (
                                "b".into(),
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("a".into(), Value::Int(2)),
                                        ("b".into(), Value::Float(3.1)),
                                    ])
                                    .unwrap(),
                                )),
                            ),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            (
                                "b".into(),
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("a".into(), Value::Int(2)),
                                        ("b".into(), Value::Float(3.1)),
                                    ])
                                    .unwrap(),
                                )),
                            ),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            (
                                "b".into(),
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("a".into(), Value::Int(2)),
                                        ("b".into(), Value::Float(3.1)),
                                    ])
                                    .unwrap(),
                                )),
                            ),
                        ])
                        .unwrap(),
                    )),
                ],
            ),
            Case::new(struct_(
                struct_type1.clone(),
                &[("a", lit(Value::None)), ("b", lit(2.1))],
            ))
            .invalid(),
            // also works if expression can be promoted into struct field
            Case::new(struct_(
                struct_type1.clone(),
                &[("a", lit(1)), ("b", col("a"))],
            ))
            .produces(
                Type::Struct(Box::new(struct_type1.clone())),
                [
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(1.0)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.0)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(3.0)),
                        ])
                        .unwrap(),
                    )),
                ],
            ),
            Case::new(struct_(
                struct_type3.clone(),
                &[("a", lit(1)), ("b", lit(2))],
            ))
            .produces(
                Type::Struct(Box::new(struct_type3.clone())),
                [
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.0)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.0)),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::Int(1)),
                            ("b".into(), Value::Float(2.0)),
                        ])
                        .unwrap(),
                    )),
                ],
            ),
            // if all fields are None, then it's valid
            Case::new(struct_(
                struct_type3.clone(),
                &[("a", lit(Value::None)), ("b", lit(Value::None))],
            ))
            .produces(
                Type::Struct(Box::new(struct_type3.clone())),
                [
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::None),
                            ("b".into(), Value::None),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::None),
                            ("b".into(), Value::None),
                        ])
                        .unwrap(),
                    )),
                    Value::Struct(Arc::new(
                        value::Struct::new(vec![
                            ("a".into(), Value::None),
                            ("b".into(), Value::None),
                        ])
                        .unwrap(),
                    )),
                ],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_proto_roundtrip() {
        let scenarios = [
            col("a"),
            lit(1),
            lit(1.0),
            lit("hi"),
            lit(true),
            lit(Value::None),
            col("a") + lit(3.0),
            col("a") - lit(3.0),
            col("a") * lit(3.0),
            col("a") / lit(3.0),
            unary(UnOp::Neg, col("a")),
            binary(col("a"), BinOp::Mod, lit(3.0)),
            binary(col("a"), BinOp::Eq, lit(3.0)),
            binary(col("a"), BinOp::Neq, lit(3.0)),
            binary(col("a"), BinOp::Gt, lit(3.0)),
            binary(col("a"), BinOp::Gte, lit(3.0)),
            binary(col("a"), BinOp::Lt, lit(3.0)),
            binary(col("a"), BinOp::Lte, lit(3.0)),
            col("a").and(lit(true)),
            col("a").or(lit(true)),
            not(col("a")),
            when(col("x").gt(lit(4)), lit(1)),
            when(col("x").gt(lit(4)), lit(1)).otherwise(lit(3)),
            when(col("x").gt(lit(4)), lit(1))
                .when(col("x").gt(lit(3)), lit(2))
                .otherwise(lit(3)),
            isnull(col("a")),
            isnull(lit(1)),
            isnull(lit(Value::None)),
            fillnull(col("a"), lit(1)),
            fillnull(lit(1), lit(1)),
            abs(col("a")),
            floor(col("a")),
            ceil(col("a")),
            round(col("a")),
            lit(1).repeat(lit(3)),
            col("a").repeat(lit(3)),
            col("a").repeat(lit(1) + lit(2)),
            Expr::StringFn {
                func: Box::new(StringFn::Len),
                expr: Box::new(col("x")),
            },
            Expr::StringFn {
                func: Box::new(StringFn::ToLower),
                expr: Box::new(col("x")),
            },
            Expr::StringFn {
                func: Box::new(StringFn::ToUpper),
                expr: Box::new(col("x")),
            },
            Expr::StringFn {
                func: Box::new(StringFn::ToUpper),
                expr: Box::new(col("x")),
            },
            Expr::StringFn {
                func: Box::new(StringFn::StartsWith { key: lit("hi") }),
                expr: Box::new(col("x")),
            },
            Expr::StringFn {
                func: Box::new(StringFn::EndsWith { key: lit("hi") }),
                expr: Box::new(col("x")),
            },
            Expr::DictFn {
                dict: Box::new(col("dict")),
                func: Box::new(DictFn::Get {
                    key: lit("foo"),
                    default: None,
                }),
            },
            Expr::DictFn {
                dict: Box::new(col("dict")),
                func: Box::new(DictFn::Get {
                    key: lit("foo"),
                    default: Some(lit(1)),
                }),
            },
            Expr::StructFn {
                struct_: Box::new(col("struct")),
                func: Box::new(StructFn::Get {
                    field: "foo".to_string(),
                }),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Len),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::HasNull),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Contains { item: lit("hi") }),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Sum),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Mean),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Min),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Max),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::All),
            },
            Expr::ListFn {
                list: Box::new(col("list")),
                func: Box::new(ListFn::Any),
            },
            col("a").since(lit("other"), "second"),
            col("a").since(lit("other"), "millis"),
            col("a").since(lit("other"), "micros"),
            col("a").since(lit("other"), "minute"),
            col("a").since(lit("other"), "hour"),
            col("a").since(lit("other"), "day"),
            col("a").since(lit("other"), "month"),
            col("a").since(lit("other"), "year"),
            col("a").since(lit("other"), "week"),
            col("a").since_epoch("second"),
            col("a").since_epoch("millis"),
            col("a").since_epoch("micros"),
            col("a").since_epoch("minute"),
            col("a").since_epoch("hour"),
            col("a").since_epoch("day"),
            col("a").since_epoch("month"),
            col("a").since_epoch("year"),
            col("a").since_epoch("week"),
            col("a").dt_part("year"),
            col("a").dt_part("month"),
            col("a").dt_part("week"),
            col("a").dt_part("day"),
            col("a").dt_part("hour"),
            col("a").dt_part("minute"),
            col("a").dt_part("second"),
            col("a").dt_part("millis"),
            col("a").dt_part("micros"),
            col("dt").strftime("%Y-%m-%d"),
            col("dt").strftime("%Y-%m-%d %H:%M:%S"),
            col("dt").strptime("%Y-%m-%d %H:%M:%S %Z"),
            col("a").strptime_tz("%Y-%m-%d %H:%M:%S %Z", "UTC"),
            col("a").str_json_decode(Int),
            col("a").str_json_decode(Type::optional(Int)),
            col("a").str_json_decode(Float),
            col("a").str_json_decode(Type::optional(Type::List(Box::new(Int)))),
            col("a").str_json_decode(String),
            col("a").str_json_decode(Type::Struct(Box::new(
                StructType::new(
                    "struct1".into(),
                    vec![Field::new("a", Int), Field::new("b", Float)],
                )
                .unwrap(),
            ))),
            from_epoch(col("a"), "second"),
            from_epoch(col("a"), "micros"),
            from_epoch(col("a"), "millis"),
            from_epoch(col("a"), "day"),
            from_epoch(col("a"), "week"),
            from_epoch(col("a"), "year"),
            dt_lit(2021, 1, 12, 21, 32, 47, 0),
            dt_lit_tz(2021, 1, 12, 21, 32, 47, 0, "UTC"),
            struct_(
                StructType::new(
                    "struct1".into(),
                    vec![Field::new("a", Int), Field::new("b", Float)],
                )
                .unwrap(),
                &[("a", lit(1)), ("b", lit(2.1))],
            ),
        ];
        for expr in scenarios {
            let proto_expr: ProtoExpr = expr.clone().try_into().unwrap();
            let new_expr: Expr = proto_expr.try_into().unwrap();
            assert_eq!(expr, new_expr);
        }
    }

    #[test]
    fn test_list_all_any() {
        let df = list_df();
        let cases = [
            Case::new(lit(1).list_any()).invalid(),
            Case::new(lit(1).list_all()).invalid(),
            Case::new(lit(1.0).list_any()).invalid(),
            Case::new(lit(1.0).list_all()).invalid(),
            Case::new(lit(true).list_any()).invalid(),
            Case::new(lit(true).list_all()).invalid(),
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                )))
                .list_any(),
            )
            .invalid(),
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                )))
                .list_all(),
            )
            .invalid(),
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Bool, &[Value::Bool(true), Value::Bool(false)]).unwrap(),
                )))
                .list_any(),
            )
            .produces(Bool, [true, true, true]),
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Bool, &[Value::Bool(true), Value::Bool(false)]).unwrap(),
                )))
                .list_all(),
            )
            .produces(Bool, [false, false, false]),
            Case::new(col("a").list_map("x", var("x").gte(lit(2))).list_any())
                .produces(Bool, [true, true, true]),
            Case::new(col("a").list_map("x", var("x").gte(lit(2))).list_all())
                .produces(Bool, [false, true, true]),
            Case::new(col("b").list_map("x", var("x").gte(lit(2))).list_any())
                .produces(Type::optional(Bool), [Some(true), None, Some(true)]),
            Case::new(col("b").list_map("x", var("x").gte(lit(2))).list_all())
                .produces(Type::optional(Bool), [Some(false), None, Some(true)]),
            Case::new(col("c").list_map("x", var("x").gte(lit(2))).list_any())
                .produces(Type::optional(Bool), [Some(true), Some(true), Some(true)]),
            Case::new(col("c").list_map("x", var("x").gte(lit(2))).list_all())
                .produces(Type::optional(Bool), [None, Some(true), None]),
            Case::new(
                col("empty_float")
                    .list_map("x", var("x").gte(lit(2.0)))
                    .list_any(),
            )
            .produces(Bool, [false, false, true]),
            Case::new(
                col("empty_float")
                    .list_map("x", var("x").gte(lit(2.0)))
                    .list_all(),
            )
            .produces(Bool, [true, true, true]),
            Case::new(
                col("empty_optional")
                    .list_map("x", var("x").gte(lit(2)))
                    .list_any(),
            )
            .produces(Type::optional(Bool), [Some(false), None, Some(true)]),
            Case::new(
                col("empty_optional")
                    .list_map("x", var("x").gte(lit(2)))
                    .list_all(),
            )
            .produces(Type::optional(Bool), [Some(true), None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_filter() {
        let df = list_df();
        let cases = vec![
            Case::new(col("a").list_filter("a", var("b"))).invalid(),
            // can not do filters on None list
            Case::new(lit(Value::None).list_filter("_", lit(true))).invalid(),
            // for some reason, list lits aren't parsed correctly by polars
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Type::optional(Int), &[Value::None, Value::None]).unwrap(),
                )))
                .list_filter("_", lit(true)),
            )
            .produces(
                Type::List(Box::new(Type::optional(Int))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Type::optional(Int), &[Value::None, Value::None]).unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(Type::optional(Int), &[Value::None, Value::None]).unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(Type::optional(Int), &[Value::None, Value::None]).unwrap(),
                    )),
                ],
            ),
            Case::new(col("a").list_filter("_", lit(true))).produces(
                Type::List(Box::new(Int)),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                    )),
                    Value::List(Arc::new(value::List::new(Int, &[Value::Int(3)]).unwrap())),
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(4), Value::Int(5)]).unwrap(),
                    )),
                ],
            ),
            Case::new(col("b").list_filter("_", lit(false))).produces(
                Type::optional(Type::List(Box::new(Int))),
                vec![
                    Some(Value::List(Arc::new(value::List::new(Int, &[]).unwrap()))),
                    None,
                    Some(Value::List(Arc::new(value::List::new(Int, &[]).unwrap()))),
                ],
            ),
            Case::new(col("a").list_filter("x", var("x").modulo(lit(2)).eq(lit(0)))).produces(
                Type::List(Box::new(Int)),
                vec![
                    Value::List(Arc::new(value::List::new(Int, &[Value::Int(2)]).unwrap())),
                    Value::List(Arc::new(value::List::new(Int, &[]).unwrap())),
                    Value::List(Arc::new(value::List::new(Int, &[Value::Int(4)]).unwrap())),
                ],
            ),
            Case::new(col("c").list_filter("x", var("x").modulo(lit(2)).eq(lit(0)))).invalid(),
            Case::new(col("c").list_filter("x", not(isnull(var("x"))))).produces(
                Type::List(Box::new(Type::optional(Int))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Type::optional(Int), &[Value::Int(1), Value::Int(2)])
                            .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(11), Value::Int(4), Value::Int(2)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(Type::optional(Int), &[Value::Int(3), Value::Int(6)])
                            .unwrap(),
                    )),
                ],
            ),
        ];
        check(df, cases);
    }
    #[test]
    fn test_list_min_max() {
        let df = list_df();
        let cases = [
            Case::new(lit(1).list_min()).invalid(),
            Case::new(lit(1).list_max()).invalid(),
            Case::new(lit(1.0).list_min()).invalid(),
            Case::new(lit(1.0).list_max()).invalid(),
            Case::new(lit("hi").list_min()).invalid(),
            Case::new(lit("hi").list_max()).invalid(),
            Case::new(lit(true).list_min()).invalid(),
            Case::new(lit(true).list_max()).invalid(),
            Case::new(col("str_list").list_min()).invalid(),
            Case::new(col("str_list").list_max()).invalid(),
            Case::new(col("a").list_min()).produces(Type::optional(Int), [1, 3, 4]),
            Case::new(col("a").list_max()).produces(Type::optional(Int), [2, 3, 5]),
            Case::new(col("b").list_min()).produces(Type::optional(Int), [Some(1), None, Some(4)]),
            Case::new(col("b").list_max()).produces(Type::optional(Int), [Some(2), None, Some(5)]),
            Case::new(col("c").list_min()).produces(Type::optional(Int), [None, Some(2), None]),
            Case::new(col("c").list_max()).produces(Type::optional(Int), [None, Some(11), None]),
            Case::new(col("empty_optional").list_min())
                .produces(Type::optional(Int), [None::<Value>, None, None]),
            Case::new(col("empty_optional").list_max())
                .produces(Type::optional(Int), [None::<Value>, None, None]),
            Case::new(col("empty_float").list_min()).produces(
                Type::optional(Float),
                [None::<Value>, None, Some(Value::Float(2.1))],
            ),
            Case::new(col("empty_float").list_max()).produces(
                Type::optional(Float),
                [None::<Value>, None, Some(Value::Float(2.1))],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_map() {
        let df = list_df();
        let cases = vec![
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Type::String, &["hi".into(), "world".into()]).unwrap(),
                )))
                .list_map("x", var("x").list_len()),
            )
            .invalid(),
            // map list of strings to their lengths
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Type::String, &["hi".into(), "world".into()]).unwrap(),
                )))
                .list_map("x", var("x").str_len()),
            )
            .produces(
                Type::List(Box::new(Int)),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(2), Value::Int(5)]).unwrap(),
                    ));
                    3
                ],
            ),
            // list of list of ints to their lengths
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(
                        Type::List(Box::new(Int)),
                        &[
                            Value::List(Arc::new(value::List::new(Int, &[Value::Int(1)]).unwrap())),
                            Value::List(Arc::new(
                                value::List::new(Int, &[Value::Int(0), Value::Int(1)]).unwrap(),
                            )),
                        ],
                    )
                    .unwrap(),
                )))
                .list_map("x", var("x").list_len()),
            )
            .produces(
                Type::List(Box::new(Int)),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(2)]).unwrap(),
                    ));
                    3
                ],
            ),
            Case::new(col("a").list_map("x", var("y").modulo(lit(2)))).invalid(),
            Case::new(col("a").list_map("x", var("x").modulo(lit(2)))).produces(
                Type::List(Box::new(Int)),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(0)]).unwrap(),
                    )),
                    Value::List(Arc::new(value::List::new(Int, &[Value::Int(1)]).unwrap())),
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(0), Value::Int(1)]).unwrap(),
                    )),
                ],
            ),
            Case::new(col("b").list_map("x", var("x").modulo(lit(2)))).produces(
                Type::optional(Type::List(Box::new(Int))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(1), Value::Int(0)]).unwrap(),
                    )),
                    Value::None,
                    Value::List(Arc::new(
                        value::List::new(Int, &[Value::Int(0), Value::Int(1)]).unwrap(),
                    )),
                ],
            ),
            Case::new(col("c").list_map("x", var("x").modulo(lit(2)))).produces(
                Type::List(Box::new(Type::optional(Int))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(1), Value::None, Value::Int(0)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(1), Value::Int(0), Value::Int(0)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::optional(Int),
                            &[Value::Int(1), Value::None, Value::Int(0)],
                        )
                        .unwrap(),
                    )),
                ],
            ),
            Case::new(col("c").list_map("x", not(isnull(var("x"))))).produces(
                Type::List(Box::new(Type::Bool)),
                vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Bool,
                            &[Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Bool,
                            &[Value::Bool(true), Value::Bool(true), Value::Bool(true)],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Bool,
                            &[Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                        )
                        .unwrap(),
                    )),
                ],
            ),
            Case::new(col("empty_optional").list_map("x", not(isnull(var("x"))))).produces(
                Type::optional(List(Box::new(Bool))),
                vec![
                    Value::List(Arc::new(value::List::new(Bool, &[]).unwrap())),
                    Value::None,
                    Value::List(Arc::new(
                        value::List::new(
                            Bool,
                            &[Value::Bool(true), Value::Bool(false), Value::Bool(true)],
                        )
                        .unwrap(),
                    )),
                ],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_mean() {
        let df = list_df();
        let cases = vec![
            Case::new(lit(1).list_mean()).invalid(),
            Case::new(lit(1.0).list_mean()).invalid(),
            Case::new(lit(true).list_mean()).invalid(),
            Case::new(lit(Value::None).list_mean())
                .produces(Type::optional(Float), vec![None::<Value>, None, None]),
            Case::new(col("a").list_mean()).produces(Type::optional(Float), vec![1.5, 3.0, 4.5]),
            Case::new(col("b").list_mean())
                .produces(Type::optional(Float), vec![Some(1.5), None, Some(4.5)]),
            Case::new(col("empty_float").list_mean()).produces(
                Type::optional(Float),
                vec![Value::None, Value::None, Value::Float(2.1)],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_list_sum() {
        let df = list_df();
        let cases = vec![
            Case::new(lit(1).list_sum()).invalid(),
            Case::new(lit(1.0).list_sum()).invalid(),
            Case::new(lit(true).list_sum()).invalid(),
            Case::new(
                lit(Value::List(Arc::new(
                    value::List::new(Bool, &[Value::Bool(true), Value::Bool(false)]).unwrap(),
                )))
                .list_sum(),
            )
            .invalid(),
            Case::new(lit(Value::None).list_sum())
                .produces(Type::optional(Int), vec![None::<Value>, None, None]),
            Case::new(col("a").list_sum()).produces(Type::Int, vec![3, 3, 9]),
            Case::new(col("b").list_sum())
                .produces(Type::optional(Int), vec![Some(3), None, Some(9)]),
            Case::new(col("c").list_sum())
                .produces(Type::optional(Int), vec![None, Some(17), None]),
            Case::new(col("d").list_sum()).invalid(),
            Case::new(col("e").list_sum())
                .produces(Type::optional(Float), vec![Some(3.0), None, Some(11.0)]),
            Case::new(col("empty_optional").list_sum())
                .produces(Type::optional(Int), vec![Some(0), None, None]),
        ];
        check(df, cases);
    }
    #[test]
    fn test_str_parse_complex_structs() {
        let df = Dataframe::new(vec![Col::from("string", Type::String, vec!["{\"policy_details\": [{\"idv\": \"1234\", \"txt_insurer\": \"abc\", \"policy_status\": \"Active\", \"policy_end_date\": \"2000-09-01\"}]}"]).unwrap()]).unwrap();

        let inner_struct = Type::Struct(Box::new(
            StructType::new(
                "policy_details".into(),
                vec![
                    Field::new("idv", Type::String),
                    Field::new("txt_insurer", Type::String),
                    Field::new("policy_status", Type::String),
                    Field::new("policy_end_date", Type::String),
                ],
            )
            .unwrap(),
        ));

        let main_struct = Type::Struct(Box::new(
            StructType::new(
                "main".into(),
                vec![Field::new(
                    "policy_details",
                    Type::List(Box::new(inner_struct)),
                )],
            )
            .unwrap(),
        ));

        let expr = Expr::StringFn {
            func: Box::new(StringFn::JsonDecode {
                dtype: main_struct.clone(),
            }),
            expr: Box::new(col("string")),
        };

        let schema = Arc::new(Schema::new(vec![Field::new("string", Type::String)]).unwrap());
        let compiled_expr = expr.compile(schema.clone()).unwrap();
        let _ = df
            .eval(
                &compiled_expr,
                compiled_expr.dtype().clone(),
                "string",
                None,
            )
            .unwrap();
        // also works when string contains extra fields and doesn't contain some
        // optional fields
        let df = Dataframe::new(vec![Col::from(
            "string",
            Type::String,
            vec!["{\"x\": 1, \"y\": 2}"],
        )
        .unwrap()])
        .unwrap();
        let struct_type = Type::Struct(Box::new(
            StructType::new(
                "struct1".into(),
                vec![Field::new("x", Int), Field::new("z", Type::optional(Bool))],
            )
            .unwrap(),
        ));
        let expr = Expr::StringFn {
            func: Box::new(StringFn::JsonDecode {
                dtype: struct_type.clone(),
            }),
            expr: Box::new(col("string")),
        };
        let compiled_expr = expr.compile(schema.clone()).unwrap();
        let _ = df
            .eval(
                &compiled_expr,
                compiled_expr.dtype().clone(),
                "string",
                None,
            )
            .unwrap();
    }

    #[test]
    fn test_dict_len() {
        let df = dict_df();
        let cases = vec![
            Case::new(lit(1).dict_len()).invalid(),
            Case::new(col("dict").dict_len())
                .produces(Type::optional(Int), vec![Some(3), None, Some(0), Some(2)]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_dict_get() {
        // TODO: make this more robust
        let df = dict_df();
        let cases = vec![
            Case::new(col("dict").dict_get(lit("a"), None))
                .produces(Type::optional(Int), vec![Some(1), None, None, Some(8)]),
            Case::new(col("dict").dict_get(lit("b"), None))
                .produces(Type::optional(Int), vec![Some(2), None, None, None]),
            Case::new(col("dict").dict_get(lit("c"), Some(lit(-1))))
                .produces(Type::optional(Int), vec![Some(3), None, Some(-1), Some(-1)]),
            Case::new(col("dict").dict_get(lit("d"), Some(lit(-1)))).produces(
                Type::optional(Int),
                vec![Some(-1), None, Some(-1), Some(-1)],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_now() {
        let x = (UTCTimestamp::now().unwrap().micros() - 1) / (24 * 60 * 60 * 1000_000);
        let y = (UTCTimestamp::now().unwrap().micros() - 2) / (24 * 60 * 60 * 1000_000);
        let z = (UTCTimestamp::now().unwrap().micros() - 3) / (24 * 60 * 60 * 1000_000);

        let df = default_df();
        let cases = vec![
            Case::new(now().since(col("f"), "day")).produces(Type::Int, vec![x, y, z]),
            Case::new(now().since(col("f"), "day"))
                .eval_ctx(EvalContext::new(Some("now_col".to_string())))
                .produces(Type::Int, vec![0, 0, 0]),
            Case::new(now().since(col("i"), "micros"))
                .eval_ctx(EvalContext::new(Some("now_col".to_string())))
                .produces(Type::Int, vec![6, 7, 8]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_split() {
        let df = default_df();
        let cases = vec![
            Case::new(lit(1).split(",")).invalid(),
            Case::new(lit("").split(","))
                .produces(Type::List(Box::new(String)), vec![str_list(&[""]); 3]),
            Case::new(lit("a,b,c").split(",")).produces(
                Type::List(Box::new(String)),
                vec![str_list(&["a", "b", "c"]); 3],
            ),
            Case::new(col("d").split(".")).produces(
                Type::List(Box::new(String)),
                vec![str_list(&["1"]), str_list(&["1", "0"]), str_list(&["Hi"])],
            ),
            Case::new(col("null_strings").split("y")).produces(
                Type::optional(Type::List(Box::new(String))),
                vec![Some(str_list(&["Hi"])), None, Some(str_list(&["B", "e"]))],
            ),
            Case::new(lit(Value::None).split(",")).produces(
                Type::optional(Type::List(Box::new(String))),
                vec![None::<Value>, None::<Value>, None::<Value>],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_json_extract() {
        let df = Dataframe::new(vec![
            Col::from(
                "list_strings",
                String,
                vec![r#"[1, 2, 3]"#, r#"[4, 5]"#, r#"[]"#],
            )
            .unwrap(),
            Col::from(
                "opt_list_strings",
                Type::optional(String),
                vec![Some(r#"[1, 2, 3]"#), None, Some(r#"[]"#)],
            )
            .unwrap(),
            Col::from(
                "struct_strings",
                Type::String,
                vec![
                    r#"{"a": 1, "b": 2, "c": 3}"#,
                    r#"{"a": 3, "b": 4}"#,
                    r#"{"a": 5, "b": 6}"#,
                ],
            )
            .unwrap(),
            Col::from(
                "opt_struct_strings",
                Type::optional(String),
                vec![
                    Some(r#"{"a": 1, "b": 2}"#),
                    None,
                    Some(r#"{"a": 5, "b": 6}"#),
                ],
            )
            .unwrap(),
            Col::from(
                "nested_struct_strings",
                Type::String,
                vec![
                    r#"{"a": 1, "b": {"c": 2}}"#,
                    r#"{"a": 3, "b": {"c":4}}"#,
                    r#"{"a": 5, "b": {"c": 6}}"#,
                ],
            )
            .unwrap(),
            Col::from(
                "struct_with_list",
                Type::optional(String),
                vec![
                    Some(r#"{"a": 1, "b": [{"c": 2}, {"c": 3}]}"#),
                    None,
                    Some(r#"{"a": 5, "b": []}"#),
                ],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = vec![
            // invalid path
            Case::new(lit(1).json_extract("$.a")).invalid(),
            Case::new(col("list_strings").json_extract("$[a]")).invalid(),
            Case::new(col("struct_with_list").json_extract("b")).invalid(),
            // can not have negative index on list
            Case::new(col("struct_with_list").json_extract("$.b[-1].c")).invalid(),
            // basic extraction
            Case::new(col("list_strings").json_extract("$[0]"))
                .produces(Type::optional(String), vec![Some("1"), Some("4"), None]),
            Case::new(col("opt_list_strings").json_extract("$[0]"))
                .produces(Type::optional(String), vec![Some("1"), None, None]),
            Case::new(col("struct_strings").json_extract("$.a")).produces(
                Type::optional(String),
                vec![Some("1"), Some("3"), Some("5")],
            ),
            Case::new(col("struct_strings").json_extract("$.b")).produces(
                Type::optional(String),
                vec![Some("2"), Some("4"), Some("6")],
            ),
            Case::new(col("struct_strings").json_extract("$.c"))
                .produces(Type::optional(String), vec![Some("3"), None, None]),
            Case::new(col("nested_struct_strings").json_extract("$.b.c")).produces(
                Type::optional(String),
                vec![Some("2"), Some("4"), Some("6")],
            ),
            Case::new(col("nested_struct_strings").json_extract("$.a")).produces(
                Type::optional(String),
                vec![Some("1"), Some("3"), Some("5")],
            ),
            Case::new(col("nested_struct_strings").json_extract("$.a").to_int())
                .produces(Type::optional(Int), vec![Some(1), Some(3), Some(5)]),
            Case::new(col("nested_struct_strings").json_extract("$.b")).produces(
                Type::optional(String),
                vec![Some("{\"c\":2}"), Some("{\"c\":4}"), Some("{\"c\":6}")],
            ),
            Case::new(col("nested_struct_strings").json_extract("$.d.c"))
                .produces(Type::optional(String), vec![None::<Value>, None, None]),
            Case::new(col("struct_with_list").json_extract("$.b[0].c"))
                .produces(Type::optional(String), vec![Some("2"), None, None]),
            Case::new(col("struct_with_list").json_extract("$.b[2].c"))
                .produces(Type::optional(String), vec![None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_repeat_by() {
        let df = default_df();
        let cases = vec![
            Case::new(col("a").repeat(lit(1)))
                .try_produces(Type::List(Box::new(Int)), vec![vec![1], vec![2], vec![3]]),
            Case::new(col("a").repeat(lit(2))).try_produces(
                Type::List(Box::new(Int)),
                vec![vec![1, 1], vec![2, 2], vec![3, 3]],
            ),
            Case::new(col("a").repeat(lit(0)))
                .try_produces(Type::List(Box::new(Int)), vec![Vec::<i64>::new(); 3]),
            Case::new(col("a").repeat(lit("hi"))).invalid(),
            Case::new(col("a").repeat(lit(0.5))).invalid(),
            Case::new(col("a").repeat(lit(0.0))).invalid(),
            Case::new(col("a").repeat(lit(Value::None))).invalid(),
            Case::new(col("a").repeat(col("a"))).try_produces(
                Type::List(Box::new(Int)),
                vec![vec![1], vec![2, 2], vec![3, 3, 3]],
            ),
            Case::new(col("c").repeat(col("a"))).try_produces(
                Type::List(Box::new(Float)),
                vec![vec![1.0], vec![2.0, 2.0], vec![3.1, 3.1, 3.1]],
            ),
            Case::new(col("d").repeat(col("a"))).try_produces(
                Type::List(Box::new(String)),
                vec![vec!["1"], vec!["1.0", "1.0"], vec!["Hi", "Hi", "Hi"]],
            ),
            Case::new(col("e").repeat(col("a"))).try_produces(
                Type::List(Box::new(Bool)),
                vec![vec![true], vec![false, false], vec![true, true, true]],
            ),
            Case::new(col("g").repeat(col("a"))).try_produces(
                Type::List(Box::new(Type::optional(Int))),
                vec![vec![Some(1)], vec![None::<i64>; 2], vec![Some(3); 3]],
            ),
        ];

        check(df, cases);
    }

    #[test]
    #[should_panic]
    fn test_repeat_by_panic() {
        let df = default_df();
        let cases = vec![Case::new(col("a").repeat(lit(-1))).errors()];
        check(df, cases);
    }

    #[test]
    fn test_zip() {
        let df = list_df();
        let st1 = StructType::new(
            "struct1".into(),
            vec![
                Field::new("int", Int),
                Field::new("float", Type::optional(Float)),
            ],
        )
        .unwrap();
        let l1: Value = vec![1, 2].try_into().unwrap();
        let l2: Value = vec![1.0, 2.0].try_into().unwrap();
        let cases = vec![
            Case::new(zip(&st1, &[("int", lit(l1)), ("float", lit(l2))])).produces(
                Type::List(Box::new(Type::Struct(Box::new(st1.clone())))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Struct(Box::new(st1.clone())),
                            &[
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("int".into(), Value::Int(1)),
                                        ("float".into(), Value::Float(1.0)),
                                    ])
                                    .unwrap(),
                                )),
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("int".into(), Value::Int(2)),
                                        ("float".into(), Value::Float(2.0)),
                                    ])
                                    .unwrap(),
                                )),
                            ],
                        )
                        .unwrap(),
                    ));
                    3
                ],
            ),
            Case::new(zip(&st1, &[("int", col("a")), ("float", col("e"))])).invalid(),
            Case::new(zip(
                &st1,
                &[("int", col("a")), ("float", col("basic_float"))],
            ))
            .produces(
                Type::List(Box::new(Type::Struct(Box::new(st1.clone())))),
                vec![
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Struct(Box::new(st1.clone())),
                            &[
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("int".into(), Value::Int(1)),
                                        ("float".into(), Value::Float(1.0)),
                                    ])
                                    .unwrap(),
                                )),
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("int".into(), Value::Int(2)),
                                        ("float".into(), Value::Float(2.0)),
                                    ])
                                    .unwrap(),
                                )),
                            ],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Struct(Box::new(st1.clone())),
                            &[Value::Struct(Arc::new(
                                value::Struct::new(vec![
                                    ("int".into(), Value::Int(3)),
                                    ("float".into(), Value::Float(3.0)),
                                ])
                                .unwrap(),
                            ))],
                        )
                        .unwrap(),
                    )),
                    Value::List(Arc::new(
                        value::List::new(
                            Type::Struct(Box::new(st1.clone())),
                            &[
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("int".into(), Value::Int(4)),
                                        ("float".into(), Value::Float(5.0)),
                                    ])
                                    .unwrap(),
                                )),
                                Value::Struct(Arc::new(
                                    value::Struct::new(vec![
                                        ("int".into(), Value::Int(5)),
                                        ("float".into(), Value::Float(6.0)),
                                    ])
                                    .unwrap(),
                                )),
                            ],
                        )
                        .unwrap(),
                    )),
                ],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_log() {
        let df = default_df();
        let cases = vec![
            Case::new(lit(2).log(2.0)).produces(Float, vec![1.0, 1.0, 1.0]),
            Case::new(lit(-2.0).log(2.0)).produces(Float, vec![f64::NAN, f64::NAN, f64::NAN]),
            Case::new(lit(2.0).log(-2.0)).produces(Float, vec![f64::NAN, f64::NAN, f64::NAN]),
            Case::new(lit(8.0).log(2.0)).produces(Float, vec![3.0, 3.0, 3.0]),
            Case::new(lit(10).log(std::f64::consts::E)).produces(
                Float,
                vec![2.302585092994046, 2.302585092994046, 2.302585092994046],
            ),
            Case::new(col("a").log(2.0)).produces(Float, vec![0.0, 1.0, 1.5849625007211563]),
            Case::new(col("c").log(2.0)).produces(Float, vec![0.0, 1.0, 1.632268215499513]),
            Case::new(col("g").log(2.0)).produces(
                Type::optional(Float),
                vec![Some(0.0), None, Some(1.5849625007211563)],
            ),
            Case::new(col("d").log(2.0)).invalid(),
            Case::new(col("e").log(2.0)).invalid(),
            Case::new(col("f").log(2.0)).invalid(),
            Case::new(lit(0.0).log(0.0)).produces(Float, vec![f64::NAN, f64::NAN, f64::NAN]),
            Case::new(lit(0.0).log(2.0)).produces(
                Float,
                vec![f64::NEG_INFINITY, f64::NEG_INFINITY, f64::NEG_INFINITY],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_sqrt() {
        let df = default_df();
        let cases = vec![
            Case::new(lit(4.0).sqrt()).produces(Float, vec![2.0, 2.0, 2.0]),
            Case::new(lit(4).sqrt()).produces(Float, vec![2.0, 2.0, 2.0]),
            Case::new(lit(-4.0).sqrt()).produces(Float, vec![f64::NAN, f64::NAN, f64::NAN]),
            Case::new(lit(-4).sqrt()).produces(Float, vec![f64::NAN, f64::NAN, f64::NAN]),
            Case::new(col("a").sqrt())
                .produces(Float, vec![1.0, 1.4142135623730951, 1.7320508075688772]),
            Case::new(col("c").sqrt())
                .produces(Float, vec![1.0, 1.4142135623730951, 1.760681686165901]),
            Case::new(col("g").sqrt()).produces(
                Type::optional(Float),
                vec![Some(1.0), None, Some(1.7320508075688772)],
            ),
            Case::new(lit("hi").sqrt()).invalid(),
            Case::new(lit(true).sqrt()).invalid(),
            Case::new(lit(Value::None).sqrt())
                .produces(Type::optional(Float), vec![None::<Value>, None, None]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_pow() {
        let df = default_df();
        let cases = vec![
            Case::new(lit(true).pow(lit(2))).invalid(),
            Case::new(lit("hi").pow(lit(2))).invalid(),
            Case::new(lit(2).pow(lit(true))).invalid(),
            Case::new(lit(2).pow(lit("hi"))).invalid(),
            Case::new(lit(2).pow(lit(2))).produces(Int, vec![4, 4, 4]),
            Case::new(lit(2).pow(lit(2.0))).produces(Float, vec![4.0, 4.0, 4.0]),
            Case::new(lit(2.0).pow(lit(2.0))).produces(Float, vec![4.0, 4.0, 4.0]),
            Case::new(lit(2.0).pow(lit(2))).produces(Float, vec![4.0, 4.0, 4.0]),
            Case::new(lit(2).pow(lit(-2))).errors(),
            Case::new(lit(2.0).pow(lit(-2))).produces(Float, vec![0.25, 0.25, 0.25]),
            Case::new(lit(2.0).pow(lit(-2.0))).produces(Float, vec![0.25, 0.25, 0.25]),
            Case::new(lit(2).pow(lit(-2.0))).produces(Float, vec![0.25, 0.25, 0.25]),
            Case::new(col("a").pow(col("a"))).produces(Int, vec![1, 4, 27]),
            Case::new(col("c").pow(col("a"))).produces(Float, vec![1.0, 4.0, 29.791]),
            Case::new(col("g").pow(col("a")))
                .produces(Type::optional(Int), vec![Some(1), None, Some(27)]),
            Case::new(col("a").pow(col("g")))
                .produces(Type::optional(Int), vec![Some(1), None, Some(27)]),
            Case::new(lit(Value::None).pow(lit(2)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(lit(2).pow(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_to_int() {
        let df = Dataframe::new(vec![
            Col::from("int_strings", String, vec!["1", "2", "3"]).unwrap(),
            Col::from(
                "opt_int_strings",
                Type::optional(String),
                vec![Some("1"), None, Some("3")],
            )
            .unwrap(),
        ])
        .unwrap();
        let cases = vec![
            Case::new(lit(1).to_int()).invalid(),
            Case::new(lit("1").to_int()).produces(Type::Int, vec![1, 1, 1]),
            Case::new(lit("-123").to_int()).produces(Type::Int, vec![-123, -123, -123]),
            Case::new(lit("abc").to_int()).errors(),
            Case::new(lit("123abc").to_int()).errors(),
            Case::new(lit("123.45").to_int()).errors(),
            Case::new(lit("123 456").to_int()).errors(),
            Case::new(lit("").to_int()).errors(),
            Case::new(lit(Value::None).to_int())
                .produces(Type::optional(Int), vec![None::<Value>, None, None]),
            Case::new(col("int_strings").to_int()).produces(Type::Int, vec![1, 2, 3]),
            Case::new(col("opt_int_strings").to_int())
                .produces(Type::optional(Int), vec![Some(1), None, Some(3)]),
        ];
        check(df, cases);
    }

    #[test]
    fn test_trig() {
        let df = default_df();
        let cases = vec![
            Case::new(sin(lit(true))).invalid(),
            Case::new(sin(lit("hi"))).invalid(),
            Case::new(sin(lit(0))).produces(Float, vec![0.0; 3]),
            Case::new(sin(lit(1))).produces(
                Float,
                vec![0.8414709848078965, 0.8414709848078965, 0.8414709848078965],
            ),
            Case::new(sin(lit(1.0))).produces(
                Float,
                vec![0.8414709848078965, 0.8414709848078965, 0.8414709848078965],
            ),
            Case::new(sin(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(sin(col("a"))).produces(
                Float,
                vec![0.8414709848078965, 0.9092974268256817, 0.1411200080598672],
            ),
            Case::new(sin(col("c"))).produces(
                Float,
                vec![0.8414709848078965, 0.9092974268256817, 0.04158066243329049],
            ),
            Case::new(sin(col("g"))).produces(
                Type::optional(Float),
                vec![Some(0.8414709848078965), None, Some(0.1411200080598672)],
            ),
            // now write the same tests for cos, tan, asin, acos, atan
            Case::new(cos(lit(true))).invalid(),
            Case::new(cos(lit("hi"))).invalid(),
            Case::new(cos(lit(1))).produces(
                Float,
                vec![0.5403023058681398, 0.5403023058681398, 0.5403023058681398],
            ),
            Case::new(cos(lit(1.0))).produces(
                Float,
                vec![0.5403023058681398, 0.5403023058681398, 0.5403023058681398],
            ),
            Case::new(cos(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(cos(col("a"))).produces(
                Float,
                vec![0.5403023058681398, -0.4161468365471424, -0.9899924966004454],
            ),
            Case::new(cos(col("c"))).produces(
                Float,
                vec![0.5403023058681398, -0.4161468365471424, -0.9991351502732795],
            ),
            Case::new(cos(col("g"))).produces(
                Type::optional(Float),
                vec![Some(0.5403023058681398), None, Some(-0.9899924966004454)],
            ),
            // same tests for tan
            Case::new(tan(lit(true))).invalid(),
            Case::new(tan(lit("hi"))).invalid(),
            Case::new(tan(lit(0))).produces(Float, vec![0.0; 3]),
            Case::new(tan(lit(1))).produces(
                Float,
                vec![1.5574077246549023, 1.5574077246549023, 1.5574077246549023],
            ),
            Case::new(tan(lit(1.0))).produces(
                Float,
                vec![1.5574077246549023, 1.5574077246549023, 1.5574077246549023],
            ),
            Case::new(tan(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(tan(col("a"))).produces(
                Float,
                vec![1.557407724654902, -2.185039863261519, -0.1425465430742778],
            ),
            Case::new(tan(col("c"))).produces(
                Float,
                vec![1.557407724654902, -2.185039863261519, -0.04161665458],
            ),
            Case::new(tan(col("g"))).produces(
                Type::optional(Float),
                vec![Some(1.557407724654902), None, Some(-0.1425465430742778)],
            ),
            // same tests for asin
            Case::new(asin(lit(true))).invalid(),
            Case::new(asin(lit("hi"))).invalid(),
            Case::new(asin(lit(0))).produces(Float, vec![0.0; 3]),
            Case::new(asin(lit(1))).produces(
                Float,
                vec![1.5707963267948966, 1.5707963267948966, 1.5707963267948966],
            ),
            Case::new(asin(lit(1.0))).produces(
                Float,
                vec![1.5707963267948966, 1.5707963267948966, 1.5707963267948966],
            ),
            Case::new(asin(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(asin(col("a"))).produces(Float, vec![1.5707963267948966, f64::NAN, f64::NAN]),
            Case::new(asin(col("c"))).produces(Float, vec![1.5707963267948966, f64::NAN, f64::NAN]),
            Case::new(asin(col("g"))).produces(
                Type::optional(Float),
                vec![Some(1.5707963267948966), None, Some(f64::NAN)],
            ),
            // same tests for acos
            Case::new(acos(lit(true))).invalid(),
            Case::new(acos(lit("hi"))).invalid(),
            Case::new(acos(lit(0))).produces(
                Float,
                vec![1.5707963267948966, 1.5707963267948966, 1.5707963267948966],
            ),
            Case::new(acos(lit(1))).produces(Float, vec![0.0, 0.0, 0.0]),
            Case::new(acos(lit(1.0))).produces(Float, vec![0.0, 0.0, 0.0]),
            Case::new(acos(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(acos(col("a"))).produces(Float, vec![0.0, f64::NAN, f64::NAN]),
            Case::new(acos(col("c"))).produces(Float, vec![0.0, f64::NAN, f64::NAN]),
            Case::new(acos(col("g")))
                .produces(Type::optional(Float), vec![Some(0.0), None, Some(f64::NAN)]),
            // same tests for atan
            Case::new(atan(lit(true))).invalid(),
            Case::new(atan(lit("hi"))).invalid(),
            Case::new(atan(lit(0))).produces(Float, vec![0.0; 3]),
            Case::new(atan(lit(1))).produces(
                Float,
                vec![0.7853981633974483, 0.7853981633974483, 0.7853981633974483],
            ),
            Case::new(atan(lit(1.0))).produces(
                Float,
                vec![0.7853981633974483, 0.7853981633974483, 0.7853981633974483],
            ),
            Case::new(atan(lit(Value::None)))
                .produces(Type::optional(Float), vec![None::<Value>; 3]),
            Case::new(atan(col("a"))).produces(
                Float,
                vec![0.7853981633974483, 1.1071487177940906, 1.2490457723982544],
            ),
            Case::new(atan(col("c"))).produces(
                Float,
                vec![0.7853981633974483, 1.1071487177940906, 1.2587542052323633],
            ),
            Case::new(atan(col("g"))).produces(
                Type::optional(Float),
                vec![Some(0.7853981633974483), None, Some(1.2490457723982544)],
            ),
        ];
        check(df, cases);
    }

    #[test]
    fn test_nan_infinite() {
        let df = default_df();
        let cases = vec![
            Case::new(is_nan(lit(true))).invalid(),
            Case::new(is_infinite(lit(true))).invalid(),
            Case::new(is_nan(lit("hi"))).invalid(),
            Case::new(is_infinite(lit("hi"))).invalid(),
            Case::new(is_nan(lit(0))).produces(Bool, vec![false; 3]),
            Case::new(is_infinite(lit(0))).produces(Bool, vec![false; 3]),
            Case::new(is_nan(lit(f64::NAN))).produces(Bool, vec![true; 3]),
            Case::new(is_infinite(lit(f64::INFINITY))).produces(Bool, vec![true; 3]),
            Case::new(is_infinite(lit(f64::NEG_INFINITY))).produces(Bool, vec![true; 3]),
            Case::new(is_nan(col("nan_floats"))).produces(Bool, vec![true, false, false]),
            Case::new(is_infinite(col("nan_floats"))).produces(Bool, vec![false, true, true]),
            Case::new(is_nan(col("null_nan_floats"))).produces(
                Optional(Box::new(Bool)),
                vec![None::<bool>, Some(true), Some(false)],
            ),
            Case::new(is_infinite(col("null_nan_floats"))).produces(
                Optional(Box::new(Bool)),
                vec![None::<bool>, Some(false), Some(true)],
            ),
        ];
        check(df, cases);
    }

    fn str_list(s: &[&str]) -> Value {
        Value::List(Arc::new(
            value::List::new(String, &s.iter().map(|s| value_str(s)).collect_vec()).unwrap(),
        ))
    }

    fn value_str(s: &str) -> Value {
        Value::String(Arc::new(s.to_string()))
    }
}
