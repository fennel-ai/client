// @generated
// This file is @generated by prost-build.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EvalContext {
    #[prost(string, optional, tag="1")]
    pub now_col_name: ::core::option::Option<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Expr {
    #[prost(oneof="expr::Node", tags="1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19")]
    pub node: ::core::option::Option<expr::Node>,
}
/// Nested message and enum types in `Expr`.
pub mod expr {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Node {
        #[prost(message, tag="1")]
        Ref(super::Ref),
        /// Used for serializing a literal as a JSON string
        #[prost(message, tag="2")]
        JsonLiteral(super::JsonLiteral),
        #[prost(message, tag="4")]
        Unary(::prost::alloc::boxed::Box<super::Unary>),
        #[prost(message, tag="5")]
        Case(::prost::alloc::boxed::Box<super::Case>),
        #[prost(message, tag="6")]
        Binary(::prost::alloc::boxed::Box<super::Binary>),
        #[prost(message, tag="7")]
        Isnull(::prost::alloc::boxed::Box<super::IsNull>),
        #[prost(message, tag="8")]
        Fillnull(::prost::alloc::boxed::Box<super::FillNull>),
        #[prost(message, tag="9")]
        ListFn(::prost::alloc::boxed::Box<super::ListFn>),
        #[prost(message, tag="10")]
        MathFn(::prost::alloc::boxed::Box<super::MathFn>),
        #[prost(message, tag="11")]
        StructFn(::prost::alloc::boxed::Box<super::StructFn>),
        #[prost(message, tag="12")]
        DictFn(::prost::alloc::boxed::Box<super::DictFn>),
        #[prost(message, tag="13")]
        StringFn(::prost::alloc::boxed::Box<super::StringFn>),
        #[prost(message, tag="14")]
        DatetimeFn(::prost::alloc::boxed::Box<super::DateTimeFn>),
        #[prost(message, tag="15")]
        DatetimeLiteral(super::DatetimeLiteral),
        #[prost(message, tag="16")]
        MakeStruct(super::MakeStruct),
        #[prost(message, tag="17")]
        FromEpoch(::prost::alloc::boxed::Box<super::FromEpoch>),
        #[prost(message, tag="18")]
        Var(super::Var),
        #[prost(message, tag="19")]
        Now(super::Now),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Now {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Var {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FromEpoch {
    #[prost(message, optional, boxed, tag="1")]
    pub duration: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(enumeration="TimeUnit", tag="2")]
    pub unit: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatetimeLiteral {
    #[prost(uint32, tag="1")]
    pub year: u32,
    #[prost(uint32, tag="2")]
    pub month: u32,
    #[prost(uint32, tag="3")]
    pub day: u32,
    #[prost(uint32, tag="4")]
    pub hour: u32,
    #[prost(uint32, tag="5")]
    pub minute: u32,
    #[prost(uint32, tag="6")]
    pub second: u32,
    #[prost(uint32, tag="7")]
    pub microsecond: u32,
    #[prost(message, optional, tag="8")]
    pub timezone: ::core::option::Option<Timezone>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MakeStruct {
    #[prost(message, optional, tag="1")]
    pub struct_type: ::core::option::Option<super::schema::StructType>,
    #[prost(map="string, message", tag="2")]
    pub fields: ::std::collections::HashMap<::prost::alloc::string::String, Expr>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JsonLiteral {
    /// Literal sent as a JSON string
    #[prost(string, tag="1")]
    pub literal: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub dtype: ::core::option::Option<super::schema::DataType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ref {
    #[prost(string, tag="1")]
    pub name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Unary {
    #[prost(enumeration="UnaryOp", tag="1")]
    pub op: i32,
    #[prost(message, optional, boxed, tag="2")]
    pub operand: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Binary {
    #[prost(message, optional, boxed, tag="1")]
    pub left: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub right: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(enumeration="BinOp", tag="3")]
    pub op: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Case {
    #[prost(message, repeated, tag="1")]
    pub when_then: ::prost::alloc::vec::Vec<WhenThen>,
    #[prost(message, optional, boxed, tag="2")]
    pub otherwise: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WhenThen {
    #[prost(message, optional, tag="1")]
    pub when: ::core::option::Option<Expr>,
    #[prost(message, optional, tag="2")]
    pub then: ::core::option::Option<Expr>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IsNull {
    #[prost(message, optional, boxed, tag="1")]
    pub operand: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FillNull {
    #[prost(message, optional, boxed, tag="1")]
    pub operand: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub fill: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListOp {
    #[prost(oneof="list_op::FnType", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12")]
    pub fn_type: ::core::option::Option<list_op::FnType>,
}
/// Nested message and enum types in `ListOp`.
pub mod list_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnType {
        #[prost(message, tag="1")]
        Len(super::Len),
        /// Index to fetch an element from the list
        #[prost(message, tag="2")]
        Get(::prost::alloc::boxed::Box<super::Expr>),
        /// Check if the list contains an element
        #[prost(message, tag="3")]
        Contains(::prost::alloc::boxed::Box<super::Contains>),
        #[prost(message, tag="4")]
        HasNull(super::HasNull),
        #[prost(message, tag="5")]
        Sum(super::ListSum),
        #[prost(message, tag="6")]
        Min(super::ListMin),
        #[prost(message, tag="7")]
        Max(super::ListMax),
        #[prost(message, tag="8")]
        All(super::ListAll),
        #[prost(message, tag="9")]
        Any(super::ListAny),
        #[prost(message, tag="10")]
        Mean(super::ListMean),
        #[prost(message, tag="11")]
        Filter(::prost::alloc::boxed::Box<super::ListFilter>),
        #[prost(message, tag="12")]
        Map(::prost::alloc::boxed::Box<super::ListMap>),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListFilter {
    #[prost(string, tag="1")]
    pub var: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag="2")]
    pub predicate: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListMap {
    #[prost(string, tag="1")]
    pub var: ::prost::alloc::string::String,
    #[prost(message, optional, boxed, tag="2")]
    pub map_expr: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ListSum {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ListMin {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ListMean {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ListMax {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ListAll {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ListAny {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Len {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct HasNull {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Contains {
    #[prost(message, optional, boxed, tag="1")]
    pub element: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListFn {
    #[prost(message, optional, boxed, tag="1")]
    pub list: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub r#fn: ::core::option::Option<::prost::alloc::boxed::Box<ListOp>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct MathOp {
    #[prost(oneof="math_op::FnType", tags="1, 2, 3, 4")]
    pub fn_type: ::core::option::Option<math_op::FnType>,
}
/// Nested message and enum types in `MathOp`.
pub mod math_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Oneof)]
    pub enum FnType {
        #[prost(message, tag="1")]
        Round(super::Round),
        #[prost(message, tag="2")]
        Abs(super::Abs),
        #[prost(message, tag="3")]
        Ceil(super::Ceil),
        #[prost(message, tag="4")]
        Floor(super::Floor),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Round {
    #[prost(int32, tag="1")]
    pub precision: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Abs {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Ceil {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Floor {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MathFn {
    #[prost(message, optional, boxed, tag="1")]
    pub operand: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, tag="2")]
    pub r#fn: ::core::option::Option<MathOp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructOp {
    #[prost(oneof="struct_op::FnType", tags="1")]
    pub fn_type: ::core::option::Option<struct_op::FnType>,
}
/// Nested message and enum types in `StructOp`.
pub mod struct_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnType {
        #[prost(string, tag="1")]
        Field(::prost::alloc::string::String),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StructFn {
    #[prost(message, optional, boxed, tag="1")]
    pub r#struct: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, tag="2")]
    pub r#fn: ::core::option::Option<StructOp>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DictGet {
    #[prost(message, optional, boxed, tag="1")]
    pub field: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="3")]
    pub default_value: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DictOp {
    #[prost(oneof="dict_op::FnType", tags="1, 2, 3")]
    pub fn_type: ::core::option::Option<dict_op::FnType>,
}
/// Nested message and enum types in `DictOp`.
pub mod dict_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnType {
        #[prost(message, tag="1")]
        Len(super::Len),
        #[prost(message, tag="2")]
        Get(::prost::alloc::boxed::Box<super::DictGet>),
        #[prost(message, tag="3")]
        Contains(::prost::alloc::boxed::Box<super::Contains>),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DictFn {
    #[prost(message, optional, boxed, tag="1")]
    pub dict: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub r#fn: ::core::option::Option<::prost::alloc::boxed::Box<DictOp>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringOp {
    #[prost(oneof="string_op::FnType", tags="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12")]
    pub fn_type: ::core::option::Option<string_op::FnType>,
}
/// Nested message and enum types in `StringOp`.
pub mod string_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnType {
        #[prost(message, tag="1")]
        Len(super::Len),
        #[prost(message, tag="2")]
        Tolower(super::ToLower),
        #[prost(message, tag="3")]
        Toupper(super::ToUpper),
        #[prost(message, tag="4")]
        Contains(::prost::alloc::boxed::Box<super::Contains>),
        #[prost(message, tag="5")]
        Startswith(::prost::alloc::boxed::Box<super::StartsWith>),
        #[prost(message, tag="6")]
        Endswith(::prost::alloc::boxed::Box<super::EndsWith>),
        #[prost(message, tag="7")]
        Concat(::prost::alloc::boxed::Box<super::Concat>),
        #[prost(message, tag="8")]
        Strptime(super::Strptime),
        #[prost(message, tag="9")]
        JsonDecode(super::JsonDecode),
        #[prost(message, tag="10")]
        Split(super::Split),
        #[prost(message, tag="11")]
        JsonExtract(super::JsonExtract),
        #[prost(message, tag="12")]
        ToInt(super::ToInt),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Timezone {
    #[prost(string, tag="1")]
    pub timezone: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JsonDecode {
    #[prost(message, optional, tag="1")]
    pub dtype: ::core::option::Option<super::schema::DataType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Strptime {
    #[prost(string, tag="1")]
    pub format: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub timezone: ::core::option::Option<Timezone>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ToLower {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ToUpper {
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StartsWith {
    #[prost(message, optional, boxed, tag="1")]
    pub key: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EndsWith {
    #[prost(message, optional, boxed, tag="1")]
    pub key: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Concat {
    #[prost(message, optional, boxed, tag="1")]
    pub other: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StringFn {
    #[prost(message, optional, boxed, tag="1")]
    pub string: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub r#fn: ::core::option::Option<::prost::alloc::boxed::Box<StringOp>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DateTimeFn {
    #[prost(message, optional, boxed, tag="1")]
    pub datetime: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(message, optional, boxed, tag="2")]
    pub r#fn: ::core::option::Option<::prost::alloc::boxed::Box<DateTimeOp>>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DateTimeOp {
    #[prost(oneof="date_time_op::FnType", tags="1, 2, 3, 4")]
    pub fn_type: ::core::option::Option<date_time_op::FnType>,
}
/// Nested message and enum types in `DateTimeOp`.
pub mod date_time_op {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum FnType {
        #[prost(message, tag="1")]
        Since(::prost::alloc::boxed::Box<super::Since>),
        #[prost(message, tag="2")]
        SinceEpoch(super::SinceEpoch),
        #[prost(message, tag="3")]
        Strftime(super::Strftime),
        #[prost(message, tag="4")]
        Part(super::Part),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Since {
    #[prost(message, optional, boxed, tag="1")]
    pub other: ::core::option::Option<::prost::alloc::boxed::Box<Expr>>,
    #[prost(enumeration="TimeUnit", tag="2")]
    pub unit: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct SinceEpoch {
    #[prost(enumeration="TimeUnit", tag="1")]
    pub unit: i32,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Strftime {
    #[prost(string, tag="1")]
    pub format: ::prost::alloc::string::String,
    #[prost(message, optional, tag="2")]
    pub timezone: ::core::option::Option<Timezone>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Part {
    #[prost(enumeration="TimeUnit", tag="1")]
    pub unit: i32,
    #[prost(message, optional, tag="2")]
    pub timezone: ::core::option::Option<Timezone>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Split {
    #[prost(string, tag="1")]
    pub sep: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JsonExtract {
    #[prost(string, tag="1")]
    pub path: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct ToInt {
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UnaryOp {
    Neg = 0,
    /// LEN = 2; \[DEPRECATED\]
    /// NEXT ID = 3;
    Not = 1,
}
impl UnaryOp {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            UnaryOp::Neg => "NEG",
            UnaryOp::Not => "NOT",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NEG" => Some(Self::Neg),
            "NOT" => Some(Self::Not),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum BinOp {
    Add = 0,
    Sub = 1,
    Mul = 2,
    Div = 3,
    Mod = 4,
    FloorDiv = 5,
    Eq = 6,
    Ne = 7,
    Gt = 8,
    Gte = 9,
    Lt = 10,
    Lte = 11,
    And = 12,
    Or = 13,
}
impl BinOp {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            BinOp::Add => "ADD",
            BinOp::Sub => "SUB",
            BinOp::Mul => "MUL",
            BinOp::Div => "DIV",
            BinOp::Mod => "MOD",
            BinOp::FloorDiv => "FLOOR_DIV",
            BinOp::Eq => "EQ",
            BinOp::Ne => "NE",
            BinOp::Gt => "GT",
            BinOp::Gte => "GTE",
            BinOp::Lt => "LT",
            BinOp::Lte => "LTE",
            BinOp::And => "AND",
            BinOp::Or => "OR",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "ADD" => Some(Self::Add),
            "SUB" => Some(Self::Sub),
            "MUL" => Some(Self::Mul),
            "DIV" => Some(Self::Div),
            "MOD" => Some(Self::Mod),
            "FLOOR_DIV" => Some(Self::FloorDiv),
            "EQ" => Some(Self::Eq),
            "NE" => Some(Self::Ne),
            "GT" => Some(Self::Gt),
            "GTE" => Some(Self::Gte),
            "LT" => Some(Self::Lt),
            "LTE" => Some(Self::Lte),
            "AND" => Some(Self::And),
            "OR" => Some(Self::Or),
            _ => None,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TimeUnit {
    Unknown = 0,
    Second = 1,
    Minute = 2,
    Hour = 3,
    Day = 4,
    Week = 5,
    Month = 6,
    Year = 7,
    Microsecond = 8,
    Millisecond = 9,
}
impl TimeUnit {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            TimeUnit::Unknown => "UNKNOWN",
            TimeUnit::Second => "SECOND",
            TimeUnit::Minute => "MINUTE",
            TimeUnit::Hour => "HOUR",
            TimeUnit::Day => "DAY",
            TimeUnit::Week => "WEEK",
            TimeUnit::Month => "MONTH",
            TimeUnit::Year => "YEAR",
            TimeUnit::Microsecond => "MICROSECOND",
            TimeUnit::Millisecond => "MILLISECOND",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "UNKNOWN" => Some(Self::Unknown),
            "SECOND" => Some(Self::Second),
            "MINUTE" => Some(Self::Minute),
            "HOUR" => Some(Self::Hour),
            "DAY" => Some(Self::Day),
            "WEEK" => Some(Self::Week),
            "MONTH" => Some(Self::Month),
            "YEAR" => Some(Self::Year),
            "MICROSECOND" => Some(Self::Microsecond),
            "MILLISECOND" => Some(Self::Millisecond),
            _ => None,
        }
    }
}
// @@protoc_insertion_point(module)
