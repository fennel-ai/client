"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import schema_pb2
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _UnaryOp:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _UnaryOpEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_UnaryOp.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    NEG: _UnaryOp.ValueType  # 0
    NOT: _UnaryOp.ValueType  # 1
    """LEN = 2; [DEPRECATED]
    NEXT ID = 3;
    """

class UnaryOp(_UnaryOp, metaclass=_UnaryOpEnumTypeWrapper): ...

NEG: UnaryOp.ValueType  # 0
NOT: UnaryOp.ValueType  # 1
"""LEN = 2; [DEPRECATED]
NEXT ID = 3;
"""
global___UnaryOp = UnaryOp

class _BinOp:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _BinOpEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_BinOp.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    ADD: _BinOp.ValueType  # 0
    SUB: _BinOp.ValueType  # 1
    MUL: _BinOp.ValueType  # 2
    DIV: _BinOp.ValueType  # 3
    MOD: _BinOp.ValueType  # 4
    FLOOR_DIV: _BinOp.ValueType  # 5
    EQ: _BinOp.ValueType  # 6
    NE: _BinOp.ValueType  # 7
    GT: _BinOp.ValueType  # 8
    GTE: _BinOp.ValueType  # 9
    LT: _BinOp.ValueType  # 10
    LTE: _BinOp.ValueType  # 11
    AND: _BinOp.ValueType  # 12
    OR: _BinOp.ValueType  # 13

class BinOp(_BinOp, metaclass=_BinOpEnumTypeWrapper): ...

ADD: BinOp.ValueType  # 0
SUB: BinOp.ValueType  # 1
MUL: BinOp.ValueType  # 2
DIV: BinOp.ValueType  # 3
MOD: BinOp.ValueType  # 4
FLOOR_DIV: BinOp.ValueType  # 5
EQ: BinOp.ValueType  # 6
NE: BinOp.ValueType  # 7
GT: BinOp.ValueType  # 8
GTE: BinOp.ValueType  # 9
LT: BinOp.ValueType  # 10
LTE: BinOp.ValueType  # 11
AND: BinOp.ValueType  # 12
OR: BinOp.ValueType  # 13
global___BinOp = BinOp

class _TimeUnit:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _TimeUnitEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_TimeUnit.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    UNKNOWN: _TimeUnit.ValueType  # 0
    SECOND: _TimeUnit.ValueType  # 1
    MINUTE: _TimeUnit.ValueType  # 2
    HOUR: _TimeUnit.ValueType  # 3
    DAY: _TimeUnit.ValueType  # 4
    WEEK: _TimeUnit.ValueType  # 5
    MONTH: _TimeUnit.ValueType  # 6
    YEAR: _TimeUnit.ValueType  # 7
    MICROSECOND: _TimeUnit.ValueType  # 8
    MILLISECOND: _TimeUnit.ValueType  # 9

class TimeUnit(_TimeUnit, metaclass=_TimeUnitEnumTypeWrapper): ...

UNKNOWN: TimeUnit.ValueType  # 0
SECOND: TimeUnit.ValueType  # 1
MINUTE: TimeUnit.ValueType  # 2
HOUR: TimeUnit.ValueType  # 3
DAY: TimeUnit.ValueType  # 4
WEEK: TimeUnit.ValueType  # 5
MONTH: TimeUnit.ValueType  # 6
YEAR: TimeUnit.ValueType  # 7
MICROSECOND: TimeUnit.ValueType  # 8
MILLISECOND: TimeUnit.ValueType  # 9
global___TimeUnit = TimeUnit

@typing_extensions.final
class Expr(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    REF_FIELD_NUMBER: builtins.int
    JSON_LITERAL_FIELD_NUMBER: builtins.int
    UNARY_FIELD_NUMBER: builtins.int
    CASE_FIELD_NUMBER: builtins.int
    BINARY_FIELD_NUMBER: builtins.int
    ISNULL_FIELD_NUMBER: builtins.int
    FILLNULL_FIELD_NUMBER: builtins.int
    LIST_FN_FIELD_NUMBER: builtins.int
    MATH_FN_FIELD_NUMBER: builtins.int
    STRUCT_FN_FIELD_NUMBER: builtins.int
    DICT_FN_FIELD_NUMBER: builtins.int
    STRING_FN_FIELD_NUMBER: builtins.int
    DATETIME_FN_FIELD_NUMBER: builtins.int
    DATETIME_LITERAL_FIELD_NUMBER: builtins.int
    MAKE_STRUCT_FIELD_NUMBER: builtins.int
    FROM_EPOCH_FIELD_NUMBER: builtins.int
    @property
    def ref(self) -> global___Ref: ...
    @property
    def json_literal(self) -> global___JsonLiteral:
        """Used for serializing a literal as a JSON string"""
    @property
    def unary(self) -> global___Unary: ...
    @property
    def case(self) -> global___Case: ...
    @property
    def binary(self) -> global___Binary: ...
    @property
    def isnull(self) -> global___IsNull: ...
    @property
    def fillnull(self) -> global___FillNull: ...
    @property
    def list_fn(self) -> global___ListFn: ...
    @property
    def math_fn(self) -> global___MathFn: ...
    @property
    def struct_fn(self) -> global___StructFn: ...
    @property
    def dict_fn(self) -> global___DictFn: ...
    @property
    def string_fn(self) -> global___StringFn: ...
    @property
    def datetime_fn(self) -> global___DateTimeFn: ...
    @property
    def datetime_literal(self) -> global___DatetimeLiteral: ...
    @property
    def make_struct(self) -> global___MakeStruct: ...
    @property
    def from_epoch(self) -> global___FromEpoch: ...
    def __init__(
        self,
        *,
        ref: global___Ref | None = ...,
        json_literal: global___JsonLiteral | None = ...,
        unary: global___Unary | None = ...,
        case: global___Case | None = ...,
        binary: global___Binary | None = ...,
        isnull: global___IsNull | None = ...,
        fillnull: global___FillNull | None = ...,
        list_fn: global___ListFn | None = ...,
        math_fn: global___MathFn | None = ...,
        struct_fn: global___StructFn | None = ...,
        dict_fn: global___DictFn | None = ...,
        string_fn: global___StringFn | None = ...,
        datetime_fn: global___DateTimeFn | None = ...,
        datetime_literal: global___DatetimeLiteral | None = ...,
        make_struct: global___MakeStruct | None = ...,
        from_epoch: global___FromEpoch | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["binary", b"binary", "case", b"case", "datetime_fn", b"datetime_fn", "datetime_literal", b"datetime_literal", "dict_fn", b"dict_fn", "fillnull", b"fillnull", "from_epoch", b"from_epoch", "isnull", b"isnull", "json_literal", b"json_literal", "list_fn", b"list_fn", "make_struct", b"make_struct", "math_fn", b"math_fn", "node", b"node", "ref", b"ref", "string_fn", b"string_fn", "struct_fn", b"struct_fn", "unary", b"unary"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["binary", b"binary", "case", b"case", "datetime_fn", b"datetime_fn", "datetime_literal", b"datetime_literal", "dict_fn", b"dict_fn", "fillnull", b"fillnull", "from_epoch", b"from_epoch", "isnull", b"isnull", "json_literal", b"json_literal", "list_fn", b"list_fn", "make_struct", b"make_struct", "math_fn", b"math_fn", "node", b"node", "ref", b"ref", "string_fn", b"string_fn", "struct_fn", b"struct_fn", "unary", b"unary"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["node", b"node"]) -> typing_extensions.Literal["ref", "json_literal", "unary", "case", "binary", "isnull", "fillnull", "list_fn", "math_fn", "struct_fn", "dict_fn", "string_fn", "datetime_fn", "datetime_literal", "make_struct", "from_epoch"] | None: ...

global___Expr = Expr

@typing_extensions.final
class FromEpoch(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DURATION_FIELD_NUMBER: builtins.int
    UNIT_FIELD_NUMBER: builtins.int
    @property
    def duration(self) -> global___Expr: ...
    unit: global___TimeUnit.ValueType
    def __init__(
        self,
        *,
        duration: global___Expr | None = ...,
        unit: global___TimeUnit.ValueType = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["duration", b"duration"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["duration", b"duration", "unit", b"unit"]) -> None: ...

global___FromEpoch = FromEpoch

@typing_extensions.final
class DatetimeLiteral(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    YEAR_FIELD_NUMBER: builtins.int
    MONTH_FIELD_NUMBER: builtins.int
    DAY_FIELD_NUMBER: builtins.int
    HOUR_FIELD_NUMBER: builtins.int
    MINUTE_FIELD_NUMBER: builtins.int
    SECOND_FIELD_NUMBER: builtins.int
    MICROSECOND_FIELD_NUMBER: builtins.int
    TIMEZONE_FIELD_NUMBER: builtins.int
    year: builtins.int
    month: builtins.int
    day: builtins.int
    hour: builtins.int
    minute: builtins.int
    second: builtins.int
    microsecond: builtins.int
    @property
    def timezone(self) -> global___Timezone: ...
    def __init__(
        self,
        *,
        year: builtins.int = ...,
        month: builtins.int = ...,
        day: builtins.int = ...,
        hour: builtins.int = ...,
        minute: builtins.int = ...,
        second: builtins.int = ...,
        microsecond: builtins.int = ...,
        timezone: global___Timezone | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["timezone", b"timezone"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["day", b"day", "hour", b"hour", "microsecond", b"microsecond", "minute", b"minute", "month", b"month", "second", b"second", "timezone", b"timezone", "year", b"year"]) -> None: ...

global___DatetimeLiteral = DatetimeLiteral

@typing_extensions.final
class MakeStruct(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class FieldsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> global___Expr: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: global___Expr | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    STRUCT_TYPE_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    @property
    def struct_type(self) -> schema_pb2.StructType: ...
    @property
    def fields(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, global___Expr]: ...
    def __init__(
        self,
        *,
        struct_type: schema_pb2.StructType | None = ...,
        fields: collections.abc.Mapping[builtins.str, global___Expr] | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["struct_type", b"struct_type"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fields", b"fields", "struct_type", b"struct_type"]) -> None: ...

global___MakeStruct = MakeStruct

@typing_extensions.final
class JsonLiteral(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LITERAL_FIELD_NUMBER: builtins.int
    DTYPE_FIELD_NUMBER: builtins.int
    literal: builtins.str
    """Literal sent as a JSON string"""
    @property
    def dtype(self) -> schema_pb2.DataType: ...
    def __init__(
        self,
        *,
        literal: builtins.str = ...,
        dtype: schema_pb2.DataType | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["dtype", b"dtype"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dtype", b"dtype", "literal", b"literal"]) -> None: ...

global___JsonLiteral = JsonLiteral

@typing_extensions.final
class Ref(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    name: builtins.str
    def __init__(
        self,
        *,
        name: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["name", b"name"]) -> None: ...

global___Ref = Ref

@typing_extensions.final
class Unary(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OP_FIELD_NUMBER: builtins.int
    OPERAND_FIELD_NUMBER: builtins.int
    op: global___UnaryOp.ValueType
    @property
    def operand(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        op: global___UnaryOp.ValueType = ...,
        operand: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["operand", b"operand"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["op", b"op", "operand", b"operand"]) -> None: ...

global___Unary = Unary

@typing_extensions.final
class Binary(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LEFT_FIELD_NUMBER: builtins.int
    RIGHT_FIELD_NUMBER: builtins.int
    OP_FIELD_NUMBER: builtins.int
    @property
    def left(self) -> global___Expr: ...
    @property
    def right(self) -> global___Expr: ...
    op: global___BinOp.ValueType
    def __init__(
        self,
        *,
        left: global___Expr | None = ...,
        right: global___Expr | None = ...,
        op: global___BinOp.ValueType = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["left", b"left", "right", b"right"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["left", b"left", "op", b"op", "right", b"right"]) -> None: ...

global___Binary = Binary

@typing_extensions.final
class Case(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    WHEN_THEN_FIELD_NUMBER: builtins.int
    OTHERWISE_FIELD_NUMBER: builtins.int
    @property
    def when_then(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___WhenThen]: ...
    @property
    def otherwise(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        when_then: collections.abc.Iterable[global___WhenThen] | None = ...,
        otherwise: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["otherwise", b"otherwise"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["otherwise", b"otherwise", "when_then", b"when_then"]) -> None: ...

global___Case = Case

@typing_extensions.final
class WhenThen(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    WHEN_FIELD_NUMBER: builtins.int
    THEN_FIELD_NUMBER: builtins.int
    @property
    def when(self) -> global___Expr: ...
    @property
    def then(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        when: global___Expr | None = ...,
        then: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["then", b"then", "when", b"when"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["then", b"then", "when", b"when"]) -> None: ...

global___WhenThen = WhenThen

@typing_extensions.final
class IsNull(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERAND_FIELD_NUMBER: builtins.int
    @property
    def operand(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        operand: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["operand", b"operand"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["operand", b"operand"]) -> None: ...

global___IsNull = IsNull

@typing_extensions.final
class FillNull(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERAND_FIELD_NUMBER: builtins.int
    FILL_FIELD_NUMBER: builtins.int
    @property
    def operand(self) -> global___Expr: ...
    @property
    def fill(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        operand: global___Expr | None = ...,
        fill: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["fill", b"fill", "operand", b"operand"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fill", b"fill", "operand", b"operand"]) -> None: ...

global___FillNull = FillNull

@typing_extensions.final
class ListOp(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LEN_FIELD_NUMBER: builtins.int
    GET_FIELD_NUMBER: builtins.int
    CONTAINS_FIELD_NUMBER: builtins.int
    @property
    def len(self) -> global___Len: ...
    @property
    def get(self) -> global___Expr:
        """Index to fetch an element from the list"""
    @property
    def contains(self) -> global___Contains:
        """Check if the list contains an element"""
    def __init__(
        self,
        *,
        len: global___Len | None = ...,
        get: global___Expr | None = ...,
        contains: global___Contains | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["contains", b"contains", "fn_type", b"fn_type", "get", b"get", "len", b"len"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["contains", b"contains", "fn_type", b"fn_type", "get", b"get", "len", b"len"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["fn_type", b"fn_type"]) -> typing_extensions.Literal["len", "get", "contains"] | None: ...

global___ListOp = ListOp

@typing_extensions.final
class Len(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Len = Len

@typing_extensions.final
class Contains(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ELEMENT_FIELD_NUMBER: builtins.int
    @property
    def element(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        element: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["element", b"element"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["element", b"element"]) -> None: ...

global___Contains = Contains

@typing_extensions.final
class ListFn(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LIST_FIELD_NUMBER: builtins.int
    FN_FIELD_NUMBER: builtins.int
    @property
    def list(self) -> global___Expr: ...
    @property
    def fn(self) -> global___ListOp: ...
    def __init__(
        self,
        *,
        list: global___Expr | None = ...,
        fn: global___ListOp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["fn", b"fn", "list", b"list"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fn", b"fn", "list", b"list"]) -> None: ...

global___ListFn = ListFn

@typing_extensions.final
class MathOp(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ROUND_FIELD_NUMBER: builtins.int
    ABS_FIELD_NUMBER: builtins.int
    CEIL_FIELD_NUMBER: builtins.int
    FLOOR_FIELD_NUMBER: builtins.int
    @property
    def round(self) -> global___Round: ...
    @property
    def abs(self) -> global___Abs: ...
    @property
    def ceil(self) -> global___Ceil: ...
    @property
    def floor(self) -> global___Floor: ...
    def __init__(
        self,
        *,
        round: global___Round | None = ...,
        abs: global___Abs | None = ...,
        ceil: global___Ceil | None = ...,
        floor: global___Floor | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["abs", b"abs", "ceil", b"ceil", "floor", b"floor", "fn_type", b"fn_type", "round", b"round"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["abs", b"abs", "ceil", b"ceil", "floor", b"floor", "fn_type", b"fn_type", "round", b"round"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["fn_type", b"fn_type"]) -> typing_extensions.Literal["round", "abs", "ceil", "floor"] | None: ...

global___MathOp = MathOp

@typing_extensions.final
class Round(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PRECISION_FIELD_NUMBER: builtins.int
    precision: builtins.int
    def __init__(
        self,
        *,
        precision: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["precision", b"precision"]) -> None: ...

global___Round = Round

@typing_extensions.final
class Abs(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Abs = Abs

@typing_extensions.final
class Ceil(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Ceil = Ceil

@typing_extensions.final
class Floor(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Floor = Floor

@typing_extensions.final
class MathFn(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERAND_FIELD_NUMBER: builtins.int
    FN_FIELD_NUMBER: builtins.int
    @property
    def operand(self) -> global___Expr: ...
    @property
    def fn(self) -> global___MathOp: ...
    def __init__(
        self,
        *,
        operand: global___Expr | None = ...,
        fn: global___MathOp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["fn", b"fn", "operand", b"operand"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fn", b"fn", "operand", b"operand"]) -> None: ...

global___MathFn = MathFn

@typing_extensions.final
class StructOp(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FIELD_FIELD_NUMBER: builtins.int
    field: builtins.str
    def __init__(
        self,
        *,
        field: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field", b"field", "fn_type", b"fn_type"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field", b"field", "fn_type", b"fn_type"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["fn_type", b"fn_type"]) -> typing_extensions.Literal["field"] | None: ...

global___StructOp = StructOp

@typing_extensions.final
class StructFn(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    STRUCT_FIELD_NUMBER: builtins.int
    FN_FIELD_NUMBER: builtins.int
    @property
    def struct(self) -> global___Expr: ...
    @property
    def fn(self) -> global___StructOp: ...
    def __init__(
        self,
        *,
        struct: global___Expr | None = ...,
        fn: global___StructOp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["fn", b"fn", "struct", b"struct"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fn", b"fn", "struct", b"struct"]) -> None: ...

global___StructFn = StructFn

@typing_extensions.final
class DictGet(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FIELD_FIELD_NUMBER: builtins.int
    DEFAULT_VALUE_FIELD_NUMBER: builtins.int
    @property
    def field(self) -> global___Expr: ...
    @property
    def default_value(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        field: global___Expr | None = ...,
        default_value: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["default_value", b"default_value", "field", b"field"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default_value", b"default_value", "field", b"field"]) -> None: ...

global___DictGet = DictGet

@typing_extensions.final
class DictOp(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LEN_FIELD_NUMBER: builtins.int
    GET_FIELD_NUMBER: builtins.int
    CONTAINS_FIELD_NUMBER: builtins.int
    @property
    def len(self) -> global___Len: ...
    @property
    def get(self) -> global___DictGet: ...
    @property
    def contains(self) -> global___Contains: ...
    def __init__(
        self,
        *,
        len: global___Len | None = ...,
        get: global___DictGet | None = ...,
        contains: global___Contains | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["contains", b"contains", "fn_type", b"fn_type", "get", b"get", "len", b"len"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["contains", b"contains", "fn_type", b"fn_type", "get", b"get", "len", b"len"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["fn_type", b"fn_type"]) -> typing_extensions.Literal["len", "get", "contains"] | None: ...

global___DictOp = DictOp

@typing_extensions.final
class DictFn(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DICT_FIELD_NUMBER: builtins.int
    FN_FIELD_NUMBER: builtins.int
    @property
    def dict(self) -> global___Expr: ...
    @property
    def fn(self) -> global___DictOp: ...
    def __init__(
        self,
        *,
        dict: global___Expr | None = ...,
        fn: global___DictOp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["dict", b"dict", "fn", b"fn"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dict", b"dict", "fn", b"fn"]) -> None: ...

global___DictFn = DictFn

@typing_extensions.final
class StringOp(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LEN_FIELD_NUMBER: builtins.int
    TOLOWER_FIELD_NUMBER: builtins.int
    TOUPPER_FIELD_NUMBER: builtins.int
    CONTAINS_FIELD_NUMBER: builtins.int
    STARTSWITH_FIELD_NUMBER: builtins.int
    ENDSWITH_FIELD_NUMBER: builtins.int
    CONCAT_FIELD_NUMBER: builtins.int
    STRPTIME_FIELD_NUMBER: builtins.int
    JSON_DECODE_FIELD_NUMBER: builtins.int
    @property
    def len(self) -> global___Len: ...
    @property
    def tolower(self) -> global___ToLower: ...
    @property
    def toupper(self) -> global___ToUpper: ...
    @property
    def contains(self) -> global___Contains: ...
    @property
    def startswith(self) -> global___StartsWith: ...
    @property
    def endswith(self) -> global___EndsWith: ...
    @property
    def concat(self) -> global___Concat: ...
    @property
    def strptime(self) -> global___Strptime: ...
    @property
    def json_decode(self) -> global___JsonDecode: ...
    def __init__(
        self,
        *,
        len: global___Len | None = ...,
        tolower: global___ToLower | None = ...,
        toupper: global___ToUpper | None = ...,
        contains: global___Contains | None = ...,
        startswith: global___StartsWith | None = ...,
        endswith: global___EndsWith | None = ...,
        concat: global___Concat | None = ...,
        strptime: global___Strptime | None = ...,
        json_decode: global___JsonDecode | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["concat", b"concat", "contains", b"contains", "endswith", b"endswith", "fn_type", b"fn_type", "json_decode", b"json_decode", "len", b"len", "startswith", b"startswith", "strptime", b"strptime", "tolower", b"tolower", "toupper", b"toupper"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["concat", b"concat", "contains", b"contains", "endswith", b"endswith", "fn_type", b"fn_type", "json_decode", b"json_decode", "len", b"len", "startswith", b"startswith", "strptime", b"strptime", "tolower", b"tolower", "toupper", b"toupper"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["fn_type", b"fn_type"]) -> typing_extensions.Literal["len", "tolower", "toupper", "contains", "startswith", "endswith", "concat", "strptime", "json_decode"] | None: ...

global___StringOp = StringOp

@typing_extensions.final
class Timezone(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TIMEZONE_FIELD_NUMBER: builtins.int
    timezone: builtins.str
    def __init__(
        self,
        *,
        timezone: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["timezone", b"timezone"]) -> None: ...

global___Timezone = Timezone

@typing_extensions.final
class JsonDecode(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DTYPE_FIELD_NUMBER: builtins.int
    @property
    def dtype(self) -> schema_pb2.DataType: ...
    def __init__(
        self,
        *,
        dtype: schema_pb2.DataType | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["dtype", b"dtype"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dtype", b"dtype"]) -> None: ...

global___JsonDecode = JsonDecode

@typing_extensions.final
class Strptime(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FORMAT_FIELD_NUMBER: builtins.int
    TIMEZONE_FIELD_NUMBER: builtins.int
    format: builtins.str
    @property
    def timezone(self) -> global___Timezone: ...
    def __init__(
        self,
        *,
        format: builtins.str = ...,
        timezone: global___Timezone | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["timezone", b"timezone"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["format", b"format", "timezone", b"timezone"]) -> None: ...

global___Strptime = Strptime

@typing_extensions.final
class ToLower(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___ToLower = ToLower

@typing_extensions.final
class ToUpper(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___ToUpper = ToUpper

@typing_extensions.final
class StartsWith(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    KEY_FIELD_NUMBER: builtins.int
    @property
    def key(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        key: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["key", b"key"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["key", b"key"]) -> None: ...

global___StartsWith = StartsWith

@typing_extensions.final
class EndsWith(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    KEY_FIELD_NUMBER: builtins.int
    @property
    def key(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        key: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["key", b"key"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["key", b"key"]) -> None: ...

global___EndsWith = EndsWith

@typing_extensions.final
class Concat(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OTHER_FIELD_NUMBER: builtins.int
    @property
    def other(self) -> global___Expr: ...
    def __init__(
        self,
        *,
        other: global___Expr | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["other", b"other"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["other", b"other"]) -> None: ...

global___Concat = Concat

@typing_extensions.final
class StringFn(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    STRING_FIELD_NUMBER: builtins.int
    FN_FIELD_NUMBER: builtins.int
    @property
    def string(self) -> global___Expr: ...
    @property
    def fn(self) -> global___StringOp: ...
    def __init__(
        self,
        *,
        string: global___Expr | None = ...,
        fn: global___StringOp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["fn", b"fn", "string", b"string"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fn", b"fn", "string", b"string"]) -> None: ...

global___StringFn = StringFn

@typing_extensions.final
class DateTimeFn(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATETIME_FIELD_NUMBER: builtins.int
    FN_FIELD_NUMBER: builtins.int
    @property
    def datetime(self) -> global___Expr: ...
    @property
    def fn(self) -> global___DateTimeOp: ...
    def __init__(
        self,
        *,
        datetime: global___Expr | None = ...,
        fn: global___DateTimeOp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["datetime", b"datetime", "fn", b"fn"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["datetime", b"datetime", "fn", b"fn"]) -> None: ...

global___DateTimeFn = DateTimeFn

@typing_extensions.final
class DateTimeOp(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SINCE_FIELD_NUMBER: builtins.int
    SINCE_EPOCH_FIELD_NUMBER: builtins.int
    STRFTIME_FIELD_NUMBER: builtins.int
    PART_FIELD_NUMBER: builtins.int
    @property
    def since(self) -> global___Since: ...
    @property
    def since_epoch(self) -> global___SinceEpoch: ...
    @property
    def strftime(self) -> global___Strftime: ...
    @property
    def part(self) -> global___Part: ...
    def __init__(
        self,
        *,
        since: global___Since | None = ...,
        since_epoch: global___SinceEpoch | None = ...,
        strftime: global___Strftime | None = ...,
        part: global___Part | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["fn_type", b"fn_type", "part", b"part", "since", b"since", "since_epoch", b"since_epoch", "strftime", b"strftime"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["fn_type", b"fn_type", "part", b"part", "since", b"since", "since_epoch", b"since_epoch", "strftime", b"strftime"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["fn_type", b"fn_type"]) -> typing_extensions.Literal["since", "since_epoch", "strftime", "part"] | None: ...

global___DateTimeOp = DateTimeOp

@typing_extensions.final
class Since(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OTHER_FIELD_NUMBER: builtins.int
    UNIT_FIELD_NUMBER: builtins.int
    @property
    def other(self) -> global___Expr: ...
    unit: global___TimeUnit.ValueType
    def __init__(
        self,
        *,
        other: global___Expr | None = ...,
        unit: global___TimeUnit.ValueType = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["other", b"other"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["other", b"other", "unit", b"unit"]) -> None: ...

global___Since = Since

@typing_extensions.final
class SinceEpoch(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    UNIT_FIELD_NUMBER: builtins.int
    unit: global___TimeUnit.ValueType
    def __init__(
        self,
        *,
        unit: global___TimeUnit.ValueType = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["unit", b"unit"]) -> None: ...

global___SinceEpoch = SinceEpoch

@typing_extensions.final
class Strftime(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FORMAT_FIELD_NUMBER: builtins.int
    format: builtins.str
    def __init__(
        self,
        *,
        format: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["format", b"format"]) -> None: ...

global___Strftime = Strftime

@typing_extensions.final
class Part(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    UNIT_FIELD_NUMBER: builtins.int
    unit: global___TimeUnit.ValueType
    def __init__(
        self,
        *,
        unit: global___TimeUnit.ValueType = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["unit", b"unit"]) -> None: ...

global___Part = Part
