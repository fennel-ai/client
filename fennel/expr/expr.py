from __future__ import annotations

from enum import Enum
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, Type, Optional
import pandas as pd

from fennel.dtypes.dtypes import FENNEL_STRUCT, FENNEL_STRUCT_SRC_CODE
from fennel.internal_lib.schema.schema import (
    convert_dtype_to_arrow_type_with_nullable,
    from_proto,
    parse_json,
)
import pyarrow as pa
from fennel_data_lib import assign, type_of, matches

import fennel.gen.schema_pb2 as schema_proto
from fennel.internal_lib.schema import (
    get_datatype,
    cast_col_to_arrow_dtype,
)
from fennel.internal_lib.utils.utils import (
    cast_col_to_pandas,
    is_user_defined_class,
)


class InvalidExprException(Exception):
    pass


class TypedExpr:
    def __init__(self, expr: Expr, dtype: Type):
        self.expr = expr
        self.dtype = dtype


class Expr(object):
    def __init__(self, root=None):
        self.nodeid = id(self)
        self.inline = False
        self.root = self if root is None else root

    def astype(self, dtype: Type) -> TypedExpr:
        return TypedExpr(self, dtype)

    @property
    def num(self):
        return _Number(self, MathNoop())

    @property
    def str(self):
        return _String(self, StringNoop())

    @property
    def dict(self):
        return _Dict(self, DictNoop())

    @property
    def struct(self):
        return _Struct(self, StructNoop())

    @property
    def dt(self):
        return _DateTime(self, DateTimeNoop())

    @property
    def list(self):
        return _List(self, ListNoop())

    def isnull(self):
        return IsNull(self)

    def fillnull(self, value: Any):
        return FillNull(self, value)

    # We also add all the math functions

    def abs(self) -> _Number:
        return _Number(self, Abs())

    def round(self, precision: int = 0) -> _Number:
        if not isinstance(precision, int):
            raise InvalidExprException(
                f"precision can only be an int but got {precision} instead"
            )
        if precision < 0:
            raise InvalidExprException(
                f"precision can only be a positive int but got {precision} instead"
            )
        return _Number(self, Round(precision))

    def ceil(self) -> _Number:
        return _Number(self, Ceil())

    def floor(self) -> _Number:
        return _Number(self, Floor())

    def __getitem__(self, item: Any) -> Expr:
        item = make_expr(item)
        if not isinstance(item, Expr):
            raise InvalidExprException(
                "'[]' operation can only take expression but got '%s'"
                % type(item)
            )
        return Binary(self, "[]", item)

    def __nonzero__(self):
        raise InvalidExprException("can not convert: '%s' to bool" % self)

    def __bool__(self):
        raise InvalidExprException("can not convert: '%s' to bool" % self)

    def __neg__(self) -> Expr:
        return Unary("-", self)

    def __add__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'+' only allowed between expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "+", other)

    def __radd__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'+' only allowed between expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "+", self)

    def __or__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'or' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "or", other)

    def __ror__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'or' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "or", self)

    def __eq__(self, other: Any) -> Expr:  # type: ignore[override]
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'==' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "==", other)

    def __ne__(self, other: Any) -> Expr:  # type: ignore[override]
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'!=' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "!=", other)

    def __ge__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'>=' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, ">=", other)

    def __gt__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'>' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, ">", other)

    def __le__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'<=' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "<=", other)

    def __lt__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'<' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "<", other)

    def __sub__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'-' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "-", other)

    def __rsub__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'-' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "-", self)

    def __mul__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'*' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "*", other)

    def __rmul__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'*' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "*", self)

    def __truediv__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'/' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "/", other)

    def __rtruediv__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'/' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "/", self)

    def __floordiv__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'//' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "//", other)

    def __rfloordiv__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'//' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "//", self)

    def __mod__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'%%' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "%", other)

    def __rmod__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'%%' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "%", self)

    def __and__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'and' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(self, "and", other)

    def __rand__(self, other: Any) -> Expr:
        other = make_expr(other)
        if not isinstance(other, Expr):
            raise InvalidExprException(
                "'and' only allowed between two expressions but got: '%s' instead"
                % other
            )
        return Binary(other, "and", self)

    def __invert__(self) -> Expr:
        return Unary("~", self)

    def __xor__(self, other: Any):
        raise InvalidExprException("binary operation 'xor' not supported")

    def __rxor__(self, other: Any):
        raise InvalidExprException("binary operation 'xor' not supported")

    def __hash__(self) -> int:
        return self.nodeid

    def typeof(self, schema: Dict = None) -> Type:
        schema = schema or {}
        from fennel.expr.serializer import ExprSerializer

        serializer = ExprSerializer()
        proto_expr = serializer.serialize(self.root)
        proto_bytes = proto_expr.SerializeToString()
        proto_schema = {}
        for key, value in schema.items():
            proto_schema[key] = get_datatype(value).SerializeToString()
        type_bytes = type_of(proto_bytes, proto_schema)
        datatype = schema_proto.DataType()
        datatype.ParseFromString(type_bytes)
        return from_proto(datatype)

    def matches_type(self, dtype: Type, schema: Dict) -> bool:
        schema = schema or {}
        from fennel.expr.serializer import ExprSerializer

        serializer = ExprSerializer()
        proto_expr = serializer.serialize(self.root)
        proto_bytes = proto_expr.SerializeToString()
        proto_schema = {}
        for key, value in schema.items():
            proto_schema[key] = get_datatype(value).SerializeToString()
        proto_type_bytes = get_datatype(dtype).SerializeToString()
        return matches(proto_bytes, proto_schema, proto_type_bytes)

    def eval(
        self,
        input_df: pd.DataFrame,
        schema: Dict,
        output_dtype: Optional[Type] = None,
        parse=True,
    ) -> pd.Series:
        from fennel.expr.serializer import ExprSerializer

        def pd_to_pa(pd_data: pd.DataFrame, schema: Dict[str, Type]):
            new_df = pd_data.copy()
            for column, dtype in schema.items():
                dtype = get_datatype(dtype)
                if column not in new_df.columns:
                    raise InvalidExprException(
                        f"column : {column} not found in input dataframe but defined in schema."
                    )
                new_df[column] = cast_col_to_arrow_dtype(new_df[column], dtype)
            new_df = new_df.loc[:, list(schema.keys())]
            fields = []
            for column, dtype in schema.items():
                proto_dtype = get_datatype(dtype)
                pa_type = convert_dtype_to_arrow_type_with_nullable(proto_dtype)
                if proto_dtype.HasField("optional_type"):
                    nullable = True
                else:
                    nullable = False
                field = pa.field(column, type=pa_type, nullable=nullable)
                fields.append(field)
            pa_schema = pa.schema(fields)
            # Replace pd.NA with None
            new_df = new_df.where(pd.notna(new_df), None)
            return pa.RecordBatch.from_pandas(
                new_df, preserve_index=False, schema=pa_schema
            )

        def pa_to_pd(pa_data, ret_type, parse=True):
            ret = pa_data.to_pandas(types_mapper=pd.ArrowDtype)
            ret = cast_col_to_pandas(ret, get_datatype(ret_type))
            if parse:
                ret = ret.apply(lambda x: parse_json(ret_type, x))
            return ret

        serializer = ExprSerializer()
        proto_expr = serializer.serialize(self.root)
        proto_bytes = proto_expr.SerializeToString()
        df_pa = pd_to_pa(input_df, schema)
        proto_schema = {}
        for key, value in schema.items():
            proto_schema[key] = get_datatype(value).SerializeToString()
        if output_dtype is None:
            ret_type = self.typeof(schema)
        else:
            ret_type = output_dtype

        serialized_ret_type = get_datatype(ret_type).SerializeToString()
        arrow_col = assign(
            proto_bytes, df_pa, proto_schema, serialized_ret_type
        )
        return pa_to_pd(arrow_col, ret_type, parse)

    def __str__(self) -> str:  # type: ignore
        from fennel.expr.visitor import ExprPrinter

        printer = ExprPrinter()
        return printer.print(self.root)


class _Bool(Expr):
    def __init__(self, expr: Expr):
        self.expr = expr
        super(_Bool, self).__init__()

    def __str__(self) -> str:
        return f"{self.expr}"


#########################################################
#                   Math Functions
#########################################################


class MathOp:
    pass


@dataclass
class Round(MathOp):
    precision: int


class Abs(MathOp):
    pass


class Ceil(MathOp):
    pass


class Floor(MathOp):
    pass


class MathNoop(MathOp):
    pass


class _Number(Expr):
    def __init__(self, expr: Expr, op: MathOp):
        self.op = op
        self.operand = expr
        super(_Number, self).__init__()

    def abs(self) -> _Number:
        return _Number(self, Abs())

    def round(self, precision: int) -> _Number:  # type: ignore
        return _Number(self, Round(precision))

    def ceil(self) -> _Number:
        return _Number(self, Ceil())

    def floor(self) -> _Number:
        return _Number(self, Floor())


#########################################################
#                   String Functions
#########################################################


class StringOp:
    pass


@dataclass
class StrContains(StringOp):
    item: Expr


@dataclass
class StrStartsWith(StringOp):
    item: Expr


@dataclass
class StrEndsWith(StringOp):
    item: Expr


class Lower(StringOp):
    pass


class Upper(StringOp):
    pass


class StrLen(StringOp):
    pass


class StringNoop(StringOp):
    pass


@dataclass
class StringStrpTime(StringOp):
    format: str
    timezone: Optional[str]


@dataclass
class StringParse(StringOp):
    dtype: Type


@dataclass
class Concat(StringOp):
    other: Expr


class _String(Expr):
    def __init__(self, expr: Expr, op: StringOp):
        self.op = op
        self.operand = expr
        super(_String, self).__init__()

    def lower(self) -> _String:
        return _String(self, Lower())

    def upper(self) -> _String:
        return _String(self, Upper())

    def contains(self, item) -> _Bool:
        item_expr = make_expr(item)
        return _Bool(_String(self, StrContains(item_expr)))

    def concat(self, other: Expr) -> _String:
        other = make_expr(other)
        return _String(self, Concat(other))

    def len(self) -> _Number:
        return _Number(_String(self, StrLen()), MathNoop())

    def strptime(
        self, format: str, timezone: Optional[str] = "UTC"
    ) -> _DateTime:
        return _DateTime(
            _String(self, StringStrpTime(format, timezone)), DateTimeNoop()
        )

    def parse(self, dtype: Type) -> Expr:
        return _String(self, StringParse(dtype))

    def startswith(self, item) -> _Bool:
        item_expr = make_expr(item)
        return _Bool(_String(self, StrStartsWith(item_expr)))

    def endswith(self, item) -> _Bool:
        item_expr = make_expr(item)
        return _Bool(_String(self, StrEndsWith(item_expr)))


#########################################################
#                    Dict Functions
#########################################################


class DictOp:
    pass


class DictLen(DictOp):
    pass


@dataclass
class DictGet(DictOp):
    key: Expr
    default: Optional[Expr]


class DictNoop(DictOp):
    pass


@dataclass
class DictContains(DictOp):
    field: str


class _Dict(Expr):
    def __init__(self, expr: Expr, op: DictOp):
        self.op = op
        self.expr = expr
        super(_Dict, self).__init__()

    def get(self, key: str, default: Optional[Expr] = None) -> Expr:
        key = make_expr(key)
        default = make_expr(default) if default is not None else None  # type: ignore
        return _Dict(self, DictGet(key, default))  # type: ignore

    def len(self) -> Expr:
        return _Number(_Dict(self, DictLen()), MathNoop())

    def contains(self, field: Expr) -> Expr:
        field = make_expr(field)
        return Expr(_Dict(self, DictContains(field)))  # type: ignore


#########################################################
#                    Struct Functions
#########################################################


class StructOp:
    pass


@dataclass
class StructGet(StructOp):
    field: str


class StructNoop(StructOp):
    pass


class _Struct(Expr):
    def __init__(self, expr: Expr, op: DateTimeOp):
        self.op = op
        self.operand = expr
        super(_Struct, self).__init__()

    def get(self, field: str) -> Expr:  # type: ignore
        if not isinstance(field, str):
            raise InvalidExprException(
                f"invalid field access for struct, expected string but got {field}"
            )
        return _Struct(self, StructGet(field))  # type: ignore


#########################################################
#                   DateTime Functions
#########################################################


class DateTimeOp:
    pass


class DateTimeNoop(DateTimeOp):
    pass


class TimeUnit(Enum):
    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"
    MILLISECOND = "millisecond"
    MICROSECOND = "microsecond"

    @staticmethod
    def from_string(time_unit_str: str | TimeUnit) -> TimeUnit:
        if isinstance(time_unit_str, TimeUnit):
            return time_unit_str

        for unit in TimeUnit:
            if unit.value == time_unit_str.lower():
                return unit
        raise ValueError(f"Unknown time unit: {time_unit_str}")


@dataclass
class DateTimeParts(DateTimeOp):
    part: TimeUnit


@dataclass
class DateTimeSince(DateTimeOp):
    other: Expr
    unit: TimeUnit


@dataclass
class DateTimeSinceEpoch(DateTimeOp):
    unit: TimeUnit


@dataclass
class DateTimeStrftime(DateTimeOp):
    format: str


@dataclass
class DateTimeFromEpoch(Expr):
    duration: Expr
    unit: TimeUnit


class _DateTime(Expr):
    def __init__(self, expr: Expr, op: DateTimeOp):
        self.op = op
        self.operand = expr
        super(_DateTime, self).__init__()

    def parts(self, part: TimeUnit) -> _Number:
        part = TimeUnit.from_string(part)
        return _Number(_DateTime(self, DateTimeParts(part)), MathNoop())

    def since(self, other: Expr, unit: TimeUnit) -> _Number:
        unit = TimeUnit.from_string(unit)
        other_expr = make_expr(other)
        return _Number(
            _DateTime(self, DateTimeSince(other_expr, unit)), MathNoop()
        )

    def since_epoch(self, unit: TimeUnit) -> _Number:
        unit = TimeUnit.from_string(unit)
        return _Number(_DateTime(self, DateTimeSinceEpoch(unit)), MathNoop())

    def strftime(self, format: str) -> _String:
        return _String(_DateTime(self, DateTimeStrftime(format)), StringNoop())

    @property
    def year(self) -> _Number:
        return self.parts(TimeUnit.YEAR)

    @property
    def month(self) -> _Number:
        return self.parts(TimeUnit.MONTH)

    @property
    def week(self) -> _Number:
        return self.parts(TimeUnit.WEEK)

    @property
    def day(self) -> _Number:
        return self.parts(TimeUnit.DAY)

    @property
    def hour(self) -> _Number:
        return self.parts(TimeUnit.HOUR)

    @property
    def minute(self) -> _Number:
        return self.parts(TimeUnit.MINUTE)

    @property
    def second(self) -> _Number:
        return self.parts(TimeUnit.SECOND)

    @property
    def millisecond(self) -> _Number:
        return self.parts(TimeUnit.MILLISECOND)

    @property
    def microsecond(self) -> _Number:
        return self.parts(TimeUnit.MICROSECOND)


#########################################################
#                   List Functions
#########################################################


class ListOp:
    pass


class ListLen(ListOp):
    pass


@dataclass
class ListContains(ListOp):
    item: Expr


@dataclass
class ListGet(ListOp):
    index: Expr


class ListHasNull(ListOp):
    pass


class ListNoop(ListOp):
    pass


class _List(Expr):
    def __init__(self, expr: Expr, op: ListOp):
        self.op = op
        self.expr = expr
        super(_List, self).__init__()

    def len(self) -> _Number:
        return _Number(_List(self, ListLen()), MathNoop())

    def contains(self, item: Expr) -> _Bool:
        item_expr = make_expr(item)
        return _Bool(_List(self, ListContains(item_expr)))

    def get(self, index: Expr) -> Expr:
        index_expr = make_expr(index)
        return _List(self, ListGet(index_expr))


#######################################################


class Literal(Expr):
    def __init__(self, c: Any, type: Type):
        super(Literal, self).__init__()
        if getattr(c.__class__, FENNEL_STRUCT, False):
            val = json.dumps(c.as_json())
        else:
            try:
                val = json.dumps(c)
            except TypeError:
                val = json.dumps(str(c))
        self.c = val
        self.dtype = type


class Unary(Expr):
    def __init__(self, op: str, operand: Any):
        valid = ("~", "-")
        if op not in valid:
            raise InvalidExprException(
                "unary expressions only support %s but given '%s'"
                % (", ".join(valid), op)
            )
        operand = make_expr(operand)
        if not isinstance(operand, Expr):
            raise InvalidExprException(
                "operand can only be an expression but got %s instead" % operand
            )
        self.op = op
        self.operand = operand
        super(Unary, self).__init__()

    def __str__(self) -> str:
        return f"{self.op} {self.operand}"


class Binary(Expr):
    def __init__(self, left: Any, op: str, right: Any):
        valid = (
            "+",
            "-",
            "*",
            "/",
            "//",
            "%",
            "and",
            "or",
            "==",
            ">=",
            ">",
            "<",
            "<=",
            "!=",
            "[]",
            "in",
        )
        if op not in valid:
            raise InvalidExprException(
                "binary expressions only support %s but given '%s'"
                % (", ".join(valid), op)
            )
        left = make_expr(left)
        right = make_expr(right)
        if not isinstance(left, Expr):
            raise InvalidExprException(
                "left can only be an expression but got %s instead" % left
            )
        if not isinstance(right, Expr):
            raise InvalidExprException(
                "right can only be an expression but got %s instead" % right
            )
        self.left = left
        self.op = op
        self.right = right

        super(Binary, self).__init__(None)

    def __str__(self) -> str:
        if self.op == "[]":
            return f"{self.left}[{self.right}]"
        else:
            return f"{self.left} {self.op} {self.right}"


class When(Expr):
    def __init__(self, expr: Expr, root: Optional[Expr] = None):
        self.expr = make_expr(expr)
        self._then = None
        super(When, self).__init__(self if root is None else root)

    def then(self, expr: Expr) -> Then:
        self._then = Then(expr, self.root)  # type: ignore
        return self._then  # type: ignore


class Then(Expr):
    def __init__(self, expr: Expr, root: Optional[Expr] = None):
        self.expr = make_expr(expr)
        self._otherwise = None
        self._chained_when = None
        super(Then, self).__init__(self if root is None else root)

    def when(self, expr: Expr) -> When:
        self._chained_when = When(expr, self.root)  # type: ignore
        return self._chained_when  # type: ignore

    def otherwise(self, expr: Expr) -> Otherwise:
        self._otherwise = Otherwise(make_expr(expr), self.root)  # type: ignore
        return self._otherwise  # type: ignore


class Otherwise(Expr):

    def __init__(self, expr: Expr, root: Optional[Expr] = None):
        self.expr = make_expr(expr)
        super(Otherwise, self).__init__(self if root is None else root)


class Ref(Expr):
    def __init__(self, col: str):
        if not isinstance(col, str):
            raise InvalidExprException(
                f"column name can only be a string but got {col} instead"
            )
        self._col = col
        super(Ref, self).__init__()

    def __str__(self) -> str:
        return f"col('{self._col}')"


class IsNull(Expr):
    def __init__(self, expr: Expr):
        self.expr = expr
        super(IsNull, self).__init__()

    def __str__(self) -> str:
        return f"{self.expr} is null"


class FillNull(Expr):
    def __init__(self, expr: Expr, value: Any):
        self.expr = expr
        super(FillNull, self).__init__()
        self.fill = make_expr(value)

    def __str__(self) -> str:
        return f"fillnull({self.expr}, {self.fill})"


class MakeStruct(Expr):
    def __init__(self, fields: Dict[str, Expr], type: Type):
        self.fields = fields
        self.dtype = type
        super(MakeStruct, self).__init__()


def make_expr(v: Any) -> Any:
    """Tries to convert v to Expr. Throws an exception if conversion is not possible."""
    if isinstance(v, Expr):
        return v
    elif isinstance(v, Callable):  # type: ignore
        raise TypeError(
            "Functions cannot be converted to an expression, found {v}"
        )
    elif isinstance(v, TypedExpr):
        raise TypeError(
            "astype() must be used as a standalone operation on the entire expression, syntax: (<expr>).astype(<type>). It cannot be combined with other expressions."
        )
    else:
        return lit(v)


#################################################################
#                   Top level functions                         #
#################################################################


def col(col: str) -> Expr:
    return Ref(col)


def lit(v: Any, type: Optional[Type] = None) -> Expr:
    # TODO: Add support for more types recursively
    if type is not None:
        return Literal(v, type)
    elif isinstance(v, bool):
        return Literal(v, bool)
    elif isinstance(v, int):
        return Literal(v, int)
    elif isinstance(v, float):
        return Literal(v, float)
    elif isinstance(v, str):
        return Literal(v, str)
    elif v is None:
        return Literal(v, None)  # type: ignore
    else:
        raise Exception(
            f"Cannot infer type of literal {v}, please provide type"
        )


def when(expr: Expr) -> When:
    return When(expr)


def make_struct(fields: Dict[str, Expr], type: Type) -> Expr:
    fields = {k: make_expr(v) for k, v in fields.items()}
    return MakeStruct(fields, type)


def from_epoch(duration: Expr, unit: str | TimeUnit) -> _DateTime:
    duration = make_expr(duration)
    unit = TimeUnit.from_string(unit)
    return _DateTime(DateTimeFromEpoch(duration, unit), DateTimeNoop())
