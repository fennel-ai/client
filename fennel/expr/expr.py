from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from typing import Any, Callable, Dict, Type, Optional

import json

from fennel.dtypes.dtypes import FENNEL_STRUCT
import pandas as pd
from fennel.internal_lib.schema.schema import from_proto
import pyarrow as pa
from fennel_data_lib import eval, type_of
from fennel.internal_lib.schema import get_datatype
import fennel.gen.schema_pb2 as schema_proto


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

    def round(self, precision: int) -> _Number:
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

    def typeof(self, schema: Dict) -> Type:
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

    def eval(self, input_df: pd.DataFrame, schema: Dict) -> pd.Series:
        from fennel.expr.serializer import ExprSerializer

        def convert_object(obj):
            if isinstance(obj, list):
                result = [convert_object(i) for i in obj]
            elif isinstance(obj, dict):
                result = {}
                for key in obj:
                    result[key] = convert_object(obj[key])
            elif hasattr(obj, "as_json"):
                result = obj.as_json()
            else:
                result = obj
            return result

        def convert_objects(df):
            for col in df.columns:
                df[col] = df[col].apply(convert_object)
            return df

        def pd_to_pa(pd_data, schema=None):
            # Schema unspecified - as in the case with lookups
            if not schema:
                if isinstance(pd_data, pd.Series):
                    pd_data = pd_data.apply(convert_object)
                    return pa.Array.from_pandas(pd_data)
                elif isinstance(pd_data, pd.DataFrame):
                    pd_data = convert_objects(pd_data)
                    return pa.RecordBatch.from_pandas(
                        pd_data, preserve_index=False
                    )
                else:
                    raise ValueError("only pd.Series or pd.Dataframe expected")

            # Single column expected
            if isinstance(schema, pa.Field):
                # extra columns may have been provided
                if isinstance(pd_data, pd.DataFrame):
                    if schema.name not in pd_data:
                        raise ValueError(
                            f"Dataframe does not contain column {schema.name}"
                        )
                    # df -> series
                    pd_data = pd_data[schema.name]

                if not isinstance(pd_data, pd.Series):
                    raise ValueError("only pd.Series or pd.Dataframe expected")
                pd_data = pd_data.apply(convert_object)
                return pa.Array.from_pandas(pd_data, type=schema.type)

            # Multiple columns case: use the columns we need
            result_df = pd.DataFrame()
            for col in schema.names:
                if col not in pd_data:
                    raise ValueError(f"Dataframe does not contain column {col}")
                result_df[col] = pd_data[col].apply(convert_object)
            return pa.RecordBatch.from_pandas(
                result_df, preserve_index=False, schema=schema
            )

        def pa_to_pd(pa_data):
            return pa_data.to_pandas(types_mapper=pd.ArrowDtype)

        serializer = ExprSerializer()
        proto_expr = serializer.serialize(self.root)
        proto_bytes = proto_expr.SerializeToString()
        df_pa = pd_to_pa(input_df)
        proto_schema = {}
        for key, value in schema.items():
            proto_schema[key] = get_datatype(value).SerializeToString()
        arrow_col = eval(proto_bytes, df_pa, proto_schema)
        return pa_to_pd(arrow_col)

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

    def round(self, precision: int) -> _Number:
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


class Lower(StringOp):
    pass


class Upper(StringOp):
    pass


class StrLen(StringOp):
    pass


class StringNoop(StringOp):
    pass


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
    key: Expr


class StructNoop(StructOp):
    pass


class _Struct(Expr):
    pass


#########################################################
#                   DateTime Functions
#########################################################


class DateTimeOp:
    pass


class DateTimeNoop(DateTimeOp):
    pass


class _DateTime(Expr):
    pass


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


class ListNoop(ListOp):
    pass


class _List(Expr):
    pass


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
        valid = ("~", "len", "str")
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
        if self.op in ["len", "str"]:
            return f"{self.op}({self.operand})"
        else:
            return f"{self.op}{self.operand}"


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
        self.value = make_expr(value)

    def __str__(self) -> str:
        return f"fillnull({self.expr}, {self.value})"


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
    elif isinstance(v, int):
        return Literal(v, int)
    elif isinstance(v, float):
        return Literal(v, float)
    elif isinstance(v, str):
        return Literal(v, str)
    elif isinstance(v, bool):
        return Literal(v, bool)
    elif v is None:
        return Literal(v, None)  # type: ignore
    else:
        raise Exception(
            f"Cannot infer type of literal {v}, please provide type"
        )


def when(expr: Expr) -> When:
    return When(expr)
