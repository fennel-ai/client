from __future__ import annotations
from enum import Enum
from dataclasses import dataclass
from typing import Any, Callable, Type, Optional

import json

from fennel.dtypes.dtypes import FENNEL_STRUCT


class InvalidExprException(Exception):
    pass


class Expr(object):
    def __init__(self, root=None):
        self.nodeid = id(self)
        self.inline = False
        self.out_edges = []
        self.root = self if root is None else root
        self.dtype = None

    def edge(self, node):
        self.out_edges.append(node)
        
    def astype(self, dtype: Type) -> Expr:
        self.dtype = dtype

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
        raise InvalidExprException(
            "can not convert: '%s' which is part of query graph to bool" % self
        )

    def __bool__(self):
        raise InvalidExprException(
            "can not convert expr: '%s' which is part of query graph to bool"
            % self
        )

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

    def varname(self, dollar: bool = True) -> str:
        name = self.nodeid
        if dollar:
            return f"${name}"
        else:
            return f"{name}"

    def __hash__(self) -> int:
        return self.nodeid

    def num_out_edges(self) -> int:
        return len(self.out_edges)


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
        default = make_expr(default) if default is not None else None
        return _Dict(self, DictGet(key, default))

    def len(self) -> Expr:
        return _Number(_Dict(self, DictLen()), MathNoop())

    def contains(self, field: Expr) -> Expr:
        field = make_expr(field)
        return Expr(_Dict(self, DictContains(field)))


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
        operand.edge(self)
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
        left.edge(self)
        right.edge(self)
        super(Binary, self).__init__()

    def __str__(self) -> str:
        if self.op == "[]":
            return f"{self.left}[{self.right}]"
        else:
            return f"{self.left} {self.op} {self.right}"


class When(Expr):
    def __init__(self, expr: Expr, root: Optional[Expr] = None):
        self.expr = make_expr(expr)
        self.expr.edge(self)
        print("Created when")
        self._then = None
        print("Created when2")
        super(When, self).__init__(self if root is None else root)

    def then(self, expr: Expr) -> Then:
        self._then = Then(expr, self.root)
        return self._then


class Then(Expr):
    def __init__(self, expr: Expr, root: Optional[Expr] = None):
        self.expr = make_expr(expr)
        self.expr.edge(self)
        self._otherwise = None
        self._chained_when = None
        super(Then, self).__init__(self if root is None else root)

    def when(self, expr: Expr) -> When:
        self._chained_when = When(expr, self.root)
        return self._chained_when

    def otherwise(self, expr: Expr) -> Then:
        self._otherwise = Otherwise(make_expr(expr), self.root)
        return self._otherwise


class Otherwise(Expr):

    def __init__(self, expr: Expr, root: Optional[Expr] = None):
        self.expr = make_expr(expr)
        self.expr.edge(self)
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
        return f"Field[`{self._col}`]"


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
    elif isinstance(v, Callable):
        raise f"Functions cannot be converted to an expression, found {v}"
    else:
        return lit(v)


#################################################################
#                   Top level functions                         #
#################################################################


def F(col: str) -> Expr:
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
    else:
        raise "Cannot infer type of literal, please provide type"


def when(expr: Expr) -> When:
    return When(expr)
