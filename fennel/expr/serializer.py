from typing import Any, List
import json

from fennel.dtypes.dtypes import FENNEL_STRUCT

from .visitor import Visitor
import fennel.gen.expr_pb2 as proto
from fennel.internal_lib.schema import get_datatype

from fennel.expr.expr import (
    Literal,
    Ref,
    Unary,
    When,
    Then,
    Otherwise,
    Binary,
    IsNull,
    FillNull,
    _Bool,
    _Dict,
    _Struct,
    _List,
    _Number,
    _String,
    InvalidExprException,
    MathNoop,
    Round,
    Ceil,
    Abs,
    Floor,
    StringNoop,
    StrLen,
    Lower,
    Upper,
    StrContains,
    Concat,
    DictContains,
    DictGet,
    DictLen,
    DictNoop,
)


class ExprSerializer(Visitor):
    def __init__(self):
        super(ExprSerializer, self).__init__()

    def visit(self, obj):
        ret = super(ExprSerializer, self).visit(obj)
        return ret

    def serialize(self, obj, second_pass=False):
        return self.visit(obj)

    def visitLiteral(self, obj):
        expr = proto.Expr()
        val = val_as_json(obj.c)
        expr.json_literal.literal = val
        expr.json_literal.dtype.CopyFrom(get_datatype(obj.dtype))
        return expr

    def visitRef(self, obj):
        expr = proto.Expr()
        expr.ref.name = obj._col
        return expr

    def visitUnary(self, obj):
        expr = proto.Expr()
        expr.unary.op = obj.op
        operand = self.visit(obj.operand)
        expr.unary.operand.CopyFrom(operand)
        return expr

    def visitBinary(self, obj):
        expr = proto.Expr()
        if obj.op == "and":
            expr.binary.op = proto.BinOp.AND
        elif obj.op == "or":
            expr.binary.op = proto.BinOp.OR
        elif obj.op == "+":
            expr.binary.op = proto.BinOp.ADD
        elif obj.op == "-":
            expr.binary.op = proto.BinOp.SUB
        elif obj.op == "*":
            expr.binary.op = proto.BinOp.MUL
        elif obj.op == "/":
            expr.binary.op = proto.BinOp.DIV
        elif obj.op == "//":
            expr.binary.op = proto.BinOp.FLOOR_DIV
        elif obj.op == "%":
            expr.binary.op = proto.BinOp.MOD
        elif obj.op == "==":
            expr.binary.op = proto.BinOp.EQ
        elif obj.op == "!=":
            expr.binary.op = proto.BinOp.NE
        elif obj.op == ">":
            expr.binary.op = proto.BinOp.GT
        elif obj.op == "<":
            expr.binary.op = proto.BinOp.LT
        elif obj.op == ">=":
            expr.binary.op = proto.BinOp.GTE
        elif obj.op == "<=":
            expr.binary.op = proto.BinOp.LTE
        else:
            raise InvalidExprException("invalid binary operation: %s" % obj.op)
        left = self.visit(obj.left)
        right = self.visit(obj.right)
        expr.binary.left.CopyFrom(left)
        expr.binary.right.CopyFrom(right)
        return expr

    def visitIsNull(self, obj):
        expr = proto.Expr()
        expr.isnull.operand.CopyFrom(self.visit(obj.expr))
        return expr

    def visitFillNull(self, obj):
        return "FILL NULL(%s, %s)" % (
            self.visit(obj.expr),
            self.visit(obj.fill),
        )

    def visitWhen(self, obj):
        expr = proto.Expr()
        case = proto.Case()
        cur_when = obj
        when_then_pairs: List[When, Then] = []
        while cur_when is not None:
            if cur_when._then is None:
                raise InvalidExprException(
                    f"THEN clause missing for WHEN clause {self.visit(cur_when)}"
                )
            when_then_pairs.append((cur_when, cur_when._then))
            cur_when = cur_when._then._chained_when

        case.when_then.extend(
            [
                proto.WhenThen(
                    when=self.visit(when.expr), then=self.visit(then.expr)
                )
                for when, then in when_then_pairs
            ]
        )
        if when_then_pairs[-1][1]._otherwise is not None:
            case.otherwise.CopyFrom(
                self.visit(when_then_pairs[-1][1]._otherwise.expr)
            )
        expr.case.CopyFrom(case)
        return expr

    def visitThen(self, obj):
        return self.visit(obj.expr)

    def visitOtherwise(self, obj):
        return self.visit(obj.expr)

    def visitBool(self, obj):
        return self.visit(obj.expr)

    def visitNumber(self, obj):
        expr = proto.Expr()
        if isinstance(obj.op, MathNoop):
            return self.visit(obj.operand)
        elif isinstance(obj.op, Round):
            expr.math_fn.fn.CopyFrom(
                proto.MathOp(round=proto.Round(precision=obj.op.precision))
            )
        elif isinstance(obj.op, Ceil):
            expr.math_fn.fn.CopyFrom(proto.MathOp(ceil=proto.Ceil()))
        elif isinstance(obj.op, Abs):
            expr.math_fn.fn.CopyFrom(proto.MathOp(abs=proto.Abs()))
        elif isinstance(obj.op, Floor):
            expr.math_fn.fn.CopyFrom(proto.MathOp(floor=proto.Floor()))
        else:
            raise InvalidExprException("invalid number operation: %s" % obj.op)
        expr.math_fn.operand.CopyFrom(self.visit(obj.operand))
        return expr

    def visitString(self, obj):
        expr = proto.Expr()
        if isinstance(obj.op, StringNoop):
            return self.visit(obj.operand)
        elif isinstance(obj.op, StrLen):
            expr.string_fn.fn.CopyFrom(proto.StringOp(len=proto.Len()))
        elif isinstance(obj.op, Lower):
            expr.string_fn.fn.CopyFrom(proto.StringOp(tolower=proto.ToLower()))
        elif isinstance(obj.op, Upper):
            expr.string_fn.fn.CopyFrom(proto.StringOp(toupper=proto.ToUpper()))
        elif isinstance(obj.op, StrContains):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(
                    contains=proto.Contains(element=self.visit(obj.op.item))
                )
            )
        elif isinstance(obj.op, Concat):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(
                    concat=proto.Concat(
                        other=self.visit(obj.op.other),
                    )
                )
            )
        else:
            raise InvalidExprException("invalid string operation: %s" % obj.op)
        expr.string_fn.string.CopyFrom(self.visit(obj.operand))
        return expr

    def visitDict(self, obj):
        expr = proto.Expr()
        if isinstance(obj.op, DictNoop):
            return self.visit(obj.expr)
        elif isinstance(obj.op, DictContains):
            expr.dict_fn.fn.CopyFrom(
                proto.DictOp(
                    contains=proto.Contains(element=self.visit(obj.op.item))
                )
            )
        elif isinstance(obj.op, DictGet):
            expr.dict_fn.fn.CopyFrom(
                proto.DictOp(
                    get=proto.DictGet(
                        field=self.visit(obj.op.key),
                        default_value=(
                            self.visit(obj.op.default)
                            if obj.op.default is not None
                            else None
                        ),
                    )
                )
            )
        elif isinstance(obj.op, DictLen):
            expr.dict_fn.fn.CopyFrom(proto.DictOp(len=proto.Len()))
        else:
            raise InvalidExprException("invalid dict operation: %s" % obj.op)
        expr.dict_fn.dict.CopyFrom(self.visit(obj.expr))
        return expr


def val_as_json(val: Any) -> str:
    if isinstance(val, str):
        return val
    if getattr(val.__class__, FENNEL_STRUCT, False):
        return json.dumps(val.as_json())
    try:
        return json.dumps(val)
    except TypeError:
        return json.dumps(str(val))
