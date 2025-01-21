from typing import Any, List
import json

from fennel.dtypes.dtypes import FENNEL_STRUCT

from .visitor import Visitor
import fennel.gen.expression_pb2 as proto
from fennel.internal_lib.schema import get_datatype

from fennel.expr.expr import (
    DateTimeParts,
    DateTimeSince,
    DateTimeSinceEpoch,
    DateTimeStrftime,
    ListContains,
    ListGet,
    ListHasNull,
    ListLen,
    ListNoop,
    ListSum,
    ListMean,
    ListMin,
    ListMax,
    ListAll,
    ListAny,
    ListFilter,
    ListMap,
    Literal,
    Ref,
    StructGet,
    StructNoop,
    TimeUnit,
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
    Sqrt,
    Pow,
    Log,
    NumToStr,
    Abs,
    Floor,
    Sin,
    Cos,
    Tan,
    ArcSin,
    ArcCos,
    ArcTan,
    StringNoop,
    StrLen,
    StringStrpTime,
    StringParse,
    StrStartsWith,
    StringSplit,
    StringJsonExtract,
    StrEndsWith,
    Lower,
    Upper,
    StrContains,
    Concat,
    DictContains,
    DictGet,
    DictLen,
    DictNoop,
    DateTimeNoop,
    Zip,
    Repeat,
    IsNan,
    IsInfinite,
)


def time_unit_to_proto(unit: TimeUnit) -> proto.TimeUnit:
    if unit == TimeUnit.MICROSECOND:
        return proto.TimeUnit.MICROSECOND
    elif unit == TimeUnit.MILLISECOND:
        return proto.TimeUnit.MILLISECOND
    elif unit == TimeUnit.SECOND:
        return proto.TimeUnit.SECOND
    elif unit == TimeUnit.MINUTE:
        return proto.TimeUnit.MINUTE
    elif unit == TimeUnit.HOUR:
        return proto.TimeUnit.HOUR
    elif unit == TimeUnit.DAY:
        return proto.TimeUnit.DAY
    elif unit == TimeUnit.WEEK:
        return proto.TimeUnit.WEEK
    elif unit == TimeUnit.MONTH:
        return proto.TimeUnit.MONTH
    elif unit == TimeUnit.YEAR:
        return proto.TimeUnit.YEAR
    raise InvalidExprException("invalid time unit: %s" % unit)


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

    def visitVar(self, obj):
        expr = proto.Expr()
        expr.var.name = obj.var
        return expr

    def visitUnary(self, obj):
        expr = proto.Expr()
        if obj.op == "~":
            expr.unary.op = proto.UnaryOp.NOT
        elif obj.op == "-":
            expr.unary.op = proto.UnaryOp.NEG
        else:
            raise Exception("invalid unary operation: %s" % obj.op)
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
        expr = proto.Expr()
        expr.fillnull.fill.CopyFrom(self.visit(obj.fill))
        expr.fillnull.operand.CopyFrom(self.visit(obj.expr))
        return expr

    def visitWhen(self, obj):
        expr = proto.Expr()
        case = proto.Case()
        cur_when = obj
        when_then_pairs: List[When, Then] = []
        while cur_when is not None:
            if cur_when._then is None:
                raise InvalidExprException(
                    f"THEN clause missing for WHEN clause {cur_when.expr}"
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
        elif isinstance(obj.op, NumToStr):
            expr.math_fn.fn.CopyFrom(proto.MathOp(to_string=proto.ToString()))
        elif isinstance(obj.op, Sqrt):
            expr.math_fn.fn.CopyFrom(proto.MathOp(sqrt=proto.Sqrt()))
        elif isinstance(obj.op, Pow):
            expr.math_fn.fn.CopyFrom(
                proto.MathOp(
                    pow=proto.Pow(exponent=self.visit(obj.op.exponent))
                )
            )
        elif isinstance(obj.op, Log):
            expr.math_fn.fn.CopyFrom(
                proto.MathOp(log=proto.Log(base=obj.op.base))
            )
        elif isinstance(obj.op, Sin):
            expr.math_fn.fn.CopyFrom(proto.MathOp(sin=proto.Sin()))
        elif isinstance(obj.op, Cos):
            expr.math_fn.fn.CopyFrom(proto.MathOp(cos=proto.Cos()))
        elif isinstance(obj.op, Tan):
            expr.math_fn.fn.CopyFrom(proto.MathOp(tan=proto.Tan()))
        elif isinstance(obj.op, ArcSin):
            expr.math_fn.fn.CopyFrom(proto.MathOp(asin=proto.Asin()))
        elif isinstance(obj.op, ArcCos):
            expr.math_fn.fn.CopyFrom(proto.MathOp(acos=proto.Acos()))
        elif isinstance(obj.op, ArcTan):
            expr.math_fn.fn.CopyFrom(proto.MathOp(atan=proto.Atan()))
        elif isinstance(obj.op, IsNan):
            expr.math_fn.fn.CopyFrom(proto.MathOp(is_nan=proto.IsNan()))
        elif isinstance(obj.op, IsInfinite):
            expr.math_fn.fn.CopyFrom(
                proto.MathOp(is_infinite=proto.IsInfinite())
            )
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
        elif isinstance(obj.op, StringStrpTime):
            if obj.op.timezone is not None:
                expr.string_fn.fn.CopyFrom(
                    proto.StringOp(
                        strptime=proto.Strptime(
                            format=obj.op.format,
                            timezone=proto.Timezone(timezone=obj.op.timezone),
                        )
                    )
                )
            else:
                expr.string_fn.fn.CopyFrom(
                    proto.StringOp(
                        strptime=proto.Strptime(format=obj.op.format)
                    )
                )
        elif isinstance(obj.op, StringParse):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(
                    json_decode=proto.JsonDecode(
                        dtype=get_datatype(obj.op.dtype)
                    )
                )
            )
        elif isinstance(obj.op, StrStartsWith):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(
                    startswith=proto.StartsWith(key=self.visit(obj.op.item))
                )
            )
        elif isinstance(obj.op, StrEndsWith):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(
                    endswith=proto.EndsWith(key=self.visit(obj.op.item))
                )
            )
        elif isinstance(obj.op, StringJsonExtract):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(json_extract=proto.JsonExtract(path=obj.op.path))
            )
        elif isinstance(obj.op, StringSplit):
            expr.string_fn.fn.CopyFrom(
                proto.StringOp(split=proto.Split(sep=obj.op.sep))
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

    def visitDateTime(self, obj):
        expr = proto.Expr()
        if isinstance(obj.op, DateTimeNoop):
            return self.visit(obj.operand)
        elif isinstance(obj.op, DateTimeParts):
            part = proto.Part(
                unit=time_unit_to_proto(obj.op.part),
                timezone=(
                    proto.Timezone(timezone=obj.op.timezone)
                    if obj.op.timezone is not None
                    else None
                ),
            )
            expr.datetime_fn.fn.CopyFrom(proto.DateTimeOp(part=part))
        elif isinstance(obj.op, DateTimeSince):
            expr.datetime_fn.fn.CopyFrom(
                proto.DateTimeOp(
                    since=proto.Since(
                        other=self.visit(obj.op.other),
                        unit=time_unit_to_proto(obj.op.unit),
                    )
                )
            )
        elif isinstance(obj.op, DateTimeSinceEpoch):
            expr.datetime_fn.fn.CopyFrom(
                proto.DateTimeOp(
                    since_epoch=proto.SinceEpoch(
                        unit=time_unit_to_proto(obj.op.unit)
                    )
                )
            )
        elif isinstance(obj.op, DateTimeStrftime):
            expr.datetime_fn.fn.CopyFrom(
                proto.DateTimeOp(
                    strftime=proto.Strftime(
                        format=obj.op.format,
                        timezone=(
                            proto.Timezone(timezone=obj.op.timezone)
                            if obj.op.timezone is not None
                            else None
                        ),
                    )
                )
            )
        else:
            raise InvalidExprException(
                "invalid datetime operation: %s" % obj.op
            )
        expr.datetime_fn.datetime.CopyFrom(self.visit(obj.operand))
        return expr

    def visitList(self, obj):
        expr = proto.Expr()
        if isinstance(obj.op, ListNoop):
            return self.visit(obj.expr)
        elif isinstance(obj.op, ListContains):
            expr.list_fn.fn.CopyFrom(
                proto.ListOp(
                    contains=proto.Contains(element=self.visit(obj.op.item))
                )
            )
        elif isinstance(obj.op, ListGet):
            expr.list_fn.fn.CopyFrom(
                proto.ListOp(
                    get=self.visit(obj.op.index),
                )
            )
        elif isinstance(obj.op, ListLen):
            expr.list_fn.fn.CopyFrom(proto.ListOp(len=proto.Len()))
        elif isinstance(obj.op, ListHasNull):
            expr.list_fn.fn.CopyFrom(proto.ListOp(has_null=proto.HasNull()))
        elif isinstance(obj.op, ListSum):
            expr.list_fn.fn.CopyFrom(proto.ListOp(sum=proto.ListSum()))
        elif isinstance(obj.op, ListMean):
            expr.list_fn.fn.CopyFrom(proto.ListOp(mean=proto.ListMean()))
        elif isinstance(obj.op, ListMin):
            expr.list_fn.fn.CopyFrom(proto.ListOp(min=proto.ListMin()))
        elif isinstance(obj.op, ListMax):
            expr.list_fn.fn.CopyFrom(proto.ListOp(max=proto.ListMax()))
        elif isinstance(obj.op, ListAll):
            expr.list_fn.fn.CopyFrom(proto.ListOp(all=proto.ListAll()))
        elif isinstance(obj.op, ListAny):
            expr.list_fn.fn.CopyFrom(proto.ListOp(any=proto.ListAny()))
        elif isinstance(obj.op, ListFilter):
            expr.list_fn.fn.CopyFrom(
                proto.ListOp(
                    filter=proto.ListFilter(
                        var=obj.op.var, predicate=self.visit(obj.op.predicate)
                    )
                )
            )
        elif isinstance(obj.op, ListMap):
            expr.list_fn.fn.CopyFrom(
                proto.ListOp(
                    map=proto.ListMap(
                        var=obj.op.var, map_expr=self.visit(obj.op.expr)
                    )
                )
            )

        expr.list_fn.list.CopyFrom(self.visit(obj.expr))
        return expr

    def visitStruct(self, obj):
        expr = proto.Expr()
        if isinstance(obj.op, StructNoop):
            return self.visit(obj.operand)
        elif isinstance(obj.op, StructGet):
            expr.struct_fn.fn.CopyFrom(proto.StructOp(field=obj.op.field))
        else:
            raise InvalidExprException("invalid struct operation: %s" % obj.op)
        expr.struct_fn.struct.CopyFrom(self.visit(obj.operand))
        return expr

    def visitMakeStruct(self, obj):
        expr = proto.Expr()
        for field, value in obj.fields.items():
            field_expr = expr.make_struct.fields.get_or_create(field)
            field_expr.CopyFrom(self.visit(value))

        # Ensure get_datatype returns a correct protobuf message of type StructType
        dtype = get_datatype(obj.dtype)
        if dtype.struct_type is None:
            raise InvalidExprException(
                "Expected struct_type to be a StructType, found {}".format(
                    dtype
                )
            )
        expr.make_struct.struct_type.CopyFrom(dtype.struct_type)
        return expr

    def visitDateTimeFromEpoch(self, obj):
        expr = proto.Expr()
        from_epoch = proto.FromEpoch()
        from_epoch.unit = time_unit_to_proto(obj.unit)
        from_epoch.duration.CopyFrom(self.visit(obj.duration))
        expr.from_epoch.CopyFrom(from_epoch)
        return expr

    def visitDateTimeLiteral(self, obj):
        expr = proto.Expr()
        datetime_literal = proto.DatetimeLiteral()
        datetime_literal.year = obj.year
        datetime_literal.month = obj.month
        datetime_literal.day = obj.day
        datetime_literal.hour = obj.hour
        datetime_literal.minute = obj.minute
        datetime_literal.second = obj.second
        datetime_literal.microsecond = obj.microsecond
        if obj.timezone is not None:
            datetime_literal.timezone.CopyFrom(
                proto.Timezone(timezone=obj.timezone)
            )
        expr.datetime_literal.CopyFrom(datetime_literal)
        return expr

    def visitNow(self, obj):
        expr = proto.Expr()
        expr.now.CopyFrom(proto.Now())
        return expr

    def visitZip(self, obj):
        expr = proto.Expr()
        for field, value in obj.fields.items():
            field_expr = expr.zip.fields.get_or_create(field)
            field_expr.CopyFrom(self.visit(value))

        # Ensure get_datatype returns a correct protobuf message of type StructType
        dtype = get_datatype(obj.struct_type)
        if dtype.struct_type is None:
            raise InvalidExprException(
                "invalid zip: expected struct_type to be a StructType, found {}".format(
                    dtype
                )
            )
        expr.zip.struct_type.CopyFrom(dtype.struct_type)
        return expr

    def visitRepeat(self, obj):
        expr = proto.Expr()
        expr.repeat.CopyFrom(
            proto.Repeat(expr=self.visit(obj.value), count=self.visit(obj.by))
        )
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
