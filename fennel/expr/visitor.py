from typing import List

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
    DictContains,
    Concat,
    DictGet,
    DictLen,
    DictNoop,
)


class Visitor(object):
    def visit(self, obj):
        if isinstance(obj, Literal):
            ret = self.visitLiteral(obj)

        elif isinstance(obj, Ref):
            ret = self.visitRef(obj)

        elif isinstance(obj, Unary):
            ret = self.visitUnary(obj)

        elif isinstance(obj, Binary):
            ret = self.visitBinary(obj)

        elif isinstance(obj, IsNull):
            ret = self.visitIsNull(obj)

        elif isinstance(obj, FillNull):
            ret = self.visitFillNull(obj)

        elif isinstance(obj, When):
            ret = self.visitWhen(obj)

        elif isinstance(obj, Then):
            ret = self.visitThen(obj)

        elif isinstance(obj, Otherwise):
            ret = self.visitOtherwise(obj)

        elif isinstance(obj, _Number):
            ret = self.visitNumber(obj)

        elif isinstance(obj, _Dict):
            ret = self.visitDict(obj)

        elif isinstance(obj, _List):
            ret = self.visitList(obj)

        elif isinstance(obj, _Struct):
            ret = self.visitStruct(obj)

        elif isinstance(obj, _String):
            ret = self.visitString(obj)

        elif isinstance(obj, _Bool):
            ret = self.visitBool(obj)
        else:
            raise InvalidExprException("invalid expression type: %s" % obj)

        return ret

    def visitLiteral(self, obj):
        raise NotImplementedError

    def visitRef(self, obj):
        raise NotImplementedError

    def visitUnary(self, obj):
        raise NotImplementedError

    def visitBinary(self, obj):
        raise NotImplementedError

    def visitIsNull(self, obj):
        raise NotImplementedError

    def visitFillNull(self, obj):
        raise NotImplementedError

    def visitThen(self, obj):
        raise NotImplementedError

    def visitOtherwise(self, obj):
        raise NotImplementedError

    def visitNumber(self, obj):
        raise NotImplementedError

    def visitBool(self, obj):
        raise NotImplementedError

    def visitString(self, obj):
        raise NotImplementedError

    def visitDict(self, obj):
        raise NotImplementedError

    def visitList(self, obj):
        raise NotImplementedError

    def visitWhen(self, obj):
        raise NotImplementedError

    def visitStruct(self, obj):
        raise NotImplementedError


class ExprPrinter(Visitor):

    def print(self, obj):
        return self.visit(obj)

    def visitLiteral(self, obj):
        return obj.c

    def visitRef(self, obj):
        return str(obj)

    def visitUnary(self, obj):
        return "%s(%s)" % (obj.op, self.visit(obj.expr))

    def visitBinary(self, obj):
        return "(%s %s %s)" % (
            self.visit(obj.left),
            obj.op,
            self.visit(obj.right),
        )

    def visitIsNull(self, obj):
        return "IS_NULL(%s)" % self.visit(obj.expr)

    def visitFillNull(self, obj):
        return "FILL_NULL(%s, %s)" % (
            self.visit(obj.expr),
            self.visit(obj.fill),
        )

    def visitWhen(self, obj):
        cur_when = obj
        when_then_pairs: List[When, Then] = []
        while cur_when is not None:
            if cur_when._then is None:
                raise InvalidExprException(
                    f"THEN clause missing for WHEN clause {self.visit(cur_when)}"
                )
            when_then_pairs.append((cur_when, cur_when._then))
            cur_when = cur_when._then._chained_when

        ret = " ".join(
            [
                f"WHEN {self.visit(when.expr)} THEN {self.visit(then.expr)}"
                for when, then in when_then_pairs
            ]
        )
        if when_then_pairs[-1][1]._otherwise is not None:
            ret += f" ELSE {self.visit(when_then_pairs[-1][1]._otherwise.expr)}"
        return ret

    def visitThen(self, obj):
        return f"{self.visit(obj.expr)}"

    def visitOtherwise(self, obj):
        return f"{self.visit(obj.expr)}"

    def visitBool(self, obj):
        return f"{self.visit(obj.expr)}"

    def visitNumber(self, obj):
        if isinstance(obj.op, MathNoop):
            return self.visit(obj.operand)
        elif isinstance(obj.op, Floor):
            return "FLOOR(%s)" % self.visit(obj.operand)
        elif isinstance(obj.op, Round):
            return f"ROUND({self.visit(obj.operand)}, {obj.op.precision})"
        elif isinstance(obj.op, Ceil):
            return "CEIL(%s)" % self.visit(obj.operand)
        elif isinstance(obj.op, Abs):
            return "ABS(%s)" % self.visit(obj.operand)
        else:
            raise InvalidExprException("invalid number operation: %s" % obj.op)

    def visitString(self, obj):
        if isinstance(obj.op, StringNoop):
            return self.visit(obj.operand)
        elif isinstance(obj.op, StrLen):
            return "LEN(%s)" % self.visit(obj.operand)
        elif isinstance(obj.op, Lower):
            return "LOWER(%s)" % self.visit(obj.operand)
        elif isinstance(obj.op, Upper):
            return "UPPER(%s)" % self.visit(obj.operand)
        elif isinstance(obj.op, StrContains):
            return f"CONTAINS({self.visit(obj.operand)}, {self.visit(obj.op.item)})"
        elif isinstance(obj.op, Concat):
            return f"{self.visit(obj.operand)} + {self.visit(obj.op.other)}"
        else:
            raise InvalidExprException("invalid string operation: %s" % obj.op)

    def visitDict(self, obj):
        if isinstance(obj.op, DictNoop):
            return self.visit(obj.expr)
        elif isinstance(obj.op, DictContains):
            return (
                f"CONTAINS({self.visit(obj.expr)}, {self.visit(obj.op.item)})"
            )
        elif isinstance(obj.op, DictGet):
            if obj.op.default is None:
                return f"{self.visit(obj.expr)}.get({self.visit(obj.op.key)})"
            else:
                return f"{self.visit(obj.expr)}.get('{self.visit(obj.op.key)}', {self.visit(obj.op.default)})"
        elif isinstance(obj.op, DictLen):
            return f"LEN({self.visit(obj.expr)})"
