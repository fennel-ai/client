from __future__ import annotations

from fennel.datasets import Dataset
from fennel.datasets import (
    Transform,
    Filter,
    Join,
    Union_,
    GroupBy,
    Aggregate,
)


class Visitor:
    def visit(self, obj):
        if isinstance(obj, Dataset):
            return self.visitDataset(obj)
        elif isinstance(obj, Transform):
            return self.visitTransform(obj)
        elif isinstance(obj, Filter):
            return self.visitFilter(obj)
        elif isinstance(obj, GroupBy):
            return self.visitGroupBy(obj)
        elif isinstance(obj, Aggregate):
            return self.visitAggregate(obj)
        elif isinstance(obj, Join):
            return self.visitJoin(obj)
        elif isinstance(obj, Union_):
            return self.visitUnion(obj)
        else:
            raise Exception("invalid node type: %s" % obj)

    def visitDataset(self, obj):
        raise NotImplementedError()

    def visitTransform(self, obj):
        raise NotImplementedError()

    def visitFilter(self, obj):
        raise NotImplementedError()

    def visitGroupBy(self, obj):
        raise Exception(f"group by object {obj} must be aggregated")

    def visitAggregate(self, obj):
        return NotImplementedError()

    def visitJoin(self, obj):
        raise NotImplementedError()

    def visitUnion(self, obj):
        raise NotImplementedError()
