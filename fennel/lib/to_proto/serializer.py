from __future__ import annotations

import inspect
from typing import Dict, Any, List

import fennel.gen.dataset_pb2 as proto
import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import Dataset, Pipeline, Visitor
from fennel.lib.schema import get_datatype


class Serializer(Visitor):
    def __init__(self, pipeline: Pipeline):
        super(Serializer, self).__init__()
        self.pipeline_name = pipeline.name
        self.dataset_name = pipeline.dataset_name
        self.terminal_node = pipeline.terminal_node
        self.proto_by_operator_id: Dict[str, Any] = {}
        self.operators: List[Any] = []

    def serialize(self):
        _ = self.visit(self.terminal_node)
        return self.operators

    def visit(self, obj) -> str:
        if isinstance(obj, Dataset):
            # TODO(mohit): ID of the dataset should be random..
            operator_id = obj._name
            if operator_id not in self.proto_by_operator_id:
                ret = self.visitDataset(obj)
                self.operators.append(ret)
                self.proto_by_operator_id[operator_id] = ret
            return operator_id

        node_id = obj.signature()
        if node_id not in self.proto_by_operator_id:
            ret = super(Serializer, self).visit(obj)
            self.operators.append(ret)
            self.proto_by_operator_id[node_id] = ret
        return node_id

    def visitDataset(self, obj):
        # TODO(mohit): ID of the dataset should be random..
        return proto.Operator(
            id=obj._name,
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            dataset_ref=proto.DatasetRef(
                referring_dataset_name=obj._name,
            ),
        )

    def visitTransform(self, obj):
        schema = (
            {col: get_datatype(dtype) for col, dtype in obj.schema.items()}
            if obj.schema is not None
            else None
        )
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            transform=proto.Transform(
                operand_id=self.visit(obj.node),
                schema=schema,
                pycode=pycode_proto.PyCode(
                    pickled=obj.pickled_func,
                    source_code=inspect.getsource(obj.func),
                ),
            ),
        )

    def visitFilter(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            filter=proto.Filter(
                operand_id=self.visit(obj.node),
                pycode=pycode_proto.PyCode(
                    pickled=obj.pickled_func,
                    source_code=inspect.getsource(obj.func),
                ),
            ),
        )

    def visitAggregate(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            aggregate=proto.Aggregate(
                operand_id=self.visit(obj.node),
                keys=obj.keys,
                specs=[agg.to_proto() for agg in obj.aggregates],
            ),
        )

    def visitJoin(self, obj):
        if obj.on is not None:
            on = {k: k for k in obj.on}
        else:
            on = {l_on: r_on for l_on, r_on in zip(obj.left_on, obj.right_on)}

        rhs_operator_id = obj.dataset._name
        if rhs_operator_id not in self.proto_by_operator_id:
            ret = self.visitDataset(obj.dataset)
            self.proto_by_operator_id[rhs_operator_id] = ret
            self.operators.append(ret)

        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            join=proto.Join(
                lhs_operand_id=self.visit(obj.node),
                rhs_dsref_operand_id=rhs_operator_id,
                on=on,
            ),
        )

    def visitUnion(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            union=proto.Union(
                operand_ids=[self.visit(node) for node in obj.nodes]
            ),
        )
