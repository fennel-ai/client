from __future__ import annotations

import inspect

import fennel.gen.dataset_pb2 as proto
from fennel.datasets import Dataset, Pipeline, Visitor
from fennel.lib.schema import get_datatype


class Serializer(Visitor):
    def __init__(self):
        super(Serializer, self).__init__()
        self.proto_by_node_id = {}
        self.nodes = []

    def serialize(self, pipe: Pipeline):
        root_id = self.visit(pipe.terminal_node)
        return root_id, self.nodes

    def visit(self, obj) -> str:
        if isinstance(obj, Dataset):
            if obj._name not in self.proto_by_node_id:
                self.proto_by_node_id[obj._name] = obj
                self.nodes.append(proto.Node(id=obj._name, dataset=obj._name))
            return self.visitDataset(obj)

        node_id = obj.signature()
        if node_id not in self.proto_by_node_id:
            ret = super(Serializer, self).visit(obj)
            self.nodes.append(ret)
            self.proto_by_node_id[node_id] = ret
        return node_id

    def visitDataset(self, obj):
        return obj._name

    def visitTransform(self, obj):
        if obj.schema is None:
            return proto.Node(
                operator=proto.Operator(
                    transform=proto.Transform(
                        operand_node_id=self.visit(obj.node),
                        function=obj.pickled_func,
                        function_source_code=inspect.getsource(obj.func),
                    ),
                ),
                id=obj.signature(),
            )

        return proto.Node(
            operator=proto.Operator(
                transform=proto.Transform(
                    operand_node_id=self.visit(obj.node),
                    function=obj.pickled_func,
                    function_source_code=inspect.getsource(obj.func),
                    schema={
                        col: get_datatype(dtype)
                        for col, dtype in obj.schema.items()
                    },
                ),
            ),
            id=obj.signature(),
        )

    def visitFilter(self, obj):
        return proto.Node(
            operator=proto.Operator(
                filter=proto.Filter(
                    operand_node_id=self.visit(obj.node),
                    function=obj.pickled_func,
                    function_source_code=inspect.getsource(obj.func),
                ),
            ),
            id=obj.signature(),
        )

    def visitAggregate(self, obj):
        return proto.Node(
            operator=proto.Operator(
                aggregate=proto.Aggregate(
                    operand_node_id=self.visit(obj.node),
                    keys=obj.keys,
                    aggregates=[agg.to_proto() for agg in obj.aggregates],
                ),
            ),
            id=obj.signature(),
        )

    def visitJoin(self, obj):
        if obj.on is not None:
            on = {k: k for k in obj.on}
        else:
            on = {l_on: r_on for l_on, r_on in zip(obj.left_on, obj.right_on)}

        if obj.dataset._name not in self.proto_by_node_id:
            self.proto_by_node_id[obj.dataset._name] = obj.dataset
            self.nodes.append(
                proto.Node(id=obj.dataset._name, dataset=obj.dataset._name)
            )

        return proto.Node(
            operator=proto.Operator(
                join=proto.Join(
                    lhs_node_id=self.visit(obj.node),
                    rhs_dataset_name=obj.dataset._name,
                    on=on,
                ),
            ),
            id=obj.signature(),
        )

    def visitUnion(self, obj):
        return proto.Node(
            operator=proto.Operator(
                union=proto.Union(
                    operand_node_ids=[self.visit(node) for node in obj.nodes]
                ),
            ),
            id=obj.signature(),
        )
