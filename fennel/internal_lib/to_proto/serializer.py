from __future__ import annotations

from textwrap import dedent
from typing import Dict, Any, List

import google.protobuf.duration_pb2 as duration_proto

import fennel.gen.dataset_pb2 as proto
from fennel.datasets import Dataset, Pipeline, Visitor
from fennel.datasets.datasets import EmitStrategy
from fennel.internal_lib.duration import (
    duration_to_timedelta,
)
from fennel.internal_lib.schema import get_datatype
from fennel.internal_lib.to_proto.source_code import (
    to_includes_proto,
    get_dataset_core_code,
    wrap_function,
)
from fennel.lib.includes import FENNEL_INCLUDED_MOD


class Serializer(Visitor):
    def __init__(self, pipeline: Pipeline, dataset: Dataset):
        super(Serializer, self).__init__()
        self.pipeline_name = pipeline.name
        self.dataset_name = pipeline.dataset_name
        self.terminal_node = pipeline.terminal_node
        self.proto_by_operator_id: Dict[str, Any] = {}
        self.operators: List[Any] = []
        # Get all includes from the pipeline
        gen_code = ""
        if hasattr(pipeline.func, FENNEL_INCLUDED_MOD):
            for f in getattr(pipeline.func, FENNEL_INCLUDED_MOD):
                dep = to_includes_proto(f)
                gen_code = "\n" + dedent(dep.generated_code) + "\n" + gen_code
        self.lib_generated_code = gen_code
        self.dataset_code = get_dataset_core_code(dataset)
        self.dataset_name = dataset._name
        self.dataset_version = dataset._version

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
            ds_version=self.dataset_version,
            dataset_ref=proto.DatasetRef(
                referring_dataset_name=obj._name,
            ),
        )

    def visitTransform(self, obj):
        schema = (
            {col: get_datatype(dtype) for col, dtype in obj.new_schema.items()}
            if obj.new_schema is not None
            else None
        )
        transform_func_pycode = to_includes_proto(obj.func)
        gen_pycode = wrap_function(
            self.dataset_name,
            self.dataset_code,
            self.lib_generated_code,
            transform_func_pycode,
        )
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            transform=proto.Transform(
                operand_id=self.visit(obj.node),
                schema=schema,
                pycode=gen_pycode,
            ),
        )

    def visitFilter(self, obj):
        filter_func_pycode = to_includes_proto(obj.func)
        gen_pycode = wrap_function(
            self.dataset_name,
            self.dataset_code,
            self.lib_generated_code,
            filter_func_pycode,
            is_filter=True,
        )
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            filter=proto.Filter(
                operand_id=self.visit(obj.node),
                pycode=gen_pycode,
            ),
        )

    def visitAssign(self, obj):
        assign_func_pycode = to_includes_proto(obj.func)
        gen_pycode = wrap_function(
            self.dataset_name,
            self.dataset_code,
            self.lib_generated_code,
            assign_func_pycode,
            is_assign=True,
            column_name=obj.column,
        )

        return proto.Operator(
            id=obj.signature(),
            is_root=(obj == self.terminal_node),
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            assign=proto.Assign(
                operand_id=self.visit(obj.node),
                pycode=gen_pycode,
                column_name=obj.column,
                output_type=get_datatype(obj.output_type),
            ),
        )

    def visitAggregate(self, obj):
        emit_strategy = (
            proto.Aggregate.Eager
            if obj.emit_strategy == EmitStrategy.Eager
            else proto.Aggregate.Final
        )
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            aggregate=proto.Aggregate(
                operand_id=self.visit(obj.node),
                keys=obj.keys,
                specs=[agg.to_proto() for agg in obj.aggregates],
                along=obj.along,
                emit_strategy=emit_strategy,
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
        # "forever" is a special value that means no lower bound.
        within_low, within_high = None, None
        if obj.within[0] != "forever":
            within_low = duration_proto.Duration()
            within_low.FromTimedelta(duration_to_timedelta(obj.within[0]))
        within_high_td = duration_to_timedelta(obj.within[1])
        if within_high_td.total_seconds() != 0:
            within_high = duration_proto.Duration()
            within_high.FromTimedelta(within_high_td)

        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            join=proto.Join(
                lhs_operand_id=self.visit(obj.node),
                rhs_dsref_operand_id=rhs_operator_id,
                on=on,
                how=(
                    proto.Join.How.Left
                    if obj.how == "left"
                    else proto.Join.How.Inner
                ),
                within_low=within_low,
                within_high=within_high,
            ),
        )

    def visitDrop(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            drop=proto.Drop(
                operand_id=self.visit(obj.node),
                dropcols=obj.columns,
            ),
        )

    def visitDropNull(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=(obj == self.terminal_node),
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            dropnull=proto.Dropnull(
                operand_id=self.visit(obj.node), columns=obj.columns
            ),
        )

    def visitRename(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            rename=proto.Rename(
                operand_id=self.visit(obj.node),
                column_map=obj.column_mapping,
            ),
        )

    def visitUnion(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            union=proto.Union(
                operand_ids=[self.visit(node) for node in obj.nodes]
            ),
        )

    def visitDedup(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            dedup=proto.Dedup(
                operand_id=self.visit(obj.node),
                columns=obj.by,
            ),
        )

    def visitExplode(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            explode=proto.Explode(
                operand_id=self.visit(obj.node),
                columns=obj.columns,
            ),
        )

    def visitFirst(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            first=proto.First(
                operand_id=self.visit(obj.node),
                by=obj.keys,
            ),
        )

    def visitLatest(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            latest=proto.Latest(
                operand_id=self.visit(obj.node),
                by=obj.keys,
            ),
        )

    def visitWindow(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            window=proto.WindowOperatorKind(
                operand_id=self.visit(obj.node),
                window_type=obj.window.to_proto(),
                field=obj.field,
                by=obj.input_keys,
                summary=None,
            ),
        )

    def visitChangelog(self, obj):
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            ds_version=self.dataset_version,
            changelog=proto.Changelog(
                operand_id=self.visit(obj.node),
                delete_column=obj.delete_column,
            ),
        )
