from __future__ import annotations

import hashlib
import re
from textwrap import dedent, indent
from typing import Dict, Any, List, Optional

import google.protobuf.duration_pb2 as duration_proto

import fennel.gen.dataset_pb2 as proto
import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import Dataset, Pipeline, Visitor
from fennel.lib.duration import (
    duration_to_timedelta,
)
from fennel.lib.includes import FENNEL_INCLUDED_MOD
from fennel.lib.schema import get_datatype
from fennel.lib.to_proto.source_code import (
    to_includes_proto,
    get_dataset_core_code,
)


def _del_spaces_tabs_and_newlines(s):
    return re.sub(r"[\s\n\t]+", "", s)


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

    def serialize(self):
        _ = self.visit(self.terminal_node)
        return self.operators

    def wrap_function(
        self,
        op_pycode,
        is_filter=False,
        is_assign=False,
        column_name: Optional[str] = None,
    ) -> pycode_proto.PyCode:
        gen_func_name = hashlib.sha256(
            _del_spaces_tabs_and_newlines(op_pycode.core_code).encode()
        ).hexdigest()[:10]

        gen_function_name = f"wrapper_{gen_func_name}"
        if op_pycode.entry_point == "<lambda>":
            wrapper_function = f"""
@classmethod
def {gen_function_name}(cls, *args, **kwargs):
    _fennel_internal = {op_pycode.generated_code.strip()}
    return _fennel_internal(*args, **kwargs)
"""
        else:
            wrapper_function = f"""
@classmethod
def {gen_function_name}(cls, *args, **kwargs):
    {indent(op_pycode.generated_code, "    ")}
    return {op_pycode.entry_point}(*args, **kwargs)
"""
        wrapper_function = indent(dedent(wrapper_function), "    ")
        gen_code = (
            dedent(self.lib_generated_code)
            + "\n"
            + self.dataset_code
            + "\n"
            + wrapper_function
        )

        new_entry_point = f"{self.dataset_name}_{gen_function_name}"
        ret_code = f"""
def {new_entry_point}(*args, **kwargs):
    _fennel_internal = {self.dataset_name}.__fennel_original_cls__
    return getattr(_fennel_internal, "{gen_function_name}")(*args, **kwargs)
"""
        gen_code = gen_code + "\n" + dedent(ret_code)

        if is_filter:
            old_entry_point = new_entry_point
            new_entry_point = f"{old_entry_point}_filter"
            gen_code += f"""
def {new_entry_point}(df: pd.DataFrame) -> pd.DataFrame:
    return df[{old_entry_point}(df)]
"""
        if is_assign:
            old_entry_point = new_entry_point
            new_entry_point = f"{old_entry_point}_assign"
            gen_code += f"""
def {new_entry_point}(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign({column_name}={old_entry_point})
"""

        return pycode_proto.PyCode(
            entry_point=f"{new_entry_point}",
            generated_code=gen_code,
            core_code=op_pycode.core_code,
            source_code=op_pycode.source_code,
            includes=op_pycode.includes,
            imports=op_pycode.imports,
        )

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
            {col: get_datatype(dtype) for col, dtype in obj.new_schema.items()}
            if obj.new_schema is not None
            else None
        )
        transform_func_pycode = to_includes_proto(obj.func)
        gen_pycode = self.wrap_function(transform_func_pycode)
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            transform=proto.Transform(
                operand_id=self.visit(obj.node),
                schema=schema,
                pycode=gen_pycode,
            ),
        )

    def visitFilter(self, obj):
        filter_func_pycode = to_includes_proto(obj.func)
        gen_pycode = self.wrap_function(filter_func_pycode, is_filter=True)
        return proto.Operator(
            id=obj.signature(),
            is_root=obj == self.terminal_node,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            filter=proto.Filter(
                operand_id=self.visit(obj.node),
                pycode=gen_pycode,
            ),
        )

    def visitAssign(self, obj):
        assign_func_pycode = to_includes_proto(obj.func)
        gen_pycode = self.wrap_function(
            assign_func_pycode, is_assign=True, column_name=obj.column
        )

        return proto.Operator(
            id=obj.signature(),
            is_root=(obj == self.terminal_node),
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            assign=proto.Assign(
                operand_id=self.visit(obj.node),
                pycode=gen_pycode,
                column_name=obj.column,
                output_type=get_datatype(obj.output_type),
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
            join=proto.Join(
                lhs_operand_id=self.visit(obj.node),
                rhs_dsref_operand_id=rhs_operator_id,
                on=on,
                how=proto.Join.How.Left
                if obj.how == "left"
                else proto.Join.How.Inner,
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
            first=proto.First(
                operand_id=self.visit(obj.node),
                by=obj.keys,
            ),
        )
