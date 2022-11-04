from __future__ import annotations

import ast
import cloudpickle
import datetime
import inspect
import pyarrow
import textwrap
from dataclasses import dataclass
from typing import (Callable, cast, Dict, List, Mapping, Optional, Tuple, Type,
                    TypeVar, Union)

import fennel.gen.dataset_pb2 as proto
from fennel.lib.aggregate import AggregateType
from fennel.lib.duration.duration import Duration, duration_to_timedelta, \
    timedelta_to_micros
from fennel.lib.schema.schema import get_pyarrow_field

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar("T")

DEFAULT_RETENTION = Duration("2y")
DEFAULT_MAX_STALENESS = Duration("30d")


# ---------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------

@dataclass
class Field:
    name: str
    key: bool
    timestamp: bool
    owner: str
    description: str
    pa_field: pyarrow.lib.Field
    dtype: Type

    def to_proto(self) -> proto.DatasetField:
        # Type is passed as part of the dataset schema
        return proto.DatasetField(
            name=self.name,
            is_key=self.key,
            is_timestamp=self.timestamp,
            owner=self.owner,
            description=self.description,
            is_nullable=self.pa_field.nullable,
        )


def field(
        key: bool = False,
        timestamp: bool = False,
        description: Optional[str] = None,
        owner: Optional[str] = None,
) -> T:
    return cast(
        T,
        Field(
            key=key,
            timestamp=timestamp,
            owner=owner,
            description=description,
            # These fields will be filled in later.
            name="",
            pa_field=None,
            dtype=None,
        ),
    )


# ---------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------

class _Node:
    def transform(self, func: Callable) -> _Node:
        return _Transform(self, func)

    def groupby(self, *args) -> _GroupBy:
        return _GroupBy(self, *args)

    def join(self, other: Dataset, on: Optional[List[str]] = None, left_on:
    Optional[List[str]] = None, right_on: Optional[List[str]] = None) -> _Join:
        return _Join(self, other, on, left_on, right_on)

    def signature(self):
        raise NotImplementedError

    def to_proto(self):
        serializer = Serializer()
        return serializer.serialize(self)


class _Transform(_Node):
    def __init__(self, node: _Node, func: Callable):
        self.func = func
        self.node = node


class _Aggregate(_Node):
    def __init__(self, node: _Node, keys: List[str],
                 aggregates: List[AggregateType]):
        if len(keys) == 0:
            raise ValueError("Must specify at least one key")
        self.keys = keys
        self.aggregates = aggregates
        self.node = node


class _GroupBy:
    def __init__(self, node: _Node, *args):
        self.keys = args
        self.node = node

    def aggregate(self, aggregates: List[AggregateType]) -> _Node:
        return _Aggregate(self.node, self.keys, aggregates)


class _Join(_Node):
    def __init__(self, node: _Node, dataset: Dataset, on: Optional[List[
        str]] = None, left_on: Optional[List[str]] = None, right_on:
    Optional[List[str]] = None):
        self.node = node
        self.dataset = dataset
        self.on = on
        self.left_on = left_on
        self.right_on = right_on
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError("Cannot specify on and left_on/right_on")
            if not isinstance(on, list):
                raise ValueError("on must be a list of keys")

        if on is None:
            if not isinstance(left_on, list) or not isinstance(right_on, list):
                raise ValueError(
                    "Must specify left_on and right_on as a list of keys")
            if left_on is None or right_on is None:
                raise ValueError("Must specify on or left_on/right_on")


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_annotation_comments(cls: Type[T]) -> Mapping[str, str]:
    try:
        source = textwrap.dedent(inspect.getsource(cls))

        source_lines = source.splitlines()
        tree = ast.parse(source)
        if len(tree.body) != 1:
            return {}

        comments_for_annotations: Dict[str, str] = {}
        class_def = tree.body[0]
        if isinstance(class_def, ast.ClassDef):
            for stmt in class_def.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target,
                        ast.Name):
                    line = stmt.lineno - 2
                    comments: List[str] = []
                    while line >= 0 and source_lines[line].strip().startswith(
                            "#"):
                        comment = source_lines[line].strip().strip("#").strip()
                        comments.insert(0, comment)
                        line -= 1

                    if len(comments) > 0:
                        comments_for_annotations[
                            stmt.target.id] = textwrap.dedent(
                            "\n".join(comments))

        return comments_for_annotations
    except Exception:
        return {}


def _get_field(
        cls: Type,
        annotation_name: str,
        dtype: Type,
        field2comment_map: Dict[str, str],
):
    field = getattr(cls, annotation_name, None)
    if isinstance(field, Field):
        field.name = annotation_name
        field.dtype = dtype
    else:
        field = Field(name=annotation_name, key=False, timestamp=False,
            owner=None, description="", pa_field=None, dtype=dtype)

    field.description = field2comment_map.get(annotation_name, "")
    field.pa_field = get_pyarrow_field(annotation_name, dtype)
    return field


def _create_dataset(cls: Type[T], retention: Duration = DEFAULT_RETENTION,
                    max_staleness: Duration = DEFAULT_MAX_STALENESS) -> T:
    cls_annotations = cls.__dict__.get("__annotations__", {})
    fields = [
        _get_field(
            cls=cls,
            annotation_name=name,
            dtype=cls_annotations[name],
            field2comment_map=_parse_annotation_comments(cls),
        )
        for name in cls_annotations
    ]

    pull_fn = getattr(cls, "pull", None)
    return Dataset(
        cls,
        fields,
        retention=duration_to_timedelta(retention),
        max_staleness=duration_to_timedelta(max_staleness),
        pull_fn=pull_fn,
    )


# ---------------------------------------------------------------------
# Dataset & Pipeline
# ---------------------------------------------------------------------


class dataset:
    """dataset is a decorator that creates a Dataset class.
    Parameters
    ----------
    retention : Duration ( Optional )
        The amount of time to keep data in the dataset.
    max_staleness : Duration ( Optional )
        The maximum amount of time that data in the dataset can be stale.
    """

    def __new__(cls, dataset_cls: Optional[Type[T]] = None, *args, **kwargs):
        """This is called when dataset is called without parens."""
        if dataset_cls is None:
            # We're called as @dataset(*args, *kwargs).
            return super().__new__(cls)
        # We're called as @dataset without parens.
        return _create_dataset(dataset_cls, *args, **kwargs)

    def __init__(self, retention: Duration = DEFAULT_RETENTION,
                 max_staleness: Duration = DEFAULT_MAX_STALENESS):
        """
        This is called to instantiate a dataset that is called with
        parens.
        """
        self._retention = retention
        self._max_staleness = max_staleness

    def __call__(self, dataset_cls: Optional[Type[T]]):
        """This is called when dataset is called with parens."""
        return _create_dataset(dataset_cls, self._retention,
            self._max_staleness)


@dataclass
class Pipeline:
    node: _Node
    signature: str
    inputs: List["Dataset"]

    def to_proto(self) -> proto.Pipeline:
        return proto.Pipeline(
            root=self.node.to_proto(),
            signature=self.signature,
            inputs=[proto.Dataset(name=i.name) for i in self.inputs],
        )


class Dataset(_Node):
    """Dataset is a collection of data."""
    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    _retention: datetime.timedelta
    _fields: List[Field]
    _key_field: str
    _timestamp_field: str
    _max_staleness: Optional[datetime.timedelta]
    _owner: Optional[str]
    __fennel_original_cls__: Type[T]
    pull_fn: Optional[Callable]
    name: str

    def __init__(
            self,
            cls: Type,
            fields: List[Field],
            retention: Optional[datetime.timedelta] = None,
            max_staleness: Optional[datetime.timedelta] = None,
            pull_fn: Optional[Callable] = None,
    ):
        self.name = cls.__name__
        self.__name__ = cls.__name__
        self._fields = fields
        self._set_timestamp_field()
        self._retention = retention
        self._max_staleness = max_staleness
        self.__fennel_original_cls__ = cls
        self.pull_fn = pull_fn

    def __getattr__(self, key):
        if key in self.__fennel_original_cls__.__dict__["__annotations__"]:
            return str(key)
        return super().__getattribute__(key)

    def _set_timestamp_field(self):
        timestamp_field_set = False
        for field in self._fields:
            if field.timestamp:
                if timestamp_field_set:
                    raise ValueError(
                        "Multiple timestamp fields are not supported.")
                timestamp_field_set = True

        if timestamp_field_set:
            return

        # Find a field that has datetime type and set it as timestamp.

        for field in self._fields:
            if field.dtype != datetime.datetime:
                continue

            if not timestamp_field_set:
                field.timestamp = True
                timestamp_field_set = True
            else:
                raise ValueError(
                    "Multiple timestamp fields are not supported.")
        if not timestamp_field_set:
            raise ValueError("No timestamp field found.")

    def _get_schema(self) -> bytes:
        schema = pyarrow.schema(
            [field.pa_field for field in self._fields if field.pa_field])
        return schema.serialize().to_pybytes()

    def __repr__(self):
        return f"Dataset({self.__name__}, {self._fields})"

    def _pull_to_proto(self) -> proto.PullLookup:
        return proto.PullLookup(
            function_source_code=inspect.getsource(self.pull_fn),
            function=cloudpickle.dumps(self.pull_fn),
        )

    def _get_pipelines(self) -> List[Pipeline]:
        pipelines = []
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if name == "pull":
                continue
            if not hasattr(method, "__fennel_pipeline__"):
                continue
            pipeline = method.__fennel_pipeline__
            pipelines.append(pipeline)
        return pipelines

    def create_dataset_request_proto(self) -> proto.CreateDatasetRequest:
        return proto.CreateDatasetRequest(
            name=self.__name__,
            schema=self._get_schema(),
            retention=timedelta_to_micros(self._retention),
            max_staleness=timedelta_to_micros(self._max_staleness),
            pipelines=[p.to_proto() for p in self._get_pipelines()],
            mode="pandas",
            # TODO: Parse description from docstring.
            description="",
            # Currently we don't support versioning of datasets.
            # Kept for future use.
            version=0,
            fields=[field.to_proto() for field in self._fields],
            pull_lookup=self._pull_to_proto() if self.pull_fn else None,
        )


def pipeline(pipeline_func):
    if not callable(pipeline_func):
        raise Exception("pipeline_func must be callable")
    sig = inspect.signature(pipeline_func)
    params = []
    for name, param in sig.parameters.items():
        if param.name == "self":
            raise TypeError("pipeline_func cannot have self as a parameter "
                            "and should be a static method")
        if not isinstance(param.annotation, Dataset):
            raise TypeError(f"Parameter {name} is not a Dataset")
        params.append(param.annotation)
    pipeline = Pipeline(node=pipeline_func(*params), inputs=params,
        signature="")
    pipeline_func.__fennel_pipeline__ = pipeline
    return pipeline_func


# ---------------------------------------------------------------------
# Visitor
# ---------------------------------------------------------------------


class Visitor:
    def visit(self, obj):
        if isinstance(obj, Dataset):
            return self.visitDataset(obj)
        elif isinstance(obj, _Transform):
            return self.visitTransform(obj)
        elif isinstance(obj, _GroupBy):
            return self.visitGroupBy(obj)
        elif isinstance(obj, _Aggregate):
            return self.visitAggregate(obj)
        elif isinstance(obj, _Join):
            return self.visitJoin(obj)
        else:
            raise Exception("invalid node type: %s" % obj)

    def visitDataset(self, obj):
        raise NotImplementedError()

    def visitTransform(self, obj):
        raise NotImplementedError()

    def visitGroupBy(self, obj):
        raise Exception(f"group by object {obj} must be aggregated")

    def visitAggregate(self, obj):
        return NotImplementedError()

    def visitJoin(self, obj):
        raise NotImplementedError()


class Printer(Visitor):
    def __init__(self):
        super(Printer, self).__init__()
        self.lines = []
        self.offset = 0

    def indent(self):
        return " " * self.offset

    def visit(self, obj):
        ret = super(Printer, self).visit(obj)
        return ret

    def print(self, obj):
        last = self.visit(obj)
        self.lines.append(last)
        print("\n".join(self.lines))

    def visitDataset(self, obj):
        return f"{self.indent()}{obj.name}\n"

    def visitTransform(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        self.offset -= 1
        return f"{self.indent()}Transform(\n{pipe_str}{self.indent()})\n"

    def visitAggregate(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        self.offset -= 1
        return f"{self.indent()}Aggregate(\n{pipe_str}{self.indent()})\n"

    def visitJoin(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        dataset_str = self.visit(obj.dataset)
        self.offset -= 1
        return f"{self.indent()}Join(\n{pipe_str}{self.indent()}," \
               f"{dataset_str}), on={obj.on} left={obj.left_on} right={obj.right_on}{self.indent()})\n"


class Serializer(Visitor):
    def __init__(self):
        super(Serializer, self).__init__()

    def serialize(self, obj: _Node):
        return self.visit(obj)

    def visitDataset(self, obj):
        return proto.Node(dataset=proto.Dataset(name=obj.name))

    def visitTransform(self, obj):
        return proto.Node(operator=proto.Operator(transform=proto.Transform(
            node=self.visit(obj.node),
            function=cloudpickle.dumps(obj.func),
            function_source_code=inspect.getsource(obj.func))))

    def visitAggregate(self, obj):
        return proto.Node(operator=proto.Operator(aggregate=proto.Aggregate(
            node=self.visit(obj.node),
            keys=obj.keys,
            aggregates=[agg.to_proto() for agg in
                        obj.aggregates])))

    def visitJoin(self, obj):
        if obj.on is not None:
            on = {k: k for k in obj.on}
        else:
            on = {l: r for l, r in zip(obj.left_on, obj.right_on)}

        return proto.Node(operator=proto.Operator(join=proto.Join(
            node=self.visit(obj.node),
            dataset=proto.Dataset(name=obj.dataset.name), on=on)))
