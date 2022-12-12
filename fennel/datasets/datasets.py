from __future__ import annotations

import datetime
import inspect
from dataclasses import dataclass
from typing import (
    cast,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import cloudpickle
import numpy as np
import pandas as pd
import pyarrow

import fennel.gen.dataset_pb2 as proto
from fennel.lib.aggregate import AggregateType
from fennel.lib.duration.duration import (
    Duration,
    duration_to_timedelta,
    timedelta_to_micros,
    duration_to_micros,
)
from fennel.lib.metadata import (
    meta,
    get_meta_attr,
    set_meta_attr,
    get_metadata_proto,
)
from fennel.lib.schema import get_datatype, dtype_to_string
from fennel.sources import SOURCE_FIELD, SINK_FIELD
from fennel.utils import (
    fhash,
    parse_annotation_comments,
    propogate_fennel_attributes,
)

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar("T", bound="Dataset")
F = TypeVar("F")

PIPELINE_ATTR = "__fennel_pipeline__"
ON_DEMAND_ATTR = "__fennel_on_demand__"

DEFAULT_RETENTION = Duration("2y")
DEFAULT_EXPIRATION = Duration("30d")


# ---------------------------------------------------------------------
# Field
# ---------------------------------------------------------------------


@dataclass
class Field:
    name: str
    key: bool
    timestamp: bool
    dtype: Optional[Type]

    def signature(self) -> str:
        if self.dtype is None:
            raise ValueError("dtype is not set")

        return fhash(
            self.name,
            f"{dtype_to_string(self.dtype)}",
            f"{self.is_optional()}:{self.key}:{self.timestamp}",
        )

    def to_proto(self) -> proto.Field:
        # Type is passed as part of the dataset schema
        ftype = None
        if self.key + self.timestamp > 1:
            raise ValueError("Field cannot be both a key and a timestamp")

        if self.key:
            ftype = proto.FieldType.Key
        elif self.timestamp:
            ftype = proto.FieldType.Timestamp
        else:
            ftype = proto.FieldType.Val

        return proto.Field(
            name=self.name,
            ftype=ftype,
            dtype=get_datatype(self.dtype),
            metadata=get_metadata_proto(self),
        )

    def meta(self, **kwargs: Any) -> F:
        f = cast(F, meta(**kwargs)(self))
        if get_meta_attr(f, "deleted") or get_meta_attr(f, "deprecated"):
            raise ValueError(
                "Dataset currently does not support deleted or "
                "deprecated fields."
            )
        return f

    def is_optional(self) -> bool:
        def _get_origin(type_: Any) -> Any:
            return getattr(type_, "__origin__", None)

        def _get_args(type_: Any) -> Any:
            return getattr(type_, "__args__", None)

        if (
            _get_origin(self.dtype) is Union
            and type(None) == _get_args(self.dtype)[1]
        ):
            return True

        return False


def get_field(
    cls: F,
    annotation_name: str,
    dtype: Type,
    field2comment_map: Dict[str, str],
) -> Field:
    if "." in annotation_name:
        raise ValueError(
            f"Field name {annotation_name} cannot contain a period."
        )
    field = getattr(cls, annotation_name, None)
    if isinstance(field, Field):
        field.name = annotation_name
        field.dtype = dtype
    else:
        field = Field(
            name=annotation_name,
            key=False,
            timestamp=False,
            dtype=dtype,
        )

    description = get_meta_attr(field, "description")
    if description is None or description == "":
        description = field2comment_map.get(annotation_name, "")
        set_meta_attr(field, "description", description)

    if field.key and field.is_optional():
        raise ValueError(
            f"Key {annotation_name} in dataset {cls.__name__} cannot be "  # type: ignore
            f"Optional."
        )
    return field


def field(
    key: bool = False,
    timestamp: bool = False,
) -> F:
    return cast(
        F,
        Field(
            key=key,
            timestamp=timestamp,
            # These fields will be filled in later.
            name="",
            dtype=None,
        ),
    )


# ---------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------


class _Node:
    def __init__(self):
        self.out_edges = []

    def transform(self, func: Callable) -> _Node:
        return _Transform(self, func)

    def filter(self, func: Callable) -> _Node:
        return _Filter(self, func)

    def groupby(self, *args) -> _GroupBy:
        return _GroupBy(self, *args)

    def join(
        self,
        other: Dataset,
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
    ) -> _Join:
        if not isinstance(other, Dataset) and isinstance(other, _Node):
            raise ValueError("Cannot join with an intermediate dataset")
        if not isinstance(other, _Node):
            raise TypeError("Cannot join with a non-dataset object")
        return _Join(self, other, on, left_on, right_on)

    def __add__(self, other):
        return _Union(self, other)

    def isignature(self):
        raise NotImplementedError

    def num_out_edges(self) -> int:
        return len(self.out_edges)


class _Transform(_Node):
    def __init__(self, node: _Node, func: Callable):
        super().__init__()
        self.func = func
        self.node = node
        self.node.out_edges.append(self)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)


class _Filter(_Node):
    def __init__(self, node: _Node, func: Callable):
        super().__init__()
        self.func = func
        self.node = node
        self.node.out_edges.append(self)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)


class _Aggregate(_Node):
    def __init__(
        self, node: _Node, keys: List[str], aggregates: List[AggregateType]
    ):
        super().__init__()
        if len(keys) == 0:
            raise ValueError("Must specify at least one key")
        self.keys = keys
        self.aggregates = aggregates
        self.node = node
        self.node.out_edges.append(self)

    def signature(self):
        agg_signature = fhash([agg.signature() for agg in self.aggregates])
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.keys, agg_signature)
        return fhash(self.node.signature(), self.keys, agg_signature)


class _GroupBy:
    def __init__(self, node: _Node, *args):
        super().__init__()
        self.keys = args
        self.node = node
        self.node.out_edges.append(self)

    def aggregate(self, aggregates: List[AggregateType]) -> _Node:
        return _Aggregate(self.node, list(self.keys), aggregates)


class _Join(_Node):
    def __init__(
        self,
        node: _Node,
        dataset: Dataset,
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
    ):
        super().__init__()
        self.node = node
        self.dataset = dataset
        self.on = on
        self.left_on = left_on
        self.right_on = right_on
        self.node.out_edges.append(self)
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError("Cannot specify on and left_on/right_on")
            if not isinstance(on, list):
                raise ValueError("on must be a list of keys")

        if on is None:
            if not isinstance(left_on, list) or not isinstance(right_on, list):
                raise ValueError(
                    "Must specify left_on and right_on as a list of keys"
                )
            if left_on is None or right_on is None:
                raise ValueError("Must specify on or left_on/right_on")

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(
                self.node._name,
                self.dataset._name,
                self.on,
                self.left_on,
                self.right_on,
            )
        return fhash(
            self.node.signature(),
            self.dataset._name,
            self.on,
            self.left_on,
            self.right_on,
        )


class _Union(_Node):
    def __init__(self, node: _Node, other: _Node):
        super().__init__()
        self.nodes = [node, other]
        node.out_edges.append(self)
        other.out_edges.append(self)

    def signature(self):
        return fhash([n.signature() for n in self.nodes])


# ---------------------------------------------------------------------
# dataset & pipeline decorators
# ---------------------------------------------------------------------


@overload
def dataset(
    *,
    retention: Optional[Duration] = DEFAULT_RETENTION,
) -> Callable[[Type[F]], Dataset]:
    ...


@overload
def dataset(cls: Type[F]) -> Dataset:
    ...


def dataset(
    cls: Optional[Type[F]] = None,
    retention: Optional[Duration] = DEFAULT_RETENTION,
) -> Union[Callable[[Type[F]], Dataset], Dataset]:
    """
    dataset is a decorator that creates a Dataset class.
    A dataset class contains the schema of the dataset, an optional pull
    function, and a set of pipelines that declare how to generate data for the
    dataset from other datasets.
    Parameters
    ----------
    retention : Duration ( Optional )
        The amount of time to keep data in the dataset.
    max_staleness : Duration ( Optional )
        The maximum amount of time that data in the dataset can be stale.
    """

    def _create_lookup_function(
        cls_name: str, key_fields: List[str]
    ) -> Optional[Callable]:
        if len(key_fields) == 0:
            return None

        def lookup(
            ts: pd.Series, *args, **kwargs
        ) -> Tuple[pd.DataFrame, pd.Series]:

            if len(args) > 0:
                raise ValueError(
                    f"lookup expects key value arguments and can "
                    f"optionally include properties, found {args}"
                )
            if len(kwargs) < len(key_fields):
                raise ValueError(
                    f"lookup expects keys of the table being looked up and can "
                    f"optionally include properties, found {kwargs}"
                )
            # Check that ts is a series of datetime64[ns]
            if not isinstance(ts, pd.Series):
                raise ValueError(
                    f"lookup expects a series of timestamps, found {type(ts)}"
                )
            if not np.issubdtype(ts.dtype, np.datetime64):
                raise ValueError(
                    f"lookup expects a series of timestamps, found {ts.dtype}"
                )
            # convert ts to pyarrow Array
            ts = pyarrow.Array.from_pandas(ts)
            # extract keys and properties from kwargs
            arr = []
            for key, value in kwargs.items():
                if key != "properties":
                    if not isinstance(value, pd.Series):
                        raise ValueError(f"Param {key} is not a pandas Series")
                    arr.append(value)
            if "properties" in kwargs:
                properties = kwargs["properties"]
            else:
                properties = []

            # convert keys to a pyarrow RecordBatch
            df = pd.concat(arr, axis=1)
            df.columns = key_fields
            key_recordbatch = pyarrow.RecordBatch.from_pandas(df)

            res, found = dataset_lookup(
                cls_name,
                ts,
                properties,
                key_recordbatch,
            )

            df = res.to_pandas()
            return df.replace({np.nan: None}), found.to_pandas()

        args = {k: pd.Series for k in key_fields}
        args["properties"] = List[str]
        params = [
            inspect.Parameter(
                param, inspect.Parameter.KEYWORD_ONLY, annotation=type_
            )
            for param, type_ in args.items()
        ]
        args["ts"] = pd.Series
        params = [
            inspect.Parameter(
                "ts",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=pd.Series,
            )
        ] + params
        setattr(lookup, "__signature__", inspect.Signature(params))
        setattr(lookup, "__annotations__", args)
        return lookup

    def _create_dataset(
        dataset_cls: Type[F],
        retention: Duration,
    ) -> Dataset:
        cls_annotations = dataset_cls.__dict__.get("__annotations__", {})
        fields = [
            get_field(
                cls=dataset_cls,
                annotation_name=name,
                dtype=cls_annotations[name],
                field2comment_map=parse_annotation_comments(dataset_cls),
            )
            for name in cls_annotations
        ]

        key_fields = [f.name for f in fields if f.key]

        return Dataset(
            dataset_cls,
            fields,
            retention=duration_to_timedelta(retention),
            lookup_fn=_create_lookup_function(dataset_cls.__name__, key_fields),
        )

    def wrap(c: Type[F]) -> Dataset:
        return _create_dataset(c, cast(Duration, retention))

    if cls is None:
        # We're being called as @dataset()
        return wrap
    cls = cast(Type[F], cls)
    # @dataset decorator was used without arguments
    return wrap(cls)


def pipeline(
    *params: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    for param in params:
        if callable(param):
            raise Exception("pipeline must take atleast one Dataset.")
        if not isinstance(param, Dataset):
            raise TypeError("pipeline parameters must be Dataset instances.")

    def wrapper(pipeline_func: Callable) -> Callable:
        if not callable(pipeline_func):
            raise TypeError("pipeline functions must be callable.")
        sig = inspect.signature(pipeline_func)
        for name, param in sig.parameters.items():
            if param.name == "self":
                raise TypeError(
                    "pipeline functions cannot have self as a parameter"
                    " and are like static methods."
                )
            if param.annotation != Dataset:
                raise TypeError(f"Parameter {name} is not a Dataset.")
        setattr(
            pipeline_func,
            PIPELINE_ATTR,
            Pipeline(
                node=pipeline_func(*params),
                inputs=list(params),
                func=pipeline_func,
            ),
        )
        return pipeline_func

    return wrapper


@dataclass
class OnDemand:
    func: Callable
    expires_after: Duration = DEFAULT_EXPIRATION


def on_demand(expires_after: Duration):
    if not isinstance(expires_after, Duration):
        raise TypeError(
            "on_demand must be defined with a parameter "
            "expires_after of type Duration for eg: 30d."
        )

    def decorator(func):
        setattr(func, ON_DEMAND_ATTR, OnDemand(func, expires_after))
        return func

    return decorator


def dataset_lookup(
    cls_name: str,
    ts: Any,
    properties: List[str],
    keys: Any,
) -> Tuple[Any, Any]:
    raise NotImplementedError("dataset_lookup should not be called directly.")


# ---------------------------------------------------------------------
# Dataset & Pipeline
# ---------------------------------------------------------------------


class Pipeline:
    node: _Node
    inputs: List[Dataset]
    _sign: str
    _proto: proto.Pipeline
    _root: str
    _nodes: List
    # Dataset it is part of
    _dataset_name: str
    func: Callable
    name: str

    def __init__(self, node: _Node, inputs: List[Dataset], func: Callable):
        self.node = node
        self.inputs = inputs
        self.func = func  # type: ignore
        serializer = Serializer()
        self._root, self._nodes = serializer.serialize(self)
        self.name = func.__name__

    def signature(self):
        return f"{self._dataset_name}.{self._root}"

    def to_proto(self) -> proto.Pipeline:
        return proto.Pipeline(
            root=self._root,
            nodes=self._nodes,
            inputs=[i._name for i in self.inputs],
            signature=self.signature(),
            metadata=get_metadata_proto(self.func),
        )

    def set_dataset_name(self, ds_name: str):
        self._dataset_name = ds_name

    def __str__(self):
        return f"Pipeline({self.signature()})"

    @property
    def dataset_name(self):
        return self._dataset_name


class Dataset(_Node):
    """Dataset is a collection of data."""

    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    _name: str
    _on_demand: Optional[OnDemand]
    _retention: datetime.timedelta
    _fields: List[Field]
    _key_fields: List[str]
    _pipelines: List[Pipeline]
    _timestamp_field: str
    __fennel_original_cls__: Any
    lookup: Callable

    def __init__(
        self,
        cls: F,
        fields: List[Field],
        retention: datetime.timedelta,
        lookup_fn: Optional[Callable] = None,
    ):
        super().__init__()
        self._name = cls.__name__  # type: ignore
        self.__name__ = self._name
        self._fields = fields
        self._set_timestamp_field()
        self._set_key_fields()
        self._retention = retention
        self.__fennel_original_cls__ = cls
        self._pipelines = self._get_pipelines()
        self._on_demand = self._get_on_demand()
        self._sign = self._create_signature()
        if lookup_fn is not None:
            self.lookup = lookup_fn  # type: ignore
        propogate_fennel_attributes(cls, self)

    # ------------------- Public Methods --------------------------

    def create_dataset_request_proto(self) -> proto.CreateDatasetRequest:
        self._check_owner_exists()
        sources = []
        if hasattr(self, SOURCE_FIELD):
            sources = getattr(self, SOURCE_FIELD)
        sinks = []
        if hasattr(self, SINK_FIELD):
            sinks = getattr(self, SINK_FIELD)

        return proto.CreateDatasetRequest(
            name=self.__name__,
            retention=timedelta_to_micros(self._retention),
            pipelines=[p.to_proto() for p in self._pipelines],
            input_connectors=[s.to_proto() for s in sources],
            output_connectors=[s.to_proto() for s in sinks],
            mode="pandas",
            signature=self.signature(),
            metadata=get_metadata_proto(self),
            # Currently we don't support versioning of datasets.
            # Kept for future use.
            version=0,
            fields=[field.to_proto() for field in self._fields],
            on_demand=self._on_demand_to_proto(),
        )

    def signature(self):
        return self._sign

    def fields(self):
        return [f.name for f in self._fields]

    # ------------------- Private Methods ----------------------------------

    def _check_owner_exists(self):
        owner = get_meta_attr(self, "owner")
        if owner is None or owner == "":
            raise Exception(f"Dataset {self._name} must have an owner.")

    def _create_signature(self):
        return fhash(
            self._name,
            self._retention,
            self._on_demand,
            [f.signature() for f in self._fields],
            [p.signature for p in self._pipelines],
        )

    def _set_timestamp_field(self):
        timestamp_field_set = False
        for field in self._fields:
            if field.timestamp:
                self._timestamp_field = field.name
                if timestamp_field_set:
                    raise ValueError(
                        "Multiple timestamp fields are not supported."
                    )
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
                self._timestamp_field = field.name
            else:
                raise ValueError("Multiple timestamp fields are not supported.")
        if not timestamp_field_set:
            raise ValueError("No timestamp field found.")

    def _set_key_fields(self):
        key_fields = []
        for field in self._fields:
            if field.key:
                key_fields.append(field.name)
        self._key_fields = key_fields

    def __repr__(self):
        return f"Dataset({self.__name__}, {self._fields})"

    def _on_demand_to_proto(self) -> Optional[proto.OnDemand]:
        if self._on_demand is None:
            return None
        return proto.OnDemand(
            function_source_code=inspect.getsource(
                cast(Callable, self._on_demand.func)
            ),
            function=cloudpickle.dumps(self._on_demand.func),
            expires_after=duration_to_micros(self._on_demand.expires_after),
        )

    def _get_on_demand(self) -> Optional[OnDemand]:
        on_demand: Optional[OnDemand] = None
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, ON_DEMAND_ATTR):
                continue
            if on_demand is not None:
                raise ValueError(
                    f"Multiple on_demand methods are not supported for "
                    f"dataset {self._name}."
                )
            on_demand = getattr(method, ON_DEMAND_ATTR)
        # Validate on_demand function signature.
        if on_demand is not None:
            if not inspect.isfunction(on_demand.func):
                raise ValueError(
                    f"on_demand method {on_demand.func} is not a function."
                )
            sig = inspect.signature(on_demand.func)
            if len(sig.parameters) <= 1:
                raise ValueError(
                    f"on_demand method {on_demand.func} must take at least "
                    f"ts as first parameter, followed by key fields."
                )
            check_timestamp = False
            key_fields = [f for f in self._fields if f.key]
            key_index = 0
            for name, param in sig.parameters.items():
                if not check_timestamp:
                    if param.annotation == datetime.datetime:
                        check_timestamp = True
                        continue
                    raise ValueError(
                        f"on_demand method {on_demand.func.__name__} must take "
                        f"timestamp as first parameter with type Series[datetime]."
                    )
                if param.annotation != key_fields[key_index].dtype:
                    raise ValueError(
                        f"on_demand method {on_demand.func.__name__} must take "
                        f"key fields in the same order as defined in the "
                        f"dataset with the same type, parameter "
                        f"{key_fields[key_index].name} has type"
                        f" {param.annotation} but expected "
                        f" {key_fields[key_index].dtype} "
                    )
                key_index += 1
        return on_demand

    def _get_pipelines(self) -> List[Pipeline]:
        pipelines = []
        dataset_name = self._name
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, PIPELINE_ATTR):
                continue
            pipelines.append(getattr(method, PIPELINE_ATTR))
            pipelines[-1].set_dataset_name(dataset_name)
        return pipelines

    @property
    def timestamp_field(self):
        return self._timestamp_field

    @property
    def key_fields(self):
        return self._key_fields

    @property
    def on_demand(self):
        return self._on_demand


# ---------------------------------------------------------------------
# Visitor
# ---------------------------------------------------------------------


class Visitor:
    def visit(self, obj):
        if isinstance(obj, Dataset):
            return self.visitDataset(obj)
        elif isinstance(obj, _Transform):
            return self.visitTransform(obj)
        elif isinstance(obj, _Filter):
            return self.visitFilter(obj)
        elif isinstance(obj, _GroupBy):
            return self.visitGroupBy(obj)
        elif isinstance(obj, _Aggregate):
            return self.visitAggregate(obj)
        elif isinstance(obj, _Join):
            return self.visitJoin(obj)
        elif isinstance(obj, _Union):
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
        return f"{self.indent()}{obj.into_field}\n"

    def visitTransform(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        self.offset -= 1
        return f"{self.indent()}Transform(\n{pipe_str}{self.indent()})\n"

    def visitFilter(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        self.offset -= 1
        return f"{self.indent()}Filter(\n{pipe_str}{self.indent()})\n"

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
        return (
            f"{self.indent()}Join(\n{pipe_str}{self.indent()},"
            f"{dataset_str}), on={obj.on} left={obj.left_on} right={obj.right_on}{self.indent()})\n"
        )


class Serializer(Visitor):
    def __init__(self):
        super(Serializer, self).__init__()
        self.proto_by_node_id = {}
        self.nodes = []

    def serialize(self, pipe: Pipeline):
        root_id = self.visit(pipe.node)
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
        return proto.Node(
            operator=proto.Operator(
                transform=proto.Transform(
                    operand_node_id=self.visit(obj.node),
                    function=cloudpickle.dumps(obj.func),
                    function_source_code=inspect.getsource(obj.func),
                ),
            ),
            id=obj.signature(),
        )

    def visitFilter(self, obj):
        return proto.Node(
            operator=proto.Operator(
                filter=proto.Filter(
                    operand_node_id=self.visit(obj.node),
                    function=cloudpickle.dumps(obj.func),
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
