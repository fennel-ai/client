from __future__ import annotations

import copy
import datetime
import functools
import inspect
from dataclasses import dataclass
from typing import (
    cast,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Generic,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

import cloudpickle
import numpy as np
import pandas as pd

from fennel.lib.aggregate import AggregateType
from fennel.lib.duration.duration import (
    Duration,
    duration_to_timedelta,
)
from fennel.lib.expectations import Expectations, GE_ATTR_FUNC
from fennel.lib.metadata import (
    meta,
    get_meta_attr,
    set_meta_attr,
)
from fennel.lib.schema import dtype_to_string, get_dtype
from fennel.utils import (
    fhash,
    parse_annotation_comments,
    propogate_fennel_attributes,
)

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar("T")

PIPELINE_ATTR = "__fennel_pipeline__"
ON_DEMAND_ATTR = "__fennel_on_demand__"

DEFAULT_RETENTION = Duration("2y")
DEFAULT_EXPIRATION = Duration("30d")
RESERVED_FIELD_NAMES = [
    "cls",
    "self",
    "fields",
    "key_fields",
    "on_demand",
    "timestamp_field",
]


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

    def meta(self, **kwargs: Any) -> T:  # type: ignore
        f = cast(T, meta(**kwargs)(self))
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

    def __str__(self):
        return f"{self.name}"


def get_field(
    cls: T,
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
) -> T:  # type: ignore
    return cast(
        T,
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


class _Node(Generic[T]):
    def __init__(self):
        self.out_edges = []

    def transform(self, func: Callable, schema: Dict = {}) -> _Node:
        if schema == {}:
            return Transform(self, func, None)
        return Transform(self, func, schema)

    def filter(self, func: Callable) -> _Node:
        return Filter(self, func)

    def groupby(self, *args) -> GroupBy:
        return GroupBy(self, *args)

    def left_join(
        self,
        other: Dataset,
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
    ) -> Join:
        if not isinstance(other, Dataset) and isinstance(other, _Node):
            raise ValueError("Cannot join with an intermediate dataset")
        if not isinstance(other, _Node):
            raise TypeError("Cannot join with a non-dataset object")
        return Join(self, other, on, left_on, right_on)

    def __add__(self, other):
        return Union_(self, other)

    def rename(self, columns: Dict[str, str]) -> _Node:
        return Rename(self, columns)

    def drop(self, columns: List[str]) -> _Node:
        return Drop(self, columns)

    def isignature(self):
        raise NotImplementedError

    def num_out_edges(self) -> int:
        return len(self.out_edges)


class Transform(_Node):
    def __init__(self, node: _Node, func: Callable, schema: Optional[Dict]):
        super().__init__()
        self.func = func
        self.node = node
        self.node.out_edges.append(self)
        self.schema = schema
        cloudpickle.register_pickle_by_value(inspect.getmodule(func))
        self.pickled_func = cloudpickle.dumps(func)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)


class Filter(_Node):
    def __init__(self, node: _Node, func: Callable):
        super().__init__()
        self.func = func
        self.node = node
        self.node.out_edges.append(self)
        cloudpickle.register_pickle_by_value(inspect.getmodule(func))
        wrapped_func = lambda df: df[func(df)]  # noqa: E731
        self.pickled_func = cloudpickle.dumps(wrapped_func)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)


class Aggregate(_Node):
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


class GroupBy:
    def __init__(self, node: _Node, *args):
        super().__init__()
        self.keys = args
        self.node = node
        self.node.out_edges.append(self)

    def aggregate(self, aggregates: List[AggregateType], *args) -> _Node:
        if len(args) > 0 or not isinstance(aggregates, list):
            raise TypeError(
                "aggregate operator, takes a list of aggregates "
                "found: {}".format(type(aggregates))
            )
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        return Aggregate(self.node, list(self.keys), aggregates)


class Join(_Node):
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


class Union_(_Node):
    def __init__(self, node: _Node, other: _Node):
        super().__init__()
        self.nodes = [node, other]
        node.out_edges.append(self)
        other.out_edges.append(self)

    def signature(self):
        return fhash([n.signature() for n in self.nodes])


class Rename(_Node):
    def __init__(self, node: _Node, columns: Dict[str, str]):
        super().__init__()
        self.node = node
        self.column_mapping = columns
        self.node.out_edges.append(self)


class Drop(_Node):
    def __init__(self, node: _Node, columns: List[str]):
        super().__init__()
        self.node = node
        self.columns = columns
        self.node.out_edges.append(self)


# ---------------------------------------------------------------------
# dataset & pipeline decorators
# ---------------------------------------------------------------------


@overload
def dataset(
    *,
    history: Optional[Duration] = DEFAULT_RETENTION,
) -> Callable[[Type[T]], Dataset]:
    ...


@overload
def dataset(cls: Type[T]) -> Dataset:
    ...


def dataset(
    cls: Optional[Type[T]] = None,
    history: Optional[Duration] = DEFAULT_RETENTION,
) -> Union[Callable[[Type[T]], Dataset], Dataset]:
    """
    dataset is a decorator that creates a Dataset class.
    A dataset class contains the schema of the dataset, an optional pull
    function, and a set of pipelines that declare how to generate data for the
    dataset from other datasets.
    Parameters
    ----------
    history : Duration ( Optional )
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
                    f"optionally include fields, found {args}"
                )
            if len(kwargs) < len(key_fields):
                raise ValueError(
                    f"lookup expects keys of the table being looked up and can "
                    f"optionally include fields, found {kwargs}"
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
            # extract keys and fields from kwargs
            arr = []
            for key in key_fields:
                if key == "fields":
                    continue
                if key not in kwargs:
                    raise ValueError(
                        f"Missing key {key} in the lookup call "
                        f"for dataset `{cls_name}`"
                    )
                if not isinstance(kwargs[key], pd.Series):
                    raise ValueError(
                        f"Param `{key}` is not a pandas Series "
                        f"in the lookup call for dataset `{cls_name}`"
                    )
                arr.append(kwargs[key])

            if "fields" in kwargs:
                fields = kwargs["fields"]
            else:
                fields = []

            df = pd.concat(arr, axis=1)
            df.columns = key_fields
            res, found = dataset_lookup(
                cls_name,
                ts,
                fields,
                df,
            )

            return res.replace({np.nan: None}), found

        args = {k: pd.Series for k in key_fields}
        args["fields"] = List[str]
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
        dataset_cls: Type[T],
        history: Duration,
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
            history=duration_to_timedelta(history),
            lookup_fn=_create_lookup_function(dataset_cls.__name__, key_fields),
        )

    def wrap(c: Type[T]) -> Dataset:
        return _create_dataset(c, cast(Duration, history))

    if cls is None:
        # We're being called as @dataset()
        return wrap
    cls = cast(Type[T], cls)
    # @dataset decorator was used without arguments
    return wrap(cls)


def pipeline(id: int) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    if isinstance(id, Callable) or isinstance(id, Dataset):  # type: ignore
        if hasattr(id, "__name__"):
            callable_name = id.__name__  # type: ignore
        else:
            callable_name = str(id)
        raise ValueError(
            f"pipeline `{callable_name}` must be called with an id."
        )
    if type(id) != int:
        raise ValueError("pipeline id must be an integer, found %s" % type(id))

    def wrapper(pipeline_func: Callable) -> Callable:
        if not callable(pipeline_func):
            raise TypeError("pipeline functions must be callable.")
        pipeline_name = pipeline_func.__name__
        sig = inspect.signature(pipeline_func)
        cls_param = False
        params = []
        for name, param in sig.parameters.items():
            if not cls_param and param.name != "cls":
                raise TypeError(
                    f"pipeline functions are classmethods and must have cls "
                    f"as the first parameter, found `{name}` for pipeline `{pipeline_name}`."
                )
            elif not cls_param:
                cls_param = True
                continue

            if not isinstance(param.annotation, Dataset):
                if issubclass(param.annotation, Dataset):
                    raise TypeError(
                        f"pipeline `{pipeline_name}` must have "
                        f"Dataset[<Dataset Name>] as parameters."
                    )
                raise TypeError(
                    f"Parameter {name} is not a Dataset in {pipeline_name}"
                )
            params.append(param.annotation)

        setattr(
            pipeline_func,
            PIPELINE_ATTR,
            Pipeline(inputs=list(params), func=pipeline_func, id=id),
        )
        return pipeline_func

    return wrapper


@dataclass
class OnDemand:
    func: Callable
    # On Demand function bound with the class as the first argument
    bound_func: Callable
    pickled_func: bytes
    expires_after: Duration


def on_demand(expires_after: Duration):
    if not isinstance(expires_after, Duration):
        raise TypeError(
            "on_demand must be defined with a parameter "
            "expires_after of type Duration for eg: 30d."
        )

    def decorator(func):
        setattr(func, ON_DEMAND_ATTR, OnDemand(func, func, b"", expires_after))
        return func

    return decorator


def dataset_lookup(
    cls_name: str,
    ts: pd.Series,
    fields: List[str],
    keys: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    raise NotImplementedError("dataset_lookup should not be called directly.")


# ---------------------------------------------------------------------
# Dataset & Pipeline
# ---------------------------------------------------------------------


class Pipeline:
    terminal_node: _Node
    inputs: List[Dataset]
    _sign: str
    _root: str
    _nodes: List
    # Dataset it is part of
    _dataset_name: str
    func: Callable
    name: str
    id: int

    def __init__(self, inputs: List[Dataset], func: Callable, id: int):
        self.inputs = inputs
        self.func = func  # type: ignore
        self.name = func.__name__
        self.id = id

    # Validate the schema of all intermediate nodes
    # and return the schema of the terminal node.
    def get_terminal_schema(self) -> DSSchema:
        schema_validator = SchemaValidator()
        return schema_validator.validate(self)

    def signature(self):
        return f"{self._dataset_name}.{self._root}"

    def set_terminal_node(self, node: _Node):
        if node is None:
            raise Exception(f"Pipeline {self.name} cannot return None.")
        self.terminal_node = node

    def set_dataset_name(self, ds_name: str):
        self._dataset_name = ds_name

    def __str__(self):
        return f"Pipeline({self.signature()})"

    @property
    def dataset_name(self):
        return self._dataset_name


class Dataset(_Node[T]):
    """Dataset is a collection of data."""

    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    _name: str
    _on_demand: Optional[OnDemand]
    _history: datetime.timedelta
    _fields: List[Field]
    _key_fields: List[str]
    _pipelines: List[Pipeline]
    _timestamp_field: str
    __fennel_original_cls__: Any
    expectations: List[Expectations]
    lookup: Callable

    def __init__(
        self,
        cls: T,
        fields: List[Field],
        history: datetime.timedelta,
        lookup_fn: Optional[Callable] = None,
    ):
        super().__init__()
        self._name = cls.__name__  # type: ignore
        self.__name__ = self._name
        self._validate_field_names(fields)
        self._fields = fields
        self._add_fields_to_class()
        self._set_timestamp_field()
        self._set_key_fields()
        self._history = history
        self.__fennel_original_cls__ = cls
        self._pipelines = self._get_pipelines()
        self._on_demand = self._get_on_demand()
        self._sign = self._create_signature()
        if lookup_fn is not None:
            self.lookup = lookup_fn  # type: ignore
        propogate_fennel_attributes(cls, self)
        self.expectations = self._get_expectations()

    def __class_getitem__(cls, item):
        return item

    # ------------------- Public Methods --------------------------

    def signature(self):
        return self._sign

    # ------------------- Private Methods ----------------------------------

    def _validate_field_names(self, fields: List[Field]):
        names = set()
        exceptions = []
        for f in fields:
            if f.name in names:
                raise Exception(
                    f"Duplicate field name `{f.name}` found in "
                    f"dataset `{self._name}`."
                )
            names.add(f.name)
            if f.name in RESERVED_FIELD_NAMES:
                exceptions.append(
                    Exception(
                        f"Field name `{f.name}` is reserved. "
                        f"Please use a different name in dataset `{self._name}`."
                    )
                )
        if exceptions:
            raise Exception(exceptions)

    def _add_fields_to_class(self) -> None:
        for field in self._fields:
            setattr(self, field.name, field)

    def _create_signature(self):
        return fhash(
            self._name,
            self._history,
            self._on_demand.func if self._on_demand else None,
            self._on_demand.expires_after if self._on_demand else None,
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
                        f"Multiple timestamp fields are not supported in "
                        f"dataset `{self._name}`."
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
                raise ValueError(
                    f"Multiple timestamp fields are not "
                    f"supported in dataset `{self._name}`."
                )
        if not timestamp_field_set:
            raise ValueError(
                f"No timestamp field found in dataset `{self._name}`."
            )

    def _set_key_fields(self):
        key_fields = []
        for field in self._fields:
            if field.key:
                key_fields.append(field.name)
        self._key_fields = key_fields

    def __repr__(self):
        return f"Dataset({self.__name__}, {self._fields})"

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
            cls_param = False
            key_fields = [f for f in self._fields if f.key]
            key_index = 0
            for name, param in sig.parameters.items():
                if not cls_param and param.name != "cls":
                    raise TypeError(
                        f"on_demand functions are classmethods and must have "
                        f"cls as the first parameter, found {name}."
                    )
                elif not cls_param:
                    cls_param = True
                    continue

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
            on_demand.bound_func = functools.partial(on_demand.func, self)
            cloudpickle.register_pickle_by_value(
                inspect.getmodule(on_demand.func)
            )
            on_demand.pickled_func = cloudpickle.dumps(on_demand.bound_func)
        return on_demand

    def _get_pipelines(self) -> List[Pipeline]:
        pipelines = []
        dataset_name = self._name
        ids = set()
        names = set()
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, PIPELINE_ATTR):
                continue

            pipeline = getattr(method, PIPELINE_ATTR)

            if pipeline.id in ids:
                raise ValueError(
                    f"Duplicate pipeline id {pipeline.id} for dataset {dataset_name}."
                )
            ids.add(pipeline.id)
            if pipeline.name in names:
                raise ValueError(
                    f"Duplicate pipeline name {pipeline.name} for dataset {dataset_name}."
                )
            names.add(pipeline.name)
            pipeline.set_terminal_node(pipeline.func(self, *pipeline.inputs))
            pipelines.append(pipeline)
            pipelines[-1].set_dataset_name(dataset_name)

        self._validate_pipelines(pipelines)
        return pipelines

    def _validate_pipelines(self, pipelines: List[Pipeline]):
        exceptions = []
        ds_schema = DSSchema(
            keys={f.name: f.dtype for f in self.fields if f.key},
            values={
                f.name: f.dtype
                for f in self.fields
                if not f.key and f.name != self._timestamp_field
            },
            timestamp=self.timestamp_field,
        )

        for pipeline in pipelines:
            pipeline_schema = pipeline.get_terminal_schema()
            err = pipeline_schema.matches(
                ds_schema, f"pipeline {pipeline.name} output", self._name
            )
            if len(err) > 0:
                exceptions.extend(err)
        if exceptions:
            raise TypeError(exceptions)

    def _get_expectations(self):
        expectation = None
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, GE_ATTR_FUNC):
                continue
            if expectation is not None:
                raise ValueError(
                    f"Multiple expectations are not supported for dataset {self._name}."
                )
            expectation = getattr(method, GE_ATTR_FUNC)
        if expectation is None:
            return None
        # Check that the expectation function only takes 1 parameter: cls.
        sig = inspect.signature(expectation.func)
        if len(sig.parameters) != 1:
            raise ValueError(
                f"Expectation function {expectation.func} must take only "
                f"cls as a parameter."
            )
        expectation.suite = f"dataset_{self._name}_expectations"
        expectation.expectations = expectation.func(self)
        if hasattr(expectation.func, "__fennel_metadata__"):
            raise ValueError("Expectations cannot have metadata.")
        return expectation

    @property
    def timestamp_field(self):
        return self._timestamp_field

    @property
    def key_fields(self):
        return self._key_fields

    @property
    def on_demand(self):
        return self._on_demand

    @property
    def fields(self):
        return self._fields


# ---------------------------------------------------------------------
# Visitor
# ---------------------------------------------------------------------


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
        elif isinstance(obj, Rename):
            return self.visitRename(obj)
        elif isinstance(obj, Drop):
            return self.visitDrop(obj)
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

    def visitRename(self, obj):
        raise NotImplementedError()

    def visitDrop(self, obj):
        raise NotImplementedError()


@dataclass
class DSSchema:
    keys: Dict[str, Type]
    values: Dict[str, Type]
    timestamp: str
    name: str = ""

    def fields(self) -> List[str]:
        return (
            [x for x in self.keys.keys()]
            + [x for x in self.values.keys()]
            + [self.timestamp]
        )

    def get_type(self, field) -> Type:
        if field in self.keys:
            return self.keys[field]
        elif field in self.values:
            return self.values[field]
        elif field == self.timestamp:
            return datetime.datetime
        else:
            raise Exception(f"field {field} not found in schema of {self.name}")

    def rename_column(self, old_name: str, new_name: str):
        if old_name in self.keys:
            self.keys[new_name] = self.keys.pop(old_name)
        elif old_name in self.values:
            self.values[new_name] = self.values.pop(old_name)
        elif old_name == self.timestamp:
            self.timestamp = new_name
        else:
            raise Exception(
                f"field {old_name} not found in schema of {self.name}"
            )

    def drop_column(self, name: str):
        if name in self.keys:
            self.keys.pop(name)
        elif name in self.values:
            self.values.pop(name)
        elif name == self.timestamp:
            raise Exception(
                f"cannot drop timestamp field {name} from {self.name}"
            )
        else:
            raise Exception(f"field {name} not found in schema of {self.name}")

    def matches(
        self, other_schema: DSSchema, this_name: str, other_name: str
    ) -> List[TypeError]:
        def check_fields_one_way(
            this_schema: Dict[str, Type],
            other_schema: Dict[str, Type],
            check_type: str,
        ):
            for name, dtype in this_schema.items():
                if name not in other_schema:
                    return TypeError(
                        f"Field `{name}` is present in `{this_name}` "
                        f"`{check_type}` schema but not "
                        f"present in `{other_name} {check_type}` schema."
                    )

                dtype = get_dtype(dtype)
                other_schema[name] = get_dtype(other_schema[name])

                if dtype != other_schema[name]:
                    return TypeError(
                        f"Field `{name}` has type `{dtype_to_string(dtype)}` in"
                        f" `{this_name} {check_type}` "
                        f"schema but type `{dtype_to_string(other_schema[name])}` "
                        f"in `{other_name} {check_type}` schema."
                    )

        def check_field_other_way(
            other_schema: Dict[str, Type],
            this_schema: Dict[str, Type],
            check_type: str,
        ):
            for name, dtype in other_schema.items():
                if name not in this_schema:
                    return TypeError(
                        f"Field `{name}` is present in `{other_name}` "
                        f"`{check_type}` schema "
                        f"but not present in `{this_name} {check_type}` schema."
                    )

        exceptions = []
        if self.timestamp != other_schema.timestamp:
            exceptions.append(
                TypeError(
                    f"Timestamp field mismatch: {self.timestamp} != "
                    f"`{other_schema.timestamp}` in `{this_name}` and `{other_name}`"
                )
            )
        exceptions.append(
            check_fields_one_way(self.keys, other_schema.keys, "key")
        )
        exceptions.append(
            check_field_other_way(other_schema.keys, self.keys, "key")
        )
        exceptions.append(
            check_fields_one_way(self.values, other_schema.values, "value")
        )
        exceptions.append(
            check_field_other_way(other_schema.values, self.values, "value")
        )
        exceptions = [x for x in exceptions if x is not None]
        return exceptions


class SchemaValidator(Visitor):
    def __init__(self):
        super(SchemaValidator, self).__init__()
        self.pipeline_name = ""

    def validate(self, pipe: Pipeline) -> DSSchema:
        self.pipeline_name = pipe.name
        return self.visit(pipe.terminal_node)

    def visit(self, obj) -> DSSchema:
        return super(SchemaValidator, self).visit(obj)

    def visitDataset(self, obj) -> DSSchema:
        return DSSchema(
            keys={f.name: f.dtype for f in obj.fields if f.key},
            values={
                f.name: f.dtype
                for f in obj.fields
                if not f.key and f.name != obj.timestamp_field
            },
            timestamp=obj.timestamp_field,
            name=f"'[Dataset:{obj._name}]'",
        )

    def visitTransform(self, obj) -> DSSchema:
        input_schema = self.visit(obj.node)
        if obj.schema is None:
            return input_schema
        else:
            node_name = f"'[Pipeline:{self.pipeline_name}]->transform node'"
            if input_schema.timestamp not in obj.schema:
                raise TypeError(
                    f"Timestamp field {input_schema.timestamp} must be "
                    f"present in schema of {node_name}."
                )
            for name, dtype in input_schema.keys.items():
                if name not in obj.schema:
                    raise TypeError(
                        f"Key field {name} must be present in schema of "
                        f"{node_name}."
                    )
                if dtype != obj.schema[name]:
                    raise TypeError(
                        f"Key field {name} has type {dtype_to_string(dtype)} in "
                        f"input schema "
                        f"of transform but type "
                        f"{dtype_to_string(obj.schema[name])} in output "
                        f"schema of {node_name}."
                    )
            inp_keys = input_schema.keys
            return DSSchema(
                keys=inp_keys,
                values={
                    f: dtype
                    for f, dtype in obj.schema.items()
                    if f not in inp_keys.keys() and f != input_schema.timestamp
                },
                timestamp=input_schema.timestamp,
                name=node_name,
            )

    def visitFilter(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        input_schema.name = f"'[Pipeline:{self.pipeline_name}]->filter node'"
        return input_schema

    def visitAggregate(self, obj) -> DSSchema:
        input_schema = self.visit(obj.node)
        keys = {f: input_schema.get_type(f) for f in obj.keys}
        values = {}
        for agg in obj.aggregates:
            if hasattr(agg, "of"):
                values[agg.into_field] = input_schema.get_type(agg.of)
            else:
                values[agg.into_field] = int
        return DSSchema(
            keys=keys,
            values=values,
            timestamp=input_schema.timestamp,
            name=f"'[Pipeline:{self.pipeline_name}]->aggregate node'",
        )

    def visitJoin(self, obj) -> DSSchema:
        def is_subset(subset: List[str], superset: List[str]) -> bool:
            return set(subset).issubset(set(superset))

        def make_types_optional(types: Dict[str, Type]) -> Dict[str, Type]:
            return {
                k: Optional[get_dtype(v)]  # type: ignore
                for k, v in types.items()
            }

        left_schema = self.visit(obj.node)
        right_schema = self.visit(obj.dataset)

        if obj.on is not None and len(obj.on) > 0:
            # obj.on should be the key of the right dataset
            if set(obj.on) != set(right_schema.keys.keys()):
                raise ValueError(
                    f"on field {obj.on} are not the key fields of the right "
                    f"dataset {obj.dataset._name}."
                )
            # Check the schemas of the keys
            for key in obj.on:
                if left_schema.get_type(key) != right_schema.get_type(key):
                    raise TypeError(
                        f"Key field {key} has type {dtype_to_string(left_schema.get_type(key))} "
                        f"in left schema but type "
                        f"{dtype_to_string(right_schema.get_type(key))} in right schema."
                    )
        else:
            #  obj.right_on should be the keys of the right dataset
            if set(obj.right_on) != set(right_schema.keys.keys()):
                raise ValueError(
                    f"right_on field {obj.right_on} are not the key fields of "
                    f"the right dataset {obj.dataset._name}."
                )
            #  obj.left_on should be a subset of the schema of the left dataset
            if not is_subset(obj.left_on, list(left_schema.fields())):
                raise ValueError(
                    f"left_on field {obj.left_on} are not the key fields of "
                    f"the left dataset {obj.node.dataset._name}."
                )
            # Check the schemas of the keys
            for lkey, rkey in zip(obj.left_on, obj.right_on):
                if left_schema.get_type(lkey) != right_schema.get_type(rkey):
                    raise TypeError(
                        f"Key field {lkey} has type"
                        f" {dtype_to_string(left_schema.get_type(lkey))} "
                        f"in left schema but, key field {rkey} has type "
                        f"{dtype_to_string(right_schema.get_type(rkey))} in "
                        f"right schema."
                    )
        values = copy.deepcopy(left_schema.values)
        values.update(make_types_optional(right_schema.values))
        return DSSchema(
            keys=copy.deepcopy(left_schema.keys),
            timestamp=left_schema.timestamp,
            values=values,
            name=f"'[Pipeline:{self.pipeline_name}]->join node'",
        )

    def visitUnion(self, obj) -> DSSchema:
        if len(obj.nodes) == 0:
            raise ValueError("Union must have at least one node.")
        schema = self.visit(obj.nodes[0])
        index = 1
        exceptions = []
        for node in obj.nodes[1:]:
            node_schema = self.visit(node)
            err = node_schema.matches(
                schema,
                "Union node index 0",
                f"Union node index {index} of pipeline {self.pipeline_name}",
            )
            exceptions.extend(err)
        if len(exceptions) > 0:
            raise ValueError(f"Union node schemas do not match: {exceptions}")
        schema.name = f"'[Pipeline:{self.pipeline_name}]->union node'"
        return schema

    def visitRename(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        input_schema.name = f"'[Pipeline:{self.pipeline_name}]->rename node'"
        for old, new in obj.column_mapping.items():
            print(input_schema.fields())
            if old not in input_schema.fields():
                raise ValueError(
                    f"Field {old} does not exist in schema of "
                    f"rename node {input_schema.name}."
                )
            if new in input_schema.fields():
                raise ValueError(
                    f"Field {new} already exists in schema of "
                    f"rename node {input_schema.name}."
                )
            input_schema.rename_column(old, new)
        return input_schema

    def visitDrop(self, obj):
        input_schema = copy.deepcopy(self.visit(obj.node))
        input_schema.name = f"'[Pipeline:{self.pipeline_name}]->drop node'"
        for field in obj.columns:
            if field not in input_schema.fields():
                raise ValueError(
                    f"Field {field} does not exist in schema of "
                    f"drop node {obj.name}."
                )
            input_schema.drop_column(field)
        return input_schema
