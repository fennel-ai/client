from __future__ import annotations

import copy
import datetime
import functools
import inspect
import sys
from dataclasses import dataclass
from enum import Enum
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
    get_args,
    Set,
)

import numpy as np
import pandas as pd
from typing_extensions import Literal

import fennel.gen.window_pb2 as window_proto
from fennel.datasets.aggregate import (
    AggregateType,
    Average,
    Count,
    Distinct,
    LastK,
    Sum,
    Min,
    Max,
    Stddev,
)
from fennel.dtypes.dtypes import (
    get_fennel_struct,
    Window,
)
from fennel.internal_lib.duration import (
    Duration,
    duration_to_timedelta,
    timedelta_to_micros,
)
from fennel.internal_lib.schema import (
    get_primitive_dtype,
    fennel_is_optional,
    fennel_get_optional_inner,
    get_pd_dtype,
    is_hashable,
    parse_json,
    get_python_type_from_pd,
    FENNEL_STRUCT_SRC_CODE,
    FENNEL_STRUCT_DEPENDENCIES_SRC_CODE,
)
from fennel.internal_lib.utils import (
    dtype_to_string,
    get_origin,
)
from fennel.lib.expectations import Expectations, GE_ATTR_FUNC
from fennel.lib.includes import TierSelector
from fennel.lib.metadata import (
    meta,
    OWNER,
    get_meta_attr,
    set_meta_attr,
)
from fennel.lib.params import FENNEL_INPUTS
from fennel.utils import (
    fhash,
    parse_annotation_comments,
    propogate_fennel_attributes,
    FENNEL_VIRTUAL_FILE,
)

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar("T")

PIPELINE_ATTR = "__fennel_pipeline__"
ON_DEMAND_ATTR = "__fennel_on_demand__"

DEFAULT_RETENTION = Duration("2y")
DEFAULT_EXPIRATION = Duration("30d")
DEFAULT_VERSION = 1
RESERVED_FIELD_NAMES = [
    "cls",
    "self",
    "fields",
    "key_fields",
    "on_demand",
    "timestamp_field",
    "fqn",
]

primitive_numeric_types = [int, float, pd.Int64Dtype, pd.Float64Dtype]

# ---------------------------------------------------------------------
# Field
# ---------------------------------------------------------------------


@dataclass
class Field:
    name: Optional[str]
    dataset_name: Optional[str]
    dataset: Optional[Dataset]
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
        return fennel_is_optional(self.dtype)

    def fqn(self) -> str:
        return f"{self.dataset_name}.{self.name}"

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
        field.dataset_name = cls.__name__  # type: ignore
    else:
        field = Field(
            name=annotation_name,
            dataset_name=cls.__name__,  # type: ignore
            dataset=None,  # set as part of dataset initialization
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
        # Initially None fields are assigned later
        Field(
            key=key,
            dataset_name=None,
            dataset=None,
            timestamp=timestamp,
            name=None,
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
        return Transform(self, func, copy.deepcopy(schema))

    def filter(self, func: Callable) -> _Node:
        return Filter(self, func)

    def assign(self, name: str, dtype: Type, func: Callable) -> _Node:
        return Assign(self, name, dtype, func)

    def groupby(self, *args) -> GroupBy:
        return GroupBy(self, *args)

    def join(
        self,
        other: Dataset,
        how: Literal["inner", "left"],
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
        within: Tuple[Duration, Duration] = ("forever", "0s"),
    ) -> Join:
        if not isinstance(other, Dataset) and isinstance(other, _Node):
            raise ValueError(
                "Cannot join with an intermediate dataset, i.e something defined inside a pipeline."
                " Only joining against keyed datasets is permitted."
            )
        if not isinstance(other, _Node):
            raise TypeError("Cannot join with a non-dataset object")
        return Join(self, other, within, how, on, left_on, right_on)

    def rename(self, columns: Dict[str, str]) -> _Node:
        return Rename(self, columns)

    def drop(self, *args, columns: Optional[List[str]] = None) -> _Node:
        drop_cols = _Node.__get_drop_args(*args, columns=columns)
        return self.__drop(drop_cols)

    def dropnull(self, *args, columns: Optional[List[str]] = None) -> _Node:
        cols = None
        if len(args) == 0 and columns is None:  # dropnull with no get_args
            cols = self.dsschema().get_optional_cols()
        else:
            cols = _Node.__get_drop_args(
                *args, columns=columns, name="dropnull"
            )
        return DropNull(self, cols)

    def select(self, *args, columns: Optional[List[str]] = None) -> _Node:
        cols = _Node.__get_drop_args(*args, columns=columns, name="select")
        ts = self.dsschema().timestamp
        # Keep the timestamp col
        drop_cols = list(
            filter(
                lambda c: c not in cols and c != ts, self.dsschema().fields()
            )
        )
        # All the cols were selected
        if len(drop_cols) == 0:
            return self
        return self.__drop(drop_cols, name="select")

    def dedup(self, *args, by: Optional[List[str]] = None) -> _Node:
        # If 'by' is not provided, dedup by all value fields.
        # Note: we don't use key fields because dedup cannot be applied on keyed datasets.
        collist: List[str] = []
        if len(args) == 0 and by is None:
            collist = list(self.dsschema().values.keys())
        elif len(args) > 0 and by is None:
            collist = args  # type: ignore
        elif len(args) == 0 and by is not None and isinstance(by, list):
            collist = by
        elif len(args) == 0 and by is not None and isinstance(by, str):
            collist = [by]
        else:
            raise ValueError(
                "Invalid arguments to dedup. Must specify either 'by' or positional arguments."
            )

        return Dedup(self, collist)

    def explode(self, *args, columns: List[str] = None) -> _Node:
        columns = _Node.__get_drop_args(*args, columns=columns, name="explode")
        return Explode(self, columns)

    def isignature(self):
        raise NotImplementedError

    def schema(self):
        return copy.deepcopy(self.dsschema().schema())

    def dsschema(self):
        return copy.deepcopy(self.dsschema())

    def num_out_edges(self) -> int:
        return len(self.out_edges)

    def __drop(self, columns: List[str], name="drop") -> _Node:
        return Drop(self, columns, name=name)

    @classmethod
    def __get_drop_args(
        cls, *args, columns: Optional[List[str]], name="drop"
    ) -> List[str]:
        if args and columns is not None:
            raise ValueError(
                f"can only specify either 'columns' or positional arguments to {name}, not both."
            )
        elif columns is not None:
            return columns
        elif args:
            if len(args) == 1 and isinstance(args[0], list):
                return args[0]
            else:
                return [*args]
        else:
            raise ValueError(
                f"must specify either 'columns' or positional arguments to {name}."
            )

    def __add__(self, other):
        return Union_(self, other)


class Transform(_Node):
    def __init__(self, node: _Node, func: Callable, schema: Optional[Dict]):
        super().__init__()
        self.func = func
        self.node = node
        self.node.out_edges.append(self)
        self.new_schema = schema

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)

    def dsschema(self):
        if self.new_schema is None:
            return self.node.dsschema()
        input_schema = self.node.dsschema()
        inp_keys = input_schema.keys
        return DSSchema(
            keys=inp_keys,
            values={
                f: get_pd_dtype(dtype)
                for f, dtype in self.new_schema.items()
                if f not in inp_keys.keys() and f != input_schema.timestamp
            },
            timestamp=input_schema.timestamp,
        )


class Assign(_Node):
    def __init__(
        self, node: _Node, column: str, output_type: Type, func: Callable
    ):
        super().__init__()
        self.node = node
        self.node.out_edges.append(self)
        self.func = func
        self.column = column
        self.output_type = output_type

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(
                self.node._name,
                self.func,
                self.column,
                self.output_type.__name__,
            )
        return fhash(
            self.node.signature(),
            self.func,
            self.column,
            self.output_type.__name__,
        )

    def dsschema(self):
        input_schema = self.node.dsschema()
        input_schema.update_column(self.column, get_pd_dtype(self.output_type))
        return input_schema


class Filter(_Node):
    def __init__(self, node: _Node, func: Callable):
        super().__init__()
        self.node = node
        self.node.out_edges.append(self)
        self.func = func  # noqa: E731

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)

    def dsschema(self):
        return self.node.dsschema()


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

    def dsschema(self):
        input_schema = self.node.dsschema()
        keys = {f: input_schema.get_type(f) for f in self.keys}
        values = {}
        for agg in self.aggregates:
            if isinstance(agg, Count):
                values[agg.into_field] = pd.Int64Dtype
            elif isinstance(agg, Sum):
                dtype = input_schema.get_type(agg.of)
                dtype = get_primitive_dtype(dtype)
                if dtype not in primitive_numeric_types:
                    raise TypeError(
                        f"Cannot sum field {agg.of} of type {dtype_to_string(dtype)}"
                    )
                values[agg.into_field] = dtype
            elif isinstance(agg, Min) or isinstance(agg, Max):
                dtype = input_schema.get_type(agg.of)
                dtype = get_primitive_dtype(dtype)
                values[agg.into_field] = dtype
            elif isinstance(agg, Distinct):
                dtype = input_schema.get_type(agg.of)
                list_type = get_python_type_from_pd(dtype)
                values[agg.into_field] = List[list_type]  # type: ignore
            elif isinstance(agg, Average):
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            elif isinstance(agg, LastK):
                dtype = input_schema.get_type(agg.of)
                list_type = get_python_type_from_pd(dtype)
                values[agg.into_field] = List[list_type]  # type: ignore
            elif isinstance(agg, Stddev):
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            else:
                raise TypeError(f"Unknown aggregate type {type(agg)}")
        return DSSchema(
            keys=keys,
            values=values,  # type: ignore
            timestamp=input_schema.timestamp,
        )


class GroupBy:
    def __init__(self, node: _Node, *args):
        super().__init__()
        self.keys = args
        self.node = node
        self.node.out_edges.append(self)

    def aggregate(self, *args) -> _Node:
        if len(args) == 0:
            raise TypeError(
                "aggregate operator expects atleast one aggregation operation"
            )
        if len(args) == 1 and isinstance(args[0], list):
            aggregates = args[0]
        else:
            aggregates = list(args)
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        return Aggregate(self.node, list(self.keys), aggregates)

    def first(self) -> _Node:
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        return First(self.node, list(self.keys))  # type: ignore

    def window(
        self,
        type: str,
        field: str,
        gap: Optional[str] = None,
        duration: Optional[str] = None,
        stride: Optional[str] = None,
    ) -> WindowOperator:
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        return WindowOperator(
            self.node,
            keys=list(self.keys),
            type=WindowType(type),
            field=field,
            gap=gap,
            duration=duration,
            stride=stride,
        )

    def dsschema(self):
        raise NotImplementedError


class Dedup(_Node):
    def __init__(self, node: _Node, by: List[str]):
        super().__init__()
        self.node = node
        self.by = by
        self.node.out_edges.append(self)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.by)
        return fhash(self.node.signature(), self.by)

    def dsschema(self):
        return self.node.dsschema()


class Explode(_Node):
    def __init__(self, node: _Node, columns: List[str]):
        super().__init__()
        self.node = node
        self.columns = columns
        self.node.out_edges.append(self)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.columns)
        return fhash(self.node.signature(), self.columns)

    def dsschema(self):
        dsschema = copy.deepcopy(self.node.dsschema())
        if not isinstance(self.columns, list):
            raise TypeError(
                f"Columns argument to explode must be a list, found {type(self.columns)}"
            )
        for c in self.columns:
            # Check that List[t] is a list type
            if not get_origin(dsschema.values[c]) is list:
                raise TypeError(
                    f"Cannot explode column `{c}` of type `{dsschema.values[c]}`, explode expects columns of List type"
                )
            # extract type T from List[t]
            dsschema.values[c] = Optional[get_args(dsschema.values[c])[0]]
            dsschema.values[c] = get_pd_dtype(dsschema.values[c])
        return dsschema


class First(_Node):
    def __init__(self, node: _Node, keys: List[str]):
        super().__init__()
        self.keys = keys
        self.node = node
        self.node.out_edges.append(self)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.keys)
        return fhash(self.node.signature(), self.keys)

    def dsschema(self):
        input_schema = self.node.dsschema()
        keys = {f: input_schema.get_type(f) for f in self.keys}
        value_fields = [
            f
            for f in {**input_schema.values, **input_schema.keys}
            if f not in self.keys and f != input_schema.timestamp
        ]
        values = {f: input_schema.get_type(f) for f in value_fields}
        return DSSchema(
            keys=keys,
            timestamp=input_schema.timestamp,
            values=values,
        )


class Join(_Node):
    def __init__(
        self,
        node: _Node,
        dataset: Dataset,
        within: Tuple[Duration, Duration],
        how: Literal["inner", "left"],
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
        # Currently not supported
        lsuffix: str = "",
        rsuffix: str = "",
    ):
        if how not in ["inner", "left"]:
            raise ValueError(
                f"Join type {how} is not supported. Currently only 'inner' and 'left' joins are supported"
            )
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError("Cannot specify on and left_on/right_on")
            if not isinstance(on, list):
                raise ValueError("on must be a list of keys")
        else:
            if left_on is None or right_on is None:
                raise ValueError("Must specify on or left_on/right_on")
            if not isinstance(left_on, list) or not isinstance(right_on, list):
                raise ValueError(
                    "Must specify left_on and right_on as a list of keys"
                )
        super().__init__()
        self.node = node
        self.dataset = dataset
        self.on = on
        self.left_on = left_on
        self.right_on = right_on
        self.within = within
        self.how = how
        self.lsuffix = lsuffix
        self.rsuffix = rsuffix
        self.node.out_edges.append(self)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(
                self.node._name,
                self.dataset._name,
                self.on,
                self.left_on,
                self.right_on,
                self.how,
                self.lsuffix,
                self.rsuffix,
            )
        return fhash(
            self.node.signature(),
            self.dataset._name,
            self.on,
            self.left_on,
            self.right_on,
            self.within,
            self.how,
            self.lsuffix,
            self.rsuffix,
        )

    def dsschema(self):
        def make_types_optional(types: Dict[str, Type]) -> Dict[str, Type]:
            return {
                k: Optional[get_primitive_dtype(v)]  # type: ignore
                for k, v in types.items()
            }

        left_dsschema: DSSchema = copy.deepcopy(self.node.dsschema())
        left_schema: Dict[str, Type] = left_dsschema.schema()
        right_value_schema: Dict[str, Type] = copy.deepcopy(
            self.dataset.dsschema().values
        )

        rhs_keys = set(self.dataset.dsschema().keys)
        join_keys = set(self.on) if self.on is not None else set(self.right_on)
        # Ensure on or right_on are the keys of the right dataset
        if join_keys != rhs_keys:
            raise ValueError(
                f"Join fields `{join_keys}` are not keys fields in right dataset `{self.dataset.dsschema().name}`"
            )

        common_cols = set(left_schema.keys()) & set(right_value_schema.keys())
        # for common values, suffix column name in left_schema with lsuffix and right_schema with rsuffix
        for col in common_cols:
            if self.lsuffix != "" and (col + self.lsuffix) in left_schema:
                raise ValueError(
                    f"Column name collision. `{col + self.lsuffix}` already exists in schema of left input {left_dsschema.name}, while joining with {self.dataset.dsschema().name}"
                )
            if (
                self.rsuffix != ""
                and (col + self.rsuffix) in right_value_schema
            ):
                raise ValueError(
                    f"Column name collision. `{col + self.rsuffix}` already exists in schema of right input {self.dataset.dsschema().name}, while joining with {self.dataset.dsschema().name}"
                )
            left_dsschema.rename_column(col, col + self.lsuffix)
            left_schema[col + self.lsuffix] = left_schema.pop(col)
            right_value_schema[col + self.rsuffix] = right_value_schema.pop(col)

        # If "how" is "left", make fields of right schema optional
        if self.how == "left":
            right_value_schema = make_types_optional(right_value_schema)

        # Add right value columns to left schema. Check for column name collisions
        joined_dsschema = copy.deepcopy(left_dsschema)
        for col, dtype in right_value_schema.items():
            if col in left_schema:
                raise ValueError(
                    f"Column name collision. `{col}` already exists in schema of left input {left_dsschema.name}, while joining with {self.dataset.dsschema().name}"
                )
            joined_dsschema.append_value_column(col, dtype)

        return joined_dsschema


class Union_(_Node):
    def __init__(self, node: _Node, other: _Node):
        super().__init__()
        self.nodes = [node, other]
        node.out_edges.append(self)
        other.out_edges.append(self)

    def signature(self):
        return fhash([n.signature() for n in self.nodes])

    def dsschema(self):
        if len(self.nodes) == 0:
            raise ValueError("Cannot union empty list of nodes")
        return self.nodes[0].dsschema()


class Rename(_Node):
    def __init__(self, node: _Node, columns: Dict[str, str]):
        super().__init__()
        self.node = node
        self.column_mapping = columns
        self.node.out_edges.append(self)

    def signature(self):
        return fhash(self.node.signature(), self.column_mapping)

    def dsschema(self):
        input_schema = copy.deepcopy(self.node.dsschema())
        for old, new in self.column_mapping.items():
            input_schema.rename_column(old, new)
        return input_schema


class Drop(_Node):
    def __init__(self, node: _Node, columns: List[str], name="drop"):
        super().__init__()
        self.node = node
        self.columns = columns
        self.__name = name
        self.node.out_edges.append(self)

    def signature(self):
        return fhash(self.node.signature(), self.columns)

    def dsschema(self):
        input_schema = copy.deepcopy(self.node.dsschema())
        for field in self.columns:
            input_schema.drop_column(field)
        return input_schema

    @property
    def name(self):
        return self.__name


class DropNull(_Node):
    def __init__(self, node: _Node, columns: List[str]):
        super().__init__()
        self.node = node
        self.columns = columns
        self.node.out_edges.append(self)

    def signature(self):
        return fhash(self.node.signature(), self.columns)

    def dsschema(self):
        input_schema = copy.deepcopy(self.node.dsschema())
        for field in self.columns:
            input_schema.drop_null_column(field)
        return input_schema


class WindowType(str, Enum):
    Sessionize = "session"
    Tumbling = "tumbling"
    Hopping = "hopping"

    @classmethod
    def _missing_(cls, value):
        valid_types = [m.value for m in cls]
        raise ValueError(
            f"`{value}` is not a valid 'type' in 'window' operator. "
            f"'type' in window operator must be one of {valid_types}"
        )


@dataclass
class Summary:
    field: str
    dtype: Type
    summarize_func: Callable


class WindowOperator(_Node):
    def __init__(
        self,
        node: _Node,
        keys: List[str],
        type: WindowType,
        field: str,
        gap: Optional[Duration],
        duration: Optional[Duration],
        stride: Optional[Duration],
    ):
        super().__init__()
        if len(keys) == 0:
            raise ValueError(
                "'group_by' before 'window' must specify at least one key"
            )
        if type == WindowType.Hopping:
            if stride is None:
                raise ValueError("'hopping window' must specify stride")

            if duration is None:
                raise ValueError("'hopping window' must specify duration")

            if gap is not None:
                raise ValueError("'hopping window' doesn't allow gap parameter")

        if type == WindowType.Sessionize:
            if gap is None:
                raise ValueError("'sessionize window' must specify gap")

            if duration is not None:
                raise ValueError(
                    "'sessionize window' doesn't allow duration parameter"
                )

            if stride is not None:
                raise ValueError(
                    "'sessionize window' doesn't allow stride parameter"
                )

        if type == WindowType.Tumbling:
            if duration is None:
                raise ValueError("'tumbling window' must specify duration")

            if gap is not None:
                raise ValueError(
                    "'tumbling window' doesn't allow gap parameter"
                )

            if stride is not None:
                raise ValueError(
                    "'tumbling window' doesn't allow stride parameter"
                )

        self.input_keys = keys.copy()
        self.by = keys.copy()
        keys.append(field)
        self.keys = keys
        self.type = type
        self.gap_timedelta = gap if gap is None else duration_to_timedelta(gap)
        self.duration_timedelta = (
            duration if duration is None else duration_to_timedelta(duration)
        )
        self.stride_timedelta = (
            stride if stride is None else duration_to_timedelta(stride)
        )
        if type == WindowType.Hopping:
            if timedelta_to_micros(self.duration_timedelta) < timedelta_to_micros(self.stride_timedelta):  # type: ignore
                raise ValueError(
                    "stride parameters is larger than duration parameters which is not supported in 'hopping window'"
                )
        self.field = field
        self.node = node
        self.node.out_edges.append(self)
        self.summary: Optional[Summary] = None

    def signature(self):
        return fhash(
            self.node.signature(),
            self.keys,
            self.type,
            self.gap_timedelta,
            self.field,
        )

    def dsschema(self):
        input_schema = self.node.dsschema()
        keys = {
            f: input_schema.get_type(f) for f in self.keys if f != self.field
        }
        keys[self.field] = Window
        if self.summary is not None:
            values = {self.summary.field: get_pd_dtype(self.summary.dtype)}
        else:
            values = {}
        return DSSchema(
            keys=keys,
            values=values,  # type: ignore
            timestamp=input_schema.timestamp,
        )

    def summarize(self, field: str, dtype: Type, func: Callable):
        if self.summary is not None:
            raise ValueError(
                f"'window' operator already have a summary field with name {self.summary.field}. window operator can only have 1 summary"
            )

        new_window_op = copy.deepcopy(self)
        new_window_op.summary = Summary(
            field=field, dtype=dtype, summarize_func=func
        )
        return new_window_op


# ---------------------------------------------------------------------
# dataset & pipeline decorators
# ---------------------------------------------------------------------


@overload
def dataset(  # noqa: E704
    *,
    version: Optional[int] = DEFAULT_VERSION,
    history: Optional[Duration] = DEFAULT_RETENTION,
) -> Callable[[Type[T]], Dataset]: ...


@overload
def dataset(cls: Type[T]) -> Dataset: ...  # noqa: E704


def dataset(
    cls: Optional[Type[T]] = None,
    version: Optional[int] = DEFAULT_VERSION,
    history: Optional[Duration] = DEFAULT_RETENTION,
) -> Union[Callable[[Type[T]], Dataset], Dataset]:
    """
    dataset is a decorator that creates a Dataset class.
    A dataset class contains the schema of the dataset, an optional pull
    function, and a set of pipelines that declare how to generate data for the
    dataset from other datasets.
    Parameters
    ----------
    version : int ( Optional )
        Version of the dataset
    history : Duration ( Optional )
        The amount of time to keep data in the dataset.
    max_staleness : Duration ( Optional )
        The maximum amount of time that data in the dataset can be stale.
    """

    try:
        if len(inspect.stack()) > 2:
            file_name = inspect.stack()[1].filename
        else:
            file_name = ""
    except Exception:
        file_name = ""

    def _create_lookup_function(
        cls_name: str, key_fields: List[str], struct_types: Dict[str, Any]
    ) -> Optional[Callable]:
        """
        :param cls_name: The name of the class being decorated
        :param key_fields: The fields that have been marked as keys.
        :param struct: Map from column names to Struct Classes. We use this to
        convert any dictionaries back to structs post lookup.
        """

        def lookup(
            ts: pd.Series, *args, **kwargs
        ) -> Tuple[pd.DataFrame, pd.Series]:
            if len(key_fields) == 0:
                raise Exception(
                    f"Trying to lookup dataset `{cls_name}` with no keys defined.\n"
                    f"Please define one or more keys using field(key=True) to perform a lookup."
                )
            if len(args) > 0:
                raise ValueError(
                    f"Lookup for dataset `{cls_name}` expects key value arguments and can "
                    f"optionally include fields, found {args}"
                )
            if len(kwargs) < len(key_fields):
                raise ValueError(
                    f"Lookup for dataset `{cls_name}` expects keys of the table being looked up and can "
                    f"optionally include fields, found {kwargs}"
                )
            # Check that ts is a series of datetime64[ns]
            if not isinstance(ts, pd.Series):
                raise ValueError(
                    f"Lookup for dataset `{cls_name}` expects a series of timestamps, found {type(ts)}"
                )
            if not np.issubdtype(ts.dtype, np.datetime64):
                raise ValueError(
                    f"Lookup for dataset `{cls_name}` expects a series of timestamps, found {ts.dtype}"
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
            # Convert any columns of struct type to objects from their
            # dictionary form
            for col, type_annotation in struct_types.items():
                res[col] = res[col].apply(
                    lambda x: parse_json(type_annotation, x)
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
        version: int,
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

        setattr(dataset_cls, FENNEL_VIRTUAL_FILE, file_name)

        key_fields = [f.name for f in fields if f.key]

        struct_types = {}
        struct_code = ""
        for name, annotation in cls_annotations.items():
            type_hints = f_get_type_hints(dataset_cls)
            if name in type_hints:
                annotation = type_hints[name]
            f_struct = get_fennel_struct(annotation)
            if isinstance(f_struct, Exception):
                raise TypeError(
                    f"Invalid type for field `{name}` in dataset {dataset_cls.__name__}: {f_struct}"
                )
            if f_struct is not None:
                if hasattr(f_struct, FENNEL_STRUCT_SRC_CODE):
                    code = getattr(f_struct, FENNEL_STRUCT_SRC_CODE)
                    if code not in struct_code:
                        struct_code = code + "\n\n" + struct_code
                if hasattr(f_struct, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE):
                    struct_code = (
                        getattr(f_struct, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE)
                        + "\n\n"
                        + struct_code
                    )
                struct_types[name] = annotation
        if struct_code:
            setattr(dataset_cls, FENNEL_STRUCT_SRC_CODE, struct_code)

        cls_module = inspect.getmodule(dataset_cls)
        owner = None
        if cls_module is not None and hasattr(cls_module, OWNER):
            owner = getattr(cls_module, OWNER)

        return Dataset(
            dataset_cls,
            fields,
            version=version,
            history=duration_to_timedelta(history),
            lookup_fn=_create_lookup_function(
                dataset_cls.__name__, key_fields, struct_types  # type: ignore
            ),
            owner=owner,
        )

    def wrap(c: Type[T]) -> Dataset:
        return _create_dataset(c, version, cast(Duration, history))  # type: ignore

    if cls is None:
        # We're being called as @dataset(arguments)
        return wrap
    cls = cast(Type[T], cls)
    # @dataset decorator was used without arguments
    return wrap(cls)


# Fennel implementation of get_type_hints which does not error on forward
# references not being types such as Embedding[4].
def f_get_type_hints(obj):
    annotations = getattr(obj, "__annotations__", {})
    type_hints = {}

    for name, annotation in annotations.items():
        # If the annotation is a string, try to evaluate it in the context of
        # the object's module
        if isinstance(annotation, str):
            module = sys.modules[obj.__module__]
            try:
                annotation = eval(annotation, module.__dict__)
            except Exception:
                pass

        type_hints[name] = annotation

    return type_hints


@overload
def pipeline(  # noqa: E704
    *,
    tier: Optional[Union[str, List[str]]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


@overload
def pipeline(  # noqa: E704
    pipeline_func: Callable,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


def pipeline(
    pipeline_func: Callable = None,
    tier: Optional[Union[str, List[str]]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def _create_pipeline(
        pipeline_func: Callable,
        tier: Optional[Union[str, List[str]]] = None,
    ) -> Callable:
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
            break
        if not hasattr(pipeline_func, FENNEL_INPUTS):
            raise TypeError(
                f"pipeline `{pipeline_name}` must have "
                f"Datasets as @input parameters."
            )
        inputs = getattr(pipeline_func, FENNEL_INPUTS)
        for inp in inputs:
            if not isinstance(inp, Dataset):
                if issubclass(inp, Dataset):
                    raise TypeError(
                        f"pipeline `{pipeline_name}` must have "
                        f"Dataset[<Dataset Name>] as parameters."
                    )
                if hasattr(inp, "_name"):
                    name = inp._name
                elif hasattr(inp, "__name__"):
                    name = inp.__name__
                else:
                    name = str(inp)
                raise TypeError(
                    f"Parameter {name} is not a Dataset in {pipeline_name}"
                )
            if inp.is_terminal:
                raise TypeError(
                    f"pipeline `{pipeline_name}` cannot have terminal "
                    f"dataset `{inp._name}` as input."
                )
            params.append(inp)

        setattr(
            pipeline_func,
            PIPELINE_ATTR,
            Pipeline(
                inputs=list(params),
                func=pipeline_func,
                tier=tier,
            ),
        )
        return pipeline_func

    def wrap(pipeline_func: Callable) -> Callable:
        return _create_pipeline(pipeline_func, tier)  # type: ignore

    if pipeline_func is None:
        # We're being called as @pipeline(arguments)
        return wrap
    pipeline_func = cast(Callable, pipeline_func)
    # @pipeline decorator was used without arguments
    return wrap(pipeline_func)


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
    version: int
    active: bool
    tier: TierSelector

    def __init__(
        self,
        inputs: List[Dataset],
        func: Callable,
        tier: Optional[Union[str, List[str]]] = None,
    ):
        self.inputs = inputs
        self.func = func  # type: ignore
        self.name = func.__name__
        self.tier = TierSelector(tier)

    # Validate the schema of all intermediate nodes
    # and return the schema of the terminal node.
    def get_terminal_schema(self) -> DSSchema:
        schema_validator = SchemaValidator()
        return schema_validator.validate(self)

    def signature(self):
        return f"{self._dataset_name}.{self._root}"

    def set_terminal_node(self, node: _Node) -> bool:
        if node is None:
            raise Exception(f"Pipeline {self.name} cannot return None.")
        self.terminal_node = node
        return isinstance(node, Aggregate)

    def set_dataset_name(self, ds_name: str):
        self._dataset_name = ds_name

    def __str__(self):
        return f"Pipeline({self.signature()})"

    @property
    def dataset_name(self):
        return self._dataset_name

    @property
    def fqn(self):
        return f"{self._dataset_name}-{self.name}"


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
    is_terminal: bool

    def __init__(
        self,
        cls: T,
        fields: List[Field],
        version: int,
        history: datetime.timedelta,
        lookup_fn: Optional[Callable] = None,
        owner: Optional[str] = None,
    ):
        super().__init__()
        self._name = cls.__name__  # type: ignore
        self.__name__ = self._name
        self.is_terminal = False
        self._validate_field_names(fields)
        self._fields = fields
        self._add_fields_to_class()
        self._set_timestamp_field()
        self._set_key_fields()
        self._version = version
        self._history = history
        self.__fennel_original_cls__ = cls
        propogate_fennel_attributes(cls, self)
        self._pipelines = self._get_pipelines()
        self._on_demand = self._get_on_demand()
        self._sign = self._create_signature()
        if lookup_fn is not None:
            self.lookup = lookup_fn  # type: ignore
        self._add_fields_as_attributes()
        self.expectations = self._get_expectations()
        setattr(self, OWNER, owner)

    def __class_getitem__(cls, item):
        return item

    # ------------------- Public Methods --------------------------

    def signature(self):
        return self._sign

    def dsschema(self):
        return DSSchema(
            keys={f.name: get_pd_dtype(f.dtype) for f in self._fields if f.key},
            values={
                f.name: get_pd_dtype(f.dtype)
                for f in self._fields
                if not f.key and f.name != self._timestamp_field
            },
            timestamp=self._timestamp_field,
            name=f"'[Dataset:{self._name}]'",
        )

    # ------------------- Private Methods ----------------------------------
    def _add_fields_as_attributes(self):
        for field in self._fields:
            setattr(self.__fennel_original_cls__, field.name, field)
            field.dataset = self

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
            if not field.name:
                continue
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
                        f"dataset `{self._name}`. Please set one of the datetime fields to be the timestamp field."
                    )
                timestamp_field_set = True

        if timestamp_field_set:
            return

        # Find a field that has datetime type and set it as timestamp.

        for field in self._fields:
            if field.dtype != datetime.datetime and field.dtype != "datetime":
                continue
            if not timestamp_field_set:
                field.timestamp = True
                timestamp_field_set = True
                self._timestamp_field = field.name
            else:
                raise ValueError(
                    f"Multiple timestamp fields are not "
                    f"supported in dataset `{self._name}`. Please set one of the datetime fields to be the timestamp field."
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
                break
            inputs = getattr(on_demand.func, FENNEL_INPUTS)
            for inp in inputs:
                if not check_timestamp:
                    if inp == datetime.datetime:
                        check_timestamp = True
                        continue
                    raise ValueError(
                        f"on_demand method {on_demand.func.__name__} must take "
                        f"timestamp as first parameter with type Series[datetime]."
                    )
                if inp != key_fields[key_index].dtype:
                    raise ValueError(
                        f"on_demand method {on_demand.func.__name__} must take "
                        f"key fields in the same order as defined in the "
                        f"dataset with the same type, parameter "
                        f"{key_fields[key_index].name} has type"
                        f" {inp} but expected "
                        f" {key_fields[key_index].dtype} "
                    )
                key_index += 1
            on_demand.bound_func = functools.partial(on_demand.func, self)
        return on_demand

    def _get_pipelines(self) -> List[Pipeline]:
        pipelines = []
        dataset_name = self._name
        names = set()
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, PIPELINE_ATTR):
                continue

            pipeline = getattr(method, PIPELINE_ATTR)

            if pipeline.name in names:
                raise ValueError(
                    f"Duplicate pipeline name {pipeline.name} for dataset {dataset_name}."
                )
            names.add(pipeline.name)
            is_terminal = pipeline.set_terminal_node(
                pipeline.func(self, *pipeline.inputs)
            )
            if is_terminal:
                self.is_terminal = is_terminal
            pipelines.append(pipeline)
            pipelines[-1].set_dataset_name(dataset_name)

        self._validate_pipelines(pipelines)

        if len(pipelines) == 1:
            pipelines[0].active = True

        return pipelines

    def _validate_pipelines(self, pipelines: List[Pipeline]):
        exceptions = []
        ds_schema = self.dsschema()

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

    @property
    def version(self):
        return self._version


def sync_validation_for_pipelines(pipelines: List[Pipeline], ds_name: str):
    """
    This validation function contains the checks that are run just before the sync call.
    It should only contain checks that are not possible to run during the registration phase/compilation phase.
    """
    if len(pipelines) <= 1:
        return

    tiers: Set[str] = set()
    for pipeline in pipelines:
        tier = pipeline.tier.tiers
        if tier is None:
            raise ValueError(
                f"Pipeline : `{pipeline.name}` has no tier. If there are more than one Pipelines for a dataset, "
                f"please specify tier for each of them as there can only be one Pipeline for each tier."
            )
        if isinstance(tier, list):
            for tier_item in tier:
                if tier_item in tiers:
                    raise ValueError(
                        f"Pipeline : `{pipeline.name}` mapped to Tier : {tier_item} which has more than one "
                        f"pipeline. Please specify only one."
                    )
                else:
                    tiers.add(tier_item)
        elif isinstance(tier, str):
            if tier in tiers:
                raise ValueError(
                    f"Pipeline : `{pipeline.name}` mapped to Tier : {tier} which has more than one pipeline. "
                    f"Please specify only one."
                )
            else:
                tiers.add(tier)


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
        elif isinstance(obj, Dedup):
            return self.visitDedup(obj)
        elif isinstance(obj, Explode):
            return self.visitExplode(obj)
        elif isinstance(obj, First):
            return self.visitFirst(obj)
        elif isinstance(obj, DropNull):
            return self.visitDropNull(obj)
        elif isinstance(obj, Assign):
            return self.visitAssign(obj)
        elif isinstance(obj, WindowOperator):
            return self.visitWindow(obj)
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

    def visitDedup(self, obj):
        raise NotImplementedError()

    def visitExplode(self, obj):
        raise NotImplementedError()

    def visitFirst(self, obj):
        raise NotImplementedError()

    def visitDropNull(self, obj):
        raise NotImplementedError()

    def visitAssign(self, obj):
        raise NotImplementedError()

    def visitWindow(self, obj):
        raise NotImplementedError()


@dataclass
class DSSchema:
    keys: Dict[str, Type]
    values: Dict[str, Type]
    timestamp: str
    name: str = ""

    def schema(self) -> Dict[str, Type]:
        schema = {**self.keys, **self.values, self.timestamp: datetime.datetime}
        # Convert int -> Int64, float -> Float64 and string -> String
        for k, v in schema.items():
            schema[k] = get_pd_dtype(v)
        return schema

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
            raise Exception(
                f"field `{field}` not found in schema of `{self.name}`"
            )

    def rename_column(self, old_name: str, new_name: str):
        if old_name in self.keys:
            self.keys[new_name] = self.keys.pop(old_name)
        elif old_name in self.values:
            self.values[new_name] = self.values.pop(old_name)
        elif old_name == self.timestamp:
            self.timestamp = new_name
        else:
            raise Exception(
                f"field `{old_name}` not found in schema of `{self.name}`"
            )

    def get_optional_cols(self) -> List[str]:
        return [
            col for col, t in self.schema().items() if fennel_is_optional(t)
        ]

    def drop_null_column(self, name: str):
        if name in self.keys:
            self.keys[name] = fennel_get_optional_inner(self.keys[name])
        elif name in self.values:
            self.values[name] = fennel_get_optional_inner(self.values[name])
        elif name == self.timestamp:
            raise Exception(
                f"cannot drop_null on timestamp field `{name}` of `{self.name}`"
            )
        else:
            raise Exception(
                f"field `{name}` not found in schema of `{self.name}`"
            )

    def append_value_column(self, name: str, type_: Type):
        if name in self.keys:
            raise Exception(
                f"field `{name}` already exists in schema of `{self.name}`"
            )
        elif name in self.values:
            raise Exception(
                f"field `{name}` already exists in schema of `{self.name}`"
            )
        elif name == self.timestamp:
            raise Exception(
                f"cannot append timestamp field `{name}` to `{self.name}`"
            )
        else:
            self.values[name] = type_

    def drop_column(self, name: str):
        if name in self.keys:
            self.keys.pop(name)
        elif name in self.values:
            self.values.pop(name)
        elif name == self.timestamp:
            raise Exception(
                f"cannot drop timestamp field `{name}` from `{self.name}`"
            )
        else:
            raise Exception(
                f"field `{name}` not found in schema of `{self.name}`"
            )

    def update_column(self, name: str, type: Type):
        if name in self.keys:
            self.keys[name] = type
        elif name in self.values:
            self.values[name] = type
        elif name == self.timestamp:
            raise Exception(
                f"cannot assign timestamp field `{name}` from `{self.name}`"
            )
        else:
            self.values[name] = type  # Add to values

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
        vis = super(SchemaValidator, self).visit(obj)
        return vis

    def visitDataset(self, obj) -> DSSchema:
        return obj.dsschema()

    def visitTransform(self, obj) -> DSSchema:
        input_schema = self.visit(obj.node)
        if obj.new_schema is None:
            return input_schema
        else:
            node_name = f"'[Pipeline:{self.pipeline_name}]->transform node'"
            if input_schema.timestamp not in obj.new_schema:
                raise TypeError(
                    f"Timestamp field {input_schema.timestamp} must be "
                    f"present in schema of {node_name}."
                )
            for name, dtype in input_schema.keys.items():
                if name not in obj.new_schema:
                    raise TypeError(
                        f"Key field {name} must be present in schema of "
                        f"{node_name}."
                    )
                if dtype != get_pd_dtype(obj.new_schema[name]):
                    raise TypeError(
                        f"Key field {name} has type {dtype_to_string(dtype)} in "
                        f"input schema "
                        f"of transform but type "
                        f"{dtype_to_string(obj.new_schema[name])} in output "
                        f"schema of {node_name}."
                    )
            inp_keys = input_schema.keys
            return DSSchema(
                keys=inp_keys,
                values={
                    f: get_pd_dtype(dtype)
                    for f, dtype in obj.new_schema.items()
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
        values: Dict[str, Type] = {}
        for agg in obj.aggregates:
            exceptions = agg.validate()
            if exceptions is not None:
                raise ValueError(f"Invalid aggregate `{agg}`: {exceptions}")

            if isinstance(agg, Count):
                if agg.unique:
                    if agg.of is None:
                        raise ValueError(
                            f"Count unique aggregate `{agg}` must have `of` field."
                        )
                    if not is_hashable(input_schema.get_type(agg.of)):
                        raise TypeError(
                            f"Cannot use count unique for field `{agg.of}` of "
                            f"type `{dtype_to_string(dtype)}`, as it is not "  # type: ignore
                            f"hashable"
                        )
                values[agg.into_field] = pd.Int64Dtype
            elif isinstance(agg, Distinct):
                if agg.of is None:
                    raise ValueError(
                        f"Distinct aggregate `{agg}` must have `of` field."
                    )
                dtype = input_schema.get_type(agg.of)
                if not is_hashable(input_schema.get_type(agg.of)):
                    raise TypeError(
                        f"Cannot use distinct for field `{agg.of}` of "
                        f"type `{dtype_to_string(dtype)}`, as it is not hashable"
                        # type: ignore
                    )
                list_type = get_python_type_from_pd(dtype)
                values[agg.into_field] = List[list_type]  # type: ignore
            elif isinstance(agg, Sum):
                dtype = input_schema.get_type(agg.of)
                if get_primitive_dtype(dtype) not in primitive_numeric_types:
                    raise TypeError(
                        f"Cannot sum field `{agg.of}` of type `{dtype_to_string(dtype)}`"
                    )
                values[agg.into_field] = dtype  # type: ignore
            elif isinstance(agg, Average):
                dtype = input_schema.get_type(agg.of)
                if get_primitive_dtype(dtype) not in primitive_numeric_types:
                    raise TypeError(
                        f"Cannot take average of field `{agg.of}` of type `{dtype_to_string(dtype)}`"
                    )
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            elif isinstance(agg, LastK):
                dtype = input_schema.get_type(agg.of)
                list_type = get_python_type_from_pd(dtype)
                values[agg.into_field] = List[list_type]  # type: ignore
            elif isinstance(agg, Min):
                dtype = input_schema.get_type(agg.of)
                if get_primitive_dtype(dtype) not in primitive_numeric_types:
                    raise TypeError(
                        f"invalid min: type of field `{agg.of}` is not int or float"
                    )
                if get_primitive_dtype(dtype) == pd.Int64Dtype and (
                    int(agg.default) != agg.default
                ):
                    raise TypeError(
                        f"invalid min: default value `{agg.default}` not of type `int`"
                    )
                values[agg.into_field] = dtype  # type: ignore
            elif isinstance(agg, Max):
                dtype = input_schema.get_type(agg.of)
                if get_primitive_dtype(dtype) not in primitive_numeric_types:
                    raise TypeError(
                        f"invalid max: type of field `{agg.of}` is not int or float"
                    )
                if get_primitive_dtype(dtype) == pd.Int64Dtype and (
                    int(agg.default) != agg.default
                ):
                    raise TypeError(
                        f"invalid max: default value `{agg.default}` not of type `int`"
                    )
                values[agg.into_field] = dtype  # type: ignore
            elif isinstance(agg, Stddev):
                dtype = input_schema.get_type(agg.of)
                if get_primitive_dtype(dtype) not in primitive_numeric_types:
                    raise TypeError(
                        f"Cannot get standard deviation of field {agg.of} of type {dtype_to_string(dtype)}"
                    )
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            else:
                raise TypeError(f"Unknown aggregate type {type(agg)}")
        return DSSchema(
            keys=keys,
            values=values,  # type: ignore
            timestamp=input_schema.timestamp,
            name=f"'[Pipeline:{self.pipeline_name}]->aggregate node'",
        )

    def visitJoin(self, obj) -> DSSchema:
        left_schema = self.visit(obj.node)
        right_schema = self.visit(obj.dataset)
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->join node'"

        def validate_join_bounds(within: Tuple[Duration, Duration]):
            if len(within) != 2:
                raise ValueError(
                    f"Invalid within clause: `{within}` in `{output_schema_name}`. "
                    "Should be a tuple of 2 values. e.g. ('forever', '0s')"
                )
            # Neither of them can be None
            if within[0] is None or within[1] is None:
                raise ValueError(
                    f"Invalid within clause: `{within}` in `{output_schema_name}`."
                    "Neither bounds can be None"
                )
            if within[1] == "forever":
                raise ValueError(
                    f"Invalid within clause: `{within}` in `{output_schema_name}`"
                    "Upper bound cannot be `forever`"
                )

        def is_subset(subset: List[str], superset: List[str]) -> bool:
            return set(subset).issubset(set(superset))

        validate_join_bounds(obj.within)

        if obj.on is not None and len(obj.on) > 0:
            # obj.on should be the key of the right dataset
            if set(obj.on) != set(right_schema.keys.keys()):
                raise ValueError(
                    f"on field `{obj.on}` are not the key fields of the right "
                    f"dataset `{obj.dataset._name}` for `{output_schema_name}`."
                )
            # Check the schemas of the keys
            for key in obj.on:
                if fennel_is_optional(left_schema.get_type(key)):
                    raise TypeError(
                        f"Fields used in a join operator must not be optional in left schema, "
                        f"found `{key}` of type `{dtype_to_string(left_schema.get_type(key))}` "
                        f"in `{output_schema_name}`"
                    )
                if left_schema.get_type(key) != right_schema.get_type(key):
                    raise TypeError(
                        f"Key field `{key}` has type `{dtype_to_string(left_schema.get_type(key))}` "
                        f"in left schema but type "
                        f"`{dtype_to_string(right_schema.get_type(key))}` in right schema for `{output_schema_name}`"
                    )
            # Check that none of the other fields collide

        else:
            #  obj.right_on should be the keys of the right dataset
            if set(obj.right_on) != set(right_schema.keys.keys()):
                raise ValueError(
                    f"right_on field `{obj.right_on}` are not the key fields of "
                    f"the right dataset `{obj.dataset._name}` for `{output_schema_name}`."
                )
            #  obj.left_on should be a subset of the schema of the left dataset
            if not is_subset(obj.left_on, list(left_schema.fields())):
                raise ValueError(
                    f"left_on field `{obj.left_on}` are not the key fields of "
                    f"the left dataset `{obj.node.schema()}` for `{output_schema_name}`."
                )
            # Check the schemas of the keys
            for lkey, rkey in zip(obj.left_on, obj.right_on):
                if fennel_is_optional(left_schema.get_type(lkey)):
                    raise TypeError(
                        f"Fields used in a join operator must not be optional "
                        f"in left schema, found `{lkey}` of type "
                        f"`{dtype_to_string(left_schema.get_type(lkey))}` "
                        f"in `{output_schema_name}`"
                    )
                if left_schema.get_type(lkey) != right_schema.get_type(rkey):
                    raise TypeError(
                        f"Key field `{lkey}` has type"
                        f" `{dtype_to_string(left_schema.get_type(lkey))}` "
                        f"in left schema but, key field `{rkey}` has type "
                        f"`{dtype_to_string(right_schema.get_type(rkey))}` in "
                        f"right schema for `{output_schema_name}`"
                    )

        if obj.how not in ["inner", "left"]:
            raise ValueError(
                f'"how" in {output_schema_name} must be either "inner" or "left" for `{output_schema_name}`'
            )

        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitUnion(self, obj) -> DSSchema:
        if len(obj.nodes) == 0:
            raise ValueError("Union must have at least one node.")
        schema = self.visit(obj.nodes[0])
        # If it is a keyed dataset throw an error. Union over keyed
        # datasets, is not currently supported.
        if len(schema.keys) > 0:
            raise TypeError(
                f"Union over keyed datasets is currently not supported. Found dataset with keys `{schema.keys}` in pipeline `{self.pipeline_name}`"
            )

        index = 1
        exceptions = []
        for node in obj.nodes[1:]:
            node_schema = self.visit(node)
            err = node_schema.matches(
                schema,
                "Union node index 0",
                f"Union node index {index} of pipeline {self.pipeline_name}",
            )
            index += 1
            exceptions.extend(err)
        if len(exceptions) > 0:
            raise ValueError(f"Union node schemas do not match: {exceptions}")
        schema.name = f"'[Pipeline:{self.pipeline_name}]->union node'"
        return schema

    def visitRename(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        input_schema.name = f"'[Pipeline:{self.pipeline_name}]->rename node'"
        if obj.column_mapping is None or len(obj.column_mapping) == 0:
            raise ValueError(
                f"invalid rename {input_schema.name}: must have at least one column to rename"
            )
        for old, new in obj.column_mapping.items():
            if old not in input_schema.fields():
                raise ValueError(
                    f"Field `{old}` does not exist in schema of "
                    f"rename node {input_schema.name}."
                )
            if new in input_schema.fields():
                raise ValueError(
                    f"Field `{new}` already exists in schema of "
                    f"rename node {input_schema.name}."
                )
            input_schema.rename_column(old, new)
        return input_schema

    def visitAssign(self, obj) -> DSSchema:
        input_schema: DSSchema = copy.deepcopy(self.visit(obj.node))
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->assign node'"
        if obj.column is None or len(obj.column) == 0:
            raise ValueError(
                f"invalid assign - {output_schema_name} must specify a column to assign"
            )
        val_fields = input_schema.values.keys()
        if (
            obj.column in input_schema.keys
            or obj.column == input_schema.timestamp
        ):
            raise ValueError(
                f"Field `{obj.column}` is a key or timestamp field in schema of "
                f"assign node input {input_schema.name}. Value fields are: {list(val_fields)}"
            )
        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitDrop(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->drop node'"
        if obj.columns is None or len(obj.columns) == 0:
            raise ValueError(
                f"invalid drop - {output_schema_name} must have at least one column to drop"
            )
        val_fields = input_schema.values.keys()
        for field in obj.columns:
            if field not in val_fields:
                raise ValueError(
                    f"Field `{field}` is a key or timestamp field in schema of "
                    f"{obj.name} node input {input_schema.name}. Value fields are: {list(val_fields)}"
                )
        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitDropNull(self, obj):
        input_schema = copy.deepcopy(self.visit(obj.node))
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->dropnull node'"
        if obj.columns is None or len(obj.columns) == 0:
            raise ValueError(
                f"invalid dropnull - `{output_schema_name}` must have at least one column"
            )
        for field in obj.columns:
            if (
                field not in input_schema.schema()
                or field == input_schema.timestamp
            ):
                raise ValueError(
                    f"invalid dropnull column `{field}` not present in `{input_schema.name}`"
                )
            if not fennel_is_optional(input_schema.get_type(field)):
                raise ValueError(
                    f"invalid dropnull `{field}` has type `{dtype_to_string(input_schema.get_type(field))}` expected Optional type"
                )
        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitDedup(self, obj) -> DSSchema:
        input_schema = self.visit(obj.node)
        output_schema_name = (
            f"'[Pipeline:{self.pipeline_name}]->drop_duplicates node'"
        )
        # Input schema should not have key columns.
        if len(input_schema.keys) > 0:
            raise ValueError(
                f"invalid dedup: input schema {input_schema.name} has key columns"
            )
        if len(obj.by) == 0:
            raise ValueError(
                "invalid dedup: must have at least one column to deduplicate by"
            )
        for f in obj.by:
            if f not in input_schema.fields():
                raise ValueError(
                    f"invalid dedup: field `{f}` not present in input schema {input_schema.name}"
                )
        if input_schema.timestamp in obj.by:
            raise ValueError(
                f"invalid dedup: cannot dedup on timestamp field `{obj.by}` of input schema {input_schema.name}"
            )

        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitExplode(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        # If it is a keyed dataset throw an error. Explode over keyed
        # datasets, is not defined, since for a keyed dataset, there is only one value for each key.
        if len(input_schema.keys) > 0:
            raise TypeError(
                f"Explode over keyed datasets is not defined. Found dataset with keys `{input_schema.keys}` in pipeline `{self.pipeline_name}`"
            )
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->explode node'"
        if obj.columns is None or len(obj.columns) == 0:
            raise ValueError(
                f"invalid explode {output_schema_name}: must have at least one column to explode"
            )
        # Can only explode value columns
        schema = input_schema.schema()
        val_fields = input_schema.values.keys()
        for field in obj.columns:
            # 'field' must be present in input schema.
            if field not in input_schema.fields():
                raise ValueError(
                    f"Column `{field}` in explode not present in input {input_schema.name}: {input_schema.fields()}"
                )
            # 'field' must be a value column.
            if field not in val_fields:
                raise ValueError(
                    f"Field `{field}` is a key or timestamp field in schema of "
                    f"explode node input {input_schema.name}. Value fields are: {list(val_fields)}"
                )
            # Type of 'c' must be List.
            raw_type = getattr(schema[field], "__origin__", schema[field])
            if raw_type != list:
                raise ValueError(
                    f"Column `{field}` in explode is not of type List"
                )
        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitFirst(self, obj) -> DSSchema:
        output_schema = copy.deepcopy(obj.dsschema())
        if len(output_schema.keys) == 0:
            raise ValueError(
                f"'group_by' before 'first' in {self.pipeline_name} must specify at least one key"
            )
        output_schema.name = f"'[Pipeline:{self.pipeline_name}]->first node'"
        return output_schema

    def visitWindow(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))

        output_schema = copy.deepcopy(obj.dsschema())
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->window node'"

        if obj.field in input_schema.keys.keys():
            raise ValueError(
                f"Window field name `{obj.field}` in `{output_schema_name}` must be "
                f"different from keyed fields in `{input_schema.name}`"
            )
        if obj.field in input_schema.values.keys():
            raise ValueError(
                f"Window field name `{obj.field}` in `{output_schema_name}` must be "
                f"different from non keyed fields in `{input_schema.name}`"
            )
        if len(output_schema.keys) == 0:
            raise ValueError(
                f"'group_by' before 'window' in `{self.pipeline_name}` must specify at least one key"
            )
        output_schema.name = output_schema_name
        return output_schema
