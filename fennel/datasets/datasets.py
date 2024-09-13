from __future__ import annotations
import warnings
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
)

import pandas as pd
from typing_extensions import Literal


from fennel.dtypes import Continuous
from fennel.expr.expr import TypedExpr
from fennel.expr.visitor import ExprPrinter
import fennel.gen.index_pb2 as index_proto
from fennel._vendor.pydantic import BaseModel  # type: ignore
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
    Quantile,
    ExpDecaySum,
)
from fennel.dtypes.dtypes import (
    get_fennel_struct,
    Window,
    Decimal,
    _Decimal,
    Hopping,
    Session,
    Tumbling,
)
from fennel.expr import Expr
from fennel.gen import schema_pb2 as schema_proto
from fennel.internal_lib.duration import (
    Duration,
    duration_to_timedelta,
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
    get_datatype,
)
from fennel.internal_lib.utils import (
    dtype_to_string,
    get_origin,
    parse_datetime,
)
from fennel.lib.expectations import Expectations, GE_ATTR_FUNC
from fennel.lib.includes import EnvSelector
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
DEFAULT_INDEX = False
RESERVED_FIELD_NAMES = [
    "cls",
    "self",
    "fields",
    "key_fields",
    "on_demand",
    "timestamp_field",
    "fqn",
]

INDEX_FIELD = "__fennel_index__"
DEFAULT_INDEX_TYPE = "primary"
DEFAULT_INDEX_OFFLINE = "forever"
DEFAULT_INDEX_ONLINE = True
WINDOW_FIELD_NAME = "window"

primitive_numeric_types = [int, float, pd.Int64Dtype, pd.Float64Dtype, Decimal]

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
    erase_key: bool

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

    def desc(self) -> str:
        return get_meta_attr(self, "description") or ""


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
            erase_key=False,
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
    erase_key: bool = False,
) -> T:  # type: ignore
    if erase_key and not key:
        raise ValueError("Non key field cannot be an erase key field.")
    if timestamp and key:
        raise ValueError("Timestamp field cannot be a key.")
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
            erase_key=erase_key,
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

    def filter(self, func: Callable | Expr) -> _Node:
        return Filter(self, func)

    def assign(self, *args, **kwargs) -> _Node:
        """
        Assign is an overloaded operator that can be used in several ways:
        1. Assigning a new column with a lambda function:
            >>> ds.assign("new_column", int, lambda x: x.old_column + 1)
        2. Assigning one or more columns:
            >>> ds.assign(
            ...     new_column1=(col("old_column1") + 1).astype(int),
            ...     new_column2=(col("old_column2") + 2).astype(int),
            ... )

        """
        # Check if the user is using the first syntax
        # Skip args[1] check because type check can be flaky
        # and args[0] and args[2] are always strings and callable
        if (
            len(args) == 3
            and isinstance(args[0], str)
            and isinstance(args[2], Callable)  # type: ignore
        ):
            return Assign(self, args[0], args[1], args[2])

        # If there are 3 kwargs for name, dtype, func throw an error
        if (
            len(kwargs) == 3
            and "name" in kwargs
            and "dtype" in kwargs
            and "func" in kwargs
        ):
            raise TypeError(
                "assign operator can either take 3 args for name, dtype, func or kwargs for expressions, not both"
            )

        # Check if the user is using the second syntax
        if len(args) == 0 and len(kwargs) > 0:
            return Assign.from_expressions(self, **kwargs)

        raise Exception(
            "Invalid arguments to assign, please see the documentation for more information."
        )

    def groupby(
        self, *args, window: Optional[Union[Hopping, Session, Tumbling]] = None
    ) -> GroupBy:
        return GroupBy(self, *args, window=window)

    def join(
        self,
        other: Dataset,
        how: Literal["inner", "left"],
        on: Optional[List[str]] = None,
        left_on: Optional[List[str]] = None,
        right_on: Optional[List[str]] = None,
        within: Tuple[Duration, Duration] = ("forever", "0s"),
        fields: Optional[List[str]] = None,
    ) -> Join:
        if not isinstance(other, Dataset) and isinstance(other, _Node):
            raise ValueError(
                "Cannot join with an intermediate dataset, i.e something defined inside a pipeline."
                " Only joining against keyed datasets is permitted."
            )
        if not isinstance(other, _Node):
            raise TypeError("Cannot join with a non-dataset object")
        return Join(self, other, within, how, on, left_on, right_on, fields)

    def rename(self, columns: Dict[str, str]) -> _Node:
        return Rename(self, columns)

    def drop(self, *args, columns: Optional[List[str]] = None) -> _Node:
        drop_cols = _Node.__get_list_args(*args, columns=columns)
        return Drop(self, drop_cols)

    def dropnull(self, *args, columns: Optional[List[str]] = None) -> _Node:
        if len(args) == 0 and columns is None:  # dropnull with no get_args
            cols = self.dsschema().get_optional_cols()
        else:
            cols = _Node.__get_list_args(
                *args, columns=columns, name="dropnull"
            )
        return DropNull(self, cols)

    def select(self, *args, columns: Optional[List[str]] = None) -> _Node:
        cols = _Node.__get_list_args(*args, columns=columns, name="select")
        ts = self.dsschema().timestamp
        if ts not in cols:
            cols.append(ts)
        if len(cols) == len(self.dsschema().fields()):
            warnings.warn(
                f"Selecting all columns {cols} in the dataset, consider skipping the select operator."
            )
            return self

        drop_cols = list(
            filter(lambda c: c not in cols, self.dsschema().fields())
        )
        return Select(self, cols, drop_cols)

    def dedup(self, *args, by: Optional[List[str]] = None) -> _Node:
        # If 'by' is not provided, dedup by all value fields.
        # Note: we don't use key fields because dedup cannot be applied on keyed datasets.
        collist: List[str] = []
        if len(args) == 0 and by is None:
            collist = list(self.dsschema().values.keys())
        elif len(args) > 0 and by is None:
            collist = list(args)  # type: ignore
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
        columns = _Node.__get_list_args(*args, columns=columns, name="explode")
        return Explode(self, columns)

    def changelog(self, delete_column: str) -> _Node:
        return Changelog(self, delete_column)

    def isignature(self):
        raise NotImplementedError

    def signature(self):
        raise NotImplementedError

    def schema(self):
        return copy.deepcopy(self.dsschema().schema())

    def dsschema(self):
        return copy.deepcopy(self.dsschema())

    def num_out_edges(self) -> int:
        return len(self.out_edges)

    @classmethod
    def __get_list_args(
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


def get_all_operators() -> List[str]:
    """
    Get all the operators a user can use on a Dataset in a pipeline.
    Returns:
        List[str] -> List operator names in string.
    """
    class_dict = dict(_Node.__dict__)
    output = []
    for member in class_dict:
        if "__" not in member and member not in [
            "signature",
            "isignature",
            "schema",
            "dsschema",
            "num_out_edges",
        ]:
            output.append(member)
    return output


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


class UDFType(str, Enum):
    expr = "expr"
    python = "python"


class Assign(_Node):
    def __init__(
        self,
        node: _Node,
        column: Optional[str],
        output_type: Optional[Type],
        func: Optional[Callable],
        **kwargs,
    ):
        super().__init__()
        self.node = node
        self.node.out_edges.append(self)
        self.func = func
        self.column = column
        self.output_type = output_type
        self.output_expressions = {}
        self.assign_type = UDFType.python
        # Check that either node, column and output_type are all None or all not None
        if (
            node is None
            and column is None
            and output_type is None
            and len(kwargs) == 0
        ) or (
            node is not None
            and column is not None
            and output_type is not None
            and len(kwargs) > 0
        ):
            raise ValueError(
                "Assign expects either to use the arguments `node`, `column` and `output_type` or use the keyword arguments for expressions"
            )
        # Map of column names to expressions
        if len(kwargs) > 0:
            for k, v in kwargs.items():
                self.output_expressions[k] = v
            self.assign_type = UDFType.expr

    @classmethod
    def from_expressions(cls, self, **kwargs):
        for k, v in kwargs.items():
            if isinstance(v, Expr):
                raise TypeError(
                    f"type not specified for column {k} in assign operator, please use .astype(...) to specify the type"
                )

            if not isinstance(v, TypedExpr):
                raise ValueError(
                    "Assign.from_expressions expects all values to be of type Expr",
                    f"found `{type(v)}` for column `{k}`",
                )
        return Assign(self, None, None, None, **kwargs)

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(
                self.node._name,
                self.func,
                self.column,
                (
                    self.output_type.__name__
                    if self.output_type is not None
                    else None
                ),
            )
        if self.assign_type == UDFType.python:
            return fhash(
                self.node.signature(),
                self.func,
                self.column,
                self.output_type.__name__,
            )
        else:
            return fhash(
                self.node.signature(),
                self.output_expressions,
            )

    def dsschema(self):
        input_schema = self.node.dsschema()
        if self.assign_type == UDFType.python:
            input_schema.update_column(
                self.column, get_pd_dtype(self.output_type)
            )
        else:
            for col, expr in self.output_expressions.items():
                input_schema.update_column(col, get_pd_dtype(expr.dtype))
        return input_schema


class Filter(_Node):
    def __init__(self, node: _Node, fiter_fn: Callable | Expr):
        super().__init__()
        self.node = node
        self.node.out_edges.append(self)
        self.filter_expr = None
        self.func = None  # noqa: E731
        if isinstance(fiter_fn, Callable):  # type: ignore
            self.filter_type = UDFType.python
            self.func = fiter_fn
        elif isinstance(fiter_fn, Expr):  # type: ignore
            self.filter_type = UDFType.expr
            self.filter_expr = fiter_fn
        else:
            raise ValueError(
                f"Filter expects either a lambda function or an expression object, found {type(func)}"  # type: ignore
            )

    def signature(self):
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.func)
        return fhash(self.node.signature(), self.func)

    def dsschema(self):
        return self.node.dsschema()


class Changelog(_Node):
    def __init__(self, node: _Node, delete_column: str):
        super().__init__()
        self.node = node
        self.delete_column = delete_column
        self.node.out_edges.append(self)

    def signature(self):
        return fhash(self.node.signature(), self.delete_column)

    def dsschema(self):
        input_schema = self.node.dsschema()
        # Remove all keys from the schema and make then values
        val_fields = copy.deepcopy(input_schema.keys)
        values = input_schema.values
        val_fields.update(values)
        val_fields.update({self.delete_column: pd.BooleanDtype})
        return DSSchema(
            keys={},
            values=val_fields,
            timestamp=input_schema.timestamp,
        )


class EmitStrategy(str, Enum):
    Final = "final"
    Eager = "eager"

    @classmethod
    def _missing_(cls, value):
        valid_types = [m.value for m in cls]
        raise ValueError(
            f"`{value}` is not a valid 'emit_strategy' in 'aggregate' operator. "
            f"'emit' in aggregation operator must be one of {valid_types}"
        )


class Aggregate(_Node):
    def __init__(
        self,
        node: _Node,
        keys: List[str],
        aggregates: List[AggregateType],
        along: Optional[str],
        emit_strategy: EmitStrategy,
        window_field: Optional[Union[Hopping, Session, Tumbling]] = None,
    ):
        super().__init__()
        if len(keys) == 0:
            raise ValueError("Must specify at least one key")
        self.keys_without_window = keys.copy()
        self.keys = keys.copy()
        if window_field:
            self.keys.append(WINDOW_FIELD_NAME)
        self.aggregates = aggregates
        self.node = node
        self.node.out_edges.append(self)
        self.along = along
        self.emit_strategy = emit_strategy
        self.window_field = window_field

    def signature(self):
        agg_signature = fhash([agg.signature() for agg in self.aggregates])
        if isinstance(self.node, Dataset):
            return fhash(self.node._name, self.keys, agg_signature)
        return fhash(self.node.signature(), self.keys, agg_signature)

    def dsschema(self):
        input_schema = self.node.dsschema()
        keys = {f: input_schema.get_type(f) for f in self.keys_without_window}
        if self.window_field is not None:
            keys[WINDOW_FIELD_NAME] = Window
        values = {}
        for agg in self.aggregates:
            if isinstance(agg, Count):
                values[agg.into_field] = pd.Int64Dtype  # type: ignore
            elif isinstance(agg, Sum):
                dtype = input_schema.get_type(agg.of)
                primitive_dtype = get_primitive_dtype(dtype)
                if primitive_dtype not in primitive_numeric_types:
                    raise TypeError(
                        f"Cannot sum field {agg.of} of type {dtype_to_string(dtype)}"
                    )
                if primitive_dtype == Decimal:
                    values[agg.into_field] = dtype  # type: ignore
                else:
                    values[agg.into_field] = primitive_dtype  # type: ignore
            elif isinstance(agg, Min) or isinstance(agg, Max):
                dtype = input_schema.get_type(agg.of)
                primitive_dtype = get_primitive_dtype(dtype)
                if primitive_dtype == Decimal:
                    values[agg.into_field] = dtype  # type: ignore
                else:
                    values[agg.into_field] = primitive_dtype  # type: ignore
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
            elif isinstance(agg, Quantile):
                if agg.default is None:
                    values[agg.into_field] = Optional[pd.Float64Dtype]  # type: ignore
                else:
                    values[agg.into_field] = pd.Float64Dtype  # type: ignore
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            elif isinstance(agg, ExpDecaySum):
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            else:
                raise TypeError(f"Unknown aggregate type {type(agg)}")
        return DSSchema(
            keys=keys,
            values=values,  # type: ignore
            timestamp=input_schema.timestamp,
        )


AGGREGATE_RESERVED_FIELDS = ["emit", "along", "using", "config"]


class GroupBy:
    def __init__(
        self,
        node: _Node,
        *args,
        window: Optional[Union[Hopping, Session, Tumbling]] = None,
    ):
        super().__init__()
        self.keys = args
        self.node = node
        self.node.out_edges.append(self)
        if window is not None:
            if not isinstance(window, (Hopping, Session, Tumbling)):
                raise TypeError(
                    "Type of 'window' param can only be either Hopping, Session or Tumbling."
                )
            if isinstance(window, Hopping) and window.duration == "forever":
                raise TypeError(
                    "Specifying forever hopping window in groupby 'window' param is not allowed."
                )
        self.window_field = window

    def aggregate(
        self, *args, along: Optional[str] = None, emit: str = "eager", **kwargs
    ) -> _Node:
        if isinstance(emit, AggregateType):
            raise ValueError(
                "`emit` is a reserved kwarg for aggregate operator and can not be used "
                "for an aggregation column"
            )
        if along is not None and isinstance(along, AggregateType):
            raise ValueError(
                "`along` is a reserved kwarg for aggregate operator and can not be used "
                "for an aggregation column"
            )
        if self.window_field is None and len(args) == 0 and len(kwargs) == 0:
            raise TypeError(
                "aggregate operator expects at least one aggregation operation"
            )
        if len(args) == 1 and isinstance(args[0], list):
            aggregates = args[0]
        else:
            aggregates = list(args)
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore

        if along is not None and not isinstance(along, str):
            raise ValueError(
                f"`along` kwarg in the aggregate operator must be string or None, found: {along}"
            )

        # Handle window
        if self.window_field is not None:
            if along is not None:
                raise ValueError(
                    "'along' param can only be used with non-discrete windows (Continuous/Forever) and lazy emit strategy."
                )
            if len(self.keys) == 0:
                raise ValueError(
                    "There should be at least one key in 'groupby' to use 'window'"
                )

        # Adding aggregate to aggregates list from kwargs
        for key, value in kwargs.items():
            if not isinstance(value, AggregateType):
                raise ValueError(f"Invalid aggregate type for field: {key}")
            value.into_field = key
            aggregates.append(value)

        # Check if any aggregate has into_field as ""
        for aggregate in aggregates:
            if aggregate.into_field == "":
                raise ValueError(
                    "Specify the output field name for the aggregate using 'into_field' "
                    "parameter or through named arguments."
                )
            if isinstance(aggregate, ExpDecaySum) and emit == "final":
                raise ValueError(
                    "ExpDecaySum aggregation does not support emit='final'."
                )

        if len(aggregates) == 0 and self.window_field is None:
            raise ValueError(
                "If there are zero aggregates then please either specify 'window' param in groupby or "
                "use some other operator like 'first' or 'last' instead of 'aggregate'."
            )

        return Aggregate(
            self.node,
            list(self.keys),
            aggregates,
            along,
            EmitStrategy(emit),
            self.window_field,
        )

    def first(self) -> _Node:
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        if self.window_field is not None:
            raise ValueError(
                "Only 'aggregate' method is allowed after 'groupby' when you have defined a window."
            )
        return First(self.node, list(self.keys))  # type: ignore

    def latest(self) -> _Node:
        if len(self.keys) == 1 and isinstance(self.keys[0], list):
            self.keys = self.keys[0]  # type: ignore
        if self.window_field is not None:
            raise ValueError(
                "Only 'aggregate' method is allowed after 'groupby' when you have defined a window."
            )
        return Latest(self.node, list(self.keys))  # type: ignore

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


class Latest(_Node):
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
        fields: Optional[List[str]] = None,
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
        self.fields = fields
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
                self.fields,
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
            self.fields,
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

        right_ts = self.dataset.dsschema().timestamp

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

        # If fields is set, check that it contains elements from right schema values and timestamp only
        if self.fields is not None and len(self.fields) > 0:
            allowed_col_names = [x for x in right_value_schema.keys()] + [
                right_ts
            ]
            for col_name in self.fields:
                if col_name not in allowed_col_names:
                    raise ValueError(
                        f"fields member `{col_name}` not present in allowed fields {allowed_col_names} of right input "
                        f"{self.dataset.dsschema().name}"
                    )

        # Add right value columns to left schema. Check for column name collisions. Filter keys present in fields.
        joined_dsschema = copy.deepcopy(left_dsschema)
        for col, dtype in right_value_schema.items():
            if col in left_schema:
                raise ValueError(
                    f"Column name collision. `{col}` already exists in schema of left input {left_dsschema.name}, while joining with {self.dataset.dsschema().name}"
                )
            if (
                self.fields is not None
                and len(self.fields) > 0
                and col not in self.fields
            ):
                continue
            joined_dsschema.append_value_column(col, dtype)

        # Add timestamp column if present in fields
        if self.fields is not None and right_ts in self.fields:
            if self.how == "left":
                joined_dsschema.append_value_column(
                    right_ts, Optional[datetime.datetime]
                )
            else:
                joined_dsschema.append_value_column(right_ts, datetime.datetime)

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
            input_schema.drop_column(field)
        return input_schema


class Select(_Node):
    def __init__(
        self, node: _Node, columns: List[str], drop_columns: List[str]
    ):
        super().__init__()
        self.node = node
        self.columns = columns
        self.drop_columns = drop_columns
        self.node.out_edges.append(self)

    def signature(self):
        # Doing this to make signature same as previous one.
        return fhash(self.node.signature(), self.drop_columns)

    def dsschema(self):
        input_schema = copy.deepcopy(self.node.dsschema())
        input_schema.select_columns(self.columns)
        return input_schema


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


@dataclass
class Summary:
    field: str
    dtype: Type
    summarize_func: Callable


# ---------------------------------------------------------------------
# dataset & pipeline decorators
# ---------------------------------------------------------------------


@overload
def dataset(  # noqa: E704
    *,
    version: Optional[int] = DEFAULT_VERSION,
    history: Optional[Duration] = DEFAULT_RETENTION,
    index: Optional[bool] = DEFAULT_INDEX,
    offline: Optional[bool] = None,
    online: Optional[bool] = None,
) -> Callable[[Type[T]], Dataset]: ...


@overload
def dataset(cls: Type[T]) -> Dataset: ...  # noqa: E704


def dataset(
    cls: Optional[Type[T]] = None,
    version: Optional[int] = DEFAULT_VERSION,
    history: Optional[Duration] = DEFAULT_RETENTION,
    index: Optional[bool] = DEFAULT_INDEX,
    offline: Optional[bool] = None,
    online: Optional[bool] = None,
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
    index : bool ( Optional )
        Whether we want to server the dataset for both online and offline.
    offline : bool ( Optional )
        If we want to turn off or turn on offline index for serving query_offline.
    online : bool ( Optional )
        If we want to turn off or turn on online index for serving query_offline.
    """

    try:
        if len(inspect.stack()) > 2:
            file_name = inspect.stack()[1].filename
        else:
            file_name = ""
    except Exception:
        file_name = ""

    # Getting specs for index
    index_obj = None
    if index:
        if offline or online:
            raise ValueError(
                "When 'index' is set as True then 'offline' or 'online' params can only be set as False."
            )
        # handling @dataset(index=True) choosing both index
        elif offline is None and online is None:
            index_obj = Index(
                type=IndexType.primary,
                online=True,
                offline=IndexDuration.forever,
            )
        # handling @dataset(index=True, offline=False, online=False) choosing none
        elif (
            not offline
            and not online
            and offline is not None
            and online is not None
        ):
            index_obj = Index(
                type=IndexType.primary,
                online=False,
                offline=IndexDuration.none,
            )
        # handling @dataset(index=True, offline=False) choosing only offline index
        elif offline is not None and not offline:
            index_obj = Index(
                type=IndexType.primary, online=True, offline=IndexDuration.none
            )
        # handling @dataset(index=True, online=False) choosing only online index
        elif online is not None and not online:
            index_obj = Index(
                type=IndexType.primary,
                online=False,
                offline=IndexDuration.forever,
            )
    elif not index:
        if offline and online:
            raise ValueError(
                "When 'index' is set as False then only either one of 'offline' or 'online' can be set as True."
            )
        elif offline:
            index_obj = Index(
                type=IndexType.primary,
                online=False,
                offline=IndexDuration.forever,
            )
        elif online:
            index_obj = Index(
                type=IndexType.primary,
                online=True,
                offline=IndexDuration.none,
            )

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
            if not pd.api.types.is_datetime64_any_dtype(ts.dtype):
                raise ValueError(
                    f"Lookup for dataset `{cls_name}` expects a series of timestamps, found {ts.dtype}"
                )
            # Parse the timestamp
            ts = ts.apply(lambda x: parse_datetime(x))
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
            return res, found

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
        index: Optional[Index],
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

        if isinstance(index, Index):
            if len(key_fields) < 1:
                raise ValueError(
                    f"Index is only applicable for datasets with keyed fields. "
                    f"Found zero key fields for dataset : `{dataset_cls.__name__}`."
                )
            else:
                setattr(dataset_cls, INDEX_FIELD, index)

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
        return _create_dataset(c, version, cast(Duration, history), index_obj)  # type: ignore

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
    env: Optional[Union[str, List[str]]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


@overload
def pipeline(  # noqa: E704
    pipeline_func: Callable,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]: ...


def pipeline(
    pipeline_func: Callable = None,
    env: Optional[Union[str, List[str]]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def _create_pipeline(
        pipeline_func: Callable,
        env: Optional[Union[str, List[str]]] = None,
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
                env=env,
            ),
        )
        return pipeline_func

    def wrap(pipeline_func: Callable) -> Callable:
        return _create_pipeline(pipeline_func, env)  # type: ignore

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
    env: EnvSelector

    def __init__(
        self,
        inputs: List[Dataset],
        func: Callable,
        env: Optional[Union[str, List[str]]] = None,
    ):
        self.inputs = inputs
        self.func = func  # type: ignore
        self.name = func.__name__
        self.env = EnvSelector(env)
        self.terminal_node: _Node

    # Validate the schema of all intermediate nodes
    # and return the schema of the terminal node and whether
    # the pipeline is terminal or not.
    def get_terminal_schema(self) -> DSSchema:
        schema_validator = SchemaValidator()
        return schema_validator.validate(self)

    def signature(self):
        return f"{self._dataset_name}.{self._root}"

    def is_terminal_node(self, node: _Node) -> bool:
        if node is None:
            raise Exception(f"Pipeline {self.name} cannot return None.")
        self.terminal_node = node
        has_continuous_window = False
        if isinstance(node, Aggregate):
            # If any of the aggregates are exponential decay, then it is a terminal node
            if any(isinstance(agg, ExpDecaySum) for agg in node.aggregates):
                return True
            has_continuous_window = any(
                isinstance(agg.window, Continuous) for agg in node.aggregates
            )

        return isinstance(node, Aggregate) and has_continuous_window

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
    _erase_key_fields: List[str]
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
        self._set_erase_key_fields()
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
                if field.dtype == datetime.date:
                    raise ValueError(
                        f"'date' dtype fields cannot be marked as timestamp field. Found field : "
                        f"`{field.name}` of dtype :  `{field.dtype}` in dataset `{self._name}`"
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

    def _set_erase_key_fields(self):
        erase_key_fields = []
        for field in self._fields:
            if field.erase_key:
                erase_key_fields.append(field.name)
        self._erase_key_fields = erase_key_fields

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
            is_terminal = pipeline.is_terminal_node(
                pipeline.func(self, *pipeline.inputs)
            )
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

        # Check that the dataset does not have a terminal aggregate node with Continuous windows
        if self.is_terminal:
            raise ValueError(
                f"Dataset {self._name} has a terminal aggregate node with Continuous windows, we currently dont support expectations on continuous windows."
                "This is because values are materialized into buckets which are combined at read time."
            )

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

    def num_pipelines(self):
        return len(self._pipelines)

    @property
    def timestamp_field(self):
        return self._timestamp_field

    @property
    def key_fields(self):
        return self._key_fields

    @property
    def erase_key_fields(self):
        return self._erase_key_fields

    @property
    def on_demand(self):
        return self._on_demand

    @property
    def fields(self):
        return self._fields


# ---------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------


class IndexType(str, Enum):
    primary = "primary"

    @classmethod
    def _missing_(cls, value):
        valid_types = [m.value for m in cls]
        raise ValueError(
            f"`{value}` is not a valid index 'type'. "
            f"Following types are allowed {valid_types}"
        )


class IndexDuration(Enum):
    none = None
    forever = "forever"
    duration = datetime.timedelta


class Index(BaseModel):
    type: IndexType
    online: bool
    offline: IndexDuration


@overload
def index(  # noqa: E704
    *,
    type: Literal["primary"] = DEFAULT_INDEX_TYPE,  # type: ignore
    online: bool = DEFAULT_INDEX_ONLINE,
    offline: Optional[str] = DEFAULT_INDEX_OFFLINE,
) -> Callable[[Dataset], Dataset]: ...


@overload
def index(obj: Dataset) -> Dataset: ...  # noqa: E704


def index(
    obj: Optional[Dataset] = None,
    type: Literal["primary"] = DEFAULT_INDEX_TYPE,  # type: ignore
    online: bool = DEFAULT_INDEX_ONLINE,
    offline: Optional[str] = DEFAULT_INDEX_OFFLINE,
) -> Union[Callable[[Dataset], Dataset], Dataset]:
    """index decorator, defaults to a dataset with primary index, online set to true and forever lookup.

    index decorator is used to add a index object mainly to dataset object.
    It takes in the following arguments:
    Parameters
    ----------
    obj: Dataset
        Object
    type : Literal["primary"]
        Type of index in the dataset, choose from (`primary`)
    online : str
        Whether we want to do lookup on the dataset.
    offline : Optional[str]
        Use this if we want to do lookup on past value or the following the dataset wants to be used as RHS
        in `join` operator.
    """

    def _create_indexed_dataset(
        obj: Dataset,
        type: Literal["primary"],
        online: bool,
        offline: Optional[str],
    ):
        if not isinstance(obj, Dataset):
            raise ValueError(
                "`index` decorator can only be called on a Dataset."
            )

        if hasattr(obj, INDEX_FIELD):
            raise ValueError(
                "`index` can only be called once on a Dataset. Found either more than one index decorators on "
                f"Dataset `{obj._name}` or found 'index', 'offline' or 'online' param on @dataset with @index decorator."
            )

        if type == IndexType.primary and len(obj.key_fields) < 1:
            raise ValueError(
                "Index decorator is only applicable for datasets with keyed fields. Found zero key fields."
            )

        if offline is None:
            offline_enum = IndexDuration.none
        elif offline == "forever":
            offline_enum = IndexDuration.forever
        else:
            raise ValueError(
                "Currently offline index only supports `None` and `forever`"
            )

        index_obj = Index(
            type=IndexType(type),
            online=online,
            offline=offline_enum,
        )
        setattr(obj, INDEX_FIELD, index_obj)
        return obj

    def wrap(c: Dataset) -> Dataset:
        return _create_indexed_dataset(c, type, online, offline)  # type: ignore

    if obj is None:
        # We're being called as @index(arguments)
        return wrap
    obj = cast(Dataset, obj)
    # @index decorator was used without arguments
    return wrap(obj)


def get_index(obj: Dataset) -> Optional[Index]:
    if not hasattr(obj, INDEX_FIELD):
        return None
    return getattr(obj, INDEX_FIELD)


def indices_from_ds(
    obj: Dataset,
) -> Tuple[
    Optional[index_proto.OnlineIndex], Optional[index_proto.OfflineIndex]
]:
    """
    Returns OnlineIndexProto and OfflineIndexProto associated with the dataset respectively.
    Args:
        obj: Dataset
            Object
    Returns:
        Tuple[Optional[index_proto.OnlineIndex], Optional[index_proto.OfflineIndex]]
    """
    if len(obj.key_fields) == 0:
        return None, None

    index_obj = get_index(obj)
    if index_obj is None:
        return None, None

    # Hard coding primary now, since only one type is supported currently
    index_type = index_proto.IndexType.PRIMARY

    if index_obj.online:
        online_index = index_proto.OnlineIndex(
            ds_version=obj._version,
            ds_name=obj._name,
            index_type=index_type,
            duration=index_proto.IndexDuration(forever="forever"),
        )
    else:
        online_index = None

    if index_obj.offline == IndexDuration.none:
        offline_index = None
    elif index_obj.offline == IndexDuration.forever:
        offline_index = index_proto.OfflineIndex(
            ds_version=obj._version,
            ds_name=obj._name,
            index_type=index_type,
            duration=index_proto.IndexDuration(forever="forever"),
        )
    else:
        raise ValueError(
            "Currently only forever retention is supported for Offline Index"
        )
    return online_index, offline_index


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
        elif isinstance(obj, Select):
            return self.visitSelect(obj)
        elif isinstance(obj, Dedup):
            return self.visitDedup(obj)
        elif isinstance(obj, Explode):
            return self.visitExplode(obj)
        elif isinstance(obj, First):
            return self.visitFirst(obj)
        elif isinstance(obj, Latest):
            return self.visitLatest(obj)
        elif isinstance(obj, DropNull):
            return self.visitDropNull(obj)
        elif isinstance(obj, Assign):
            return self.visitAssign(obj)
        elif isinstance(obj, Changelog):
            return self.visitChangelog(obj)
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

    def visitSelect(self, obj):
        raise NotImplementedError()

    def visitDedup(self, obj):
        raise NotImplementedError()

    def visitExplode(self, obj):
        raise NotImplementedError()

    def visitFirst(self, obj):
        raise NotImplementedError()

    def visitLatest(self, obj):
        raise NotImplementedError()

    def visitDropNull(self, obj):
        raise NotImplementedError()

    def visitAssign(self, obj):
        raise NotImplementedError()

    def visitChangelog(self, obj):
        raise NotImplementedError()


@dataclass
class DSSchema:
    keys: Dict[str, Type]
    values: Dict[str, Type]
    timestamp: str
    name: str = ""
    is_terminal: bool = False

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

    def select_columns(self, columns: List[str]):
        for column in columns:
            if column not in self.fields():
                raise ValueError(
                    f"field `{column}` not found in schema of `{self.name}`"
                )
        if self.timestamp not in columns:
            raise ValueError(
                f"cannot drop timestamp field `{self.timestamp}` from `{self.name}`"
            )
        self.keys = {key: self.keys[key] for key in self.keys if key in columns}
        self.values = {
            key: self.values[key] for key in self.values if key in columns
        }

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

    def to_fields_proto(self) -> List[schema_proto.Field]:
        """
        Converts all the fields in the DSSchema includes keys, values and timestamps into list Field Proto
        """
        fields = [
            schema_proto.Field(
                name=self.timestamp,
                dtype=schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            )
        ]
        for key in self.keys:
            fields.append(
                schema_proto.Field(name=key, dtype=get_datatype(self.keys[key]))
            )
        for value in self.values:
            fields.append(
                schema_proto.Field(
                    name=value, dtype=get_datatype(self.values[value])
                )
            )
        return fields

    def to_dtype_proto(self) -> Dict[str, schema_proto.DataType]:
        """
        Converts all the fields in the DSSchema includes keys, values and timestamps into list Field Proto
        """
        fields = {
            self.timestamp: schema_proto.DataType(
                timestamp_type=schema_proto.TimestampType()
            )
        }
        for key in self.keys:
            fields[key] = get_datatype(self.keys[key])
        for value in self.values:
            fields[value] = get_datatype(self.values[value])
        return fields


class SchemaValidator(Visitor):
    def __init__(self):
        super(SchemaValidator, self).__init__()
        self.pipeline_name = ""

    def validate(self, pipe: Pipeline) -> DSSchema:
        self.pipeline_name = pipe.name
        self.dsname = pipe.dataset_name
        return self.visit(pipe.terminal_node)

    def visit(self, obj) -> DSSchema:
        return super(SchemaValidator, self).visit(obj)

    def visitDataset(self, obj) -> DSSchema:
        return obj.dsschema()

    def visitTransform(self, obj) -> DSSchema:
        input_schema = self.visit(obj.node)
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Transform' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
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
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Filter' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        input_schema.name = f"'[Pipeline:{self.pipeline_name}]->filter node'"
        if obj.filter_type == UDFType.expr:
            expr_type = obj.filter_expr.typeof(input_schema.schema())
            if expr_type != bool:
                raise TypeError(
                    f"Filter expression must return type bool, found {dtype_to_string(expr_type)}."
                )
        return input_schema

    def visitAggregate(self, obj) -> DSSchema:
        # TODO(Aditya): Aggregations should be allowed on only
        # 1. Keyless streams.
        # 2. Keyed datasets, as long as `along` parameter is provided.
        # 3. For outputs of window operators.

        input_schema = self.visit(obj.node)
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Aggregate' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        keys = {f: input_schema.get_type(f) for f in obj.keys_without_window}
        if obj.window_field:
            keys[WINDOW_FIELD_NAME] = Window

            # Check if input dataset is keyed
            if len(input_schema.keys) > 0:
                raise ValueError(
                    f"Using 'window' param in groupby on keyed dataset is not allowed. "
                    f"Found dataset with keys `{list(input_schema.keys.keys())}` in pipeline `{self.pipeline_name}`."
                )

        values: Dict[str, Type] = {}
        if isinstance(obj.along, str):
            along_type = input_schema.values.get(obj.along)
            if along_type is None:
                raise ValueError(
                    f"error with along kwarg of aggregate operator: no datetime field of name `{obj.along}`"
                )
            if along_type != datetime.datetime:
                raise ValueError(
                    f"error with along kwarg of aggregate operator: `{obj.along}` is not of type datetime"
                )
        else:
            if obj.along is not None:
                raise ValueError(
                    f"error with along kwarg of aggregate operator: `{obj.along}` is not of type datetime"
                )

        found_discrete = False
        found_non_discrete = False
        lookback = None
        for agg in obj.aggregates:

            # If default window is present in groupby then each aggregate spec cannot have window different from
            # groupby window. Either pass them null or pass them same.
            if obj.window_field:
                if agg.window and agg.window != obj.window_field:
                    raise ValueError(
                        f"Window in spec : `{agg.window}` not be different from default "
                        f"window set in groupby : `{obj.window_field}`"
                    )
                if not agg.window:
                    agg.window = obj.window_field

            # Check if all specs are either discrete or non-discrete
            if isinstance(agg.window, (Hopping, Tumbling, Session)):
                if found_non_discrete:
                    raise ValueError(
                        f"Windows in all specs have to be either discrete (Hopping/Tumbling/Session) or"
                        f" non-discrete (Continuous/Forever) not both in pipeline `{self.pipeline_name}`."
                    )
                found_discrete = True
            else:
                if found_discrete:
                    raise ValueError(
                        f"Windows in all specs have to be either discrete (Hopping/Tumbling/Session) or"
                        f" non-discrete (Continuous/Forever) not both in pipeline `{self.pipeline_name}`."
                    )
                found_non_discrete = True

            # Check lookback in all windows are same
            if isinstance(agg.window, (Hopping, Tumbling)):
                curr_lookback = agg.window.lookback_total_seconds()
                if lookback and curr_lookback != lookback:
                    raise ValueError(
                        f"Windows in all specs must have same lookback found {lookback} and {curr_lookback} in "
                        f"pipeline `{self.pipeline_name}`."
                    )
                lookback = curr_lookback

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
                        dtype = input_schema.get_type(agg.of)
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
            elif isinstance(agg, Quantile):
                dtype = input_schema.get_type(agg.of)
                if (
                    dtype is pd.Float64Dtype
                    or dtype is pd.Int64Dtype
                    or isinstance(dtype, _Decimal)
                ):
                    if agg.default is not None:
                        values[agg.into_field] = pd.Float64Dtype  # type: ignore
                    else:
                        values[agg.into_field] = Optional[pd.Float64Dtype]  # type: ignore
                else:
                    raise TypeError(
                        f"Cannot get quantile of field {agg.of} of type {dtype_to_string(dtype)}"
                    )
            elif isinstance(agg, ExpDecaySum):
                dtype = input_schema.get_type(agg.of)
                if get_primitive_dtype(dtype) not in primitive_numeric_types:
                    raise TypeError(
                        f"Cannot take exponential decay sum of field {agg.of} of type {dtype_to_string(dtype)}"
                    )
                if agg.half_life is None:
                    raise ValueError(
                        "half_life must be set for ExponentialDecaySum"
                    )
                try:
                    _ = duration_to_timedelta(agg.half_life)
                except Exception as e:
                    raise ValueError(
                        "Invalid half_life value for ExponentialDecaySum: "
                        f"{agg.half_life}. {str(e)}"
                    )
                values[agg.into_field] = pd.Float64Dtype  # type: ignore
            else:
                raise TypeError(f"Unknown aggregate type {type(agg)}")
        if obj.along and (
            found_discrete or obj.emit_strategy == EmitStrategy.Final
        ):
            raise ValueError(
                "'along' param can only be used with non-discrete windows (Continuous/Forever) and lazy emit strategy."
            )

        return DSSchema(
            keys=keys,
            values=values,  # type: ignore
            timestamp=input_schema.timestamp,
            name=f"'[Pipeline:{self.pipeline_name}]->aggregate node'",
            is_terminal=(
                True if obj.emit_strategy == EmitStrategy.Eager else False
            ),
        )

    def visitJoin(self, obj) -> Tuple[DSSchema, bool]:
        left_schema = self.visit(obj.node)
        if left_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Join' as left dataset in terminal in pipeline : `{self.pipeline_name}`."
            )
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

        def validate_right_index(right_dataset: Dataset):
            right_index = get_index(right_dataset)
            if right_index is None:
                raise ValueError(
                    f"Index needs to be set on the right dataset `{right_dataset._name}` for `{output_schema_name}`."
                )

            if right_index.offline == IndexDuration.none:
                raise ValueError(
                    f"`offline` needs to be set on index of the right dataset `{right_dataset._name}` "
                    f"for `{output_schema_name}`."
                )

        validate_join_bounds(obj.within)
        validate_right_index(obj.dataset)

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

        if obj.fields is not None and len(obj.fields) > 0:
            allowed_fields = [x for x in right_schema.values.keys()] + [
                right_schema.timestamp
            ]
            for field in obj.fields:
                if field not in allowed_fields:
                    raise ValueError(
                        f"Field `{field}` specified in fields {obj.fields} "
                        f"doesn't exist in allowed fields {allowed_fields} of "
                        f"right schema of {output_schema_name}."
                    )

            if (
                right_schema.timestamp in obj.fields
                and right_schema.timestamp in left_schema.fields()
            ):
                raise ValueError(
                    f"Field `{right_schema.timestamp}` specified in fields {obj.fields} "
                    f"already exists in left schema of {output_schema_name}."
                )

        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitUnion(self, obj) -> DSSchema:
        if len(obj.nodes) == 0:
            raise ValueError("Union must have at least one node.")
        schema = self.visit(obj.nodes[0])
        if schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Union' as one of the datasets in pipeline : `{self.pipeline_name}` is terminal."
            )

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
            if node_schema.is_terminal:
                raise ValueError(
                    f"Cannot add node 'Union' as one of the datasets in pipeline : `{self.pipeline_name}` is terminal."
                )
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
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Assign' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
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
        input_schema = copy.deepcopy(self.visit(obj.node))
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Assign' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->assign node'"
        if obj.assign_type == UDFType.expr:
            if len(obj.output_expressions) == 0:
                raise ValueError(
                    f"invalid assign - {output_schema_name} must have at least one column to assign"
                )
            # Ensure there are no duplicate columns
            if len(obj.output_expressions) != len(
                set(obj.output_expressions.keys())
            ):
                raise ValueError(
                    f"invalid assign - {output_schema_name} cannot have duplicate columns"
                )
            # Fetch the type for every column and match it against the type provided in the expression
            type_errors = []
            for col, typed_expr in obj.output_expressions.items():
                try:
                    expr_type = typed_expr.expr.typeof(input_schema.schema())
                except Exception as e:
                    raise ValueError(
                        f"invalid assign - {output_schema_name} error in expression for column `{col}`: {str(e)}"
                    )
                if not typed_expr.expr.matches_type(
                    typed_expr.dtype, input_schema.schema()
                ):
                    printer = ExprPrinter()
                    type_errors.append(
                        f"'{col}' is expected to be of type `{dtype_to_string(typed_expr.dtype)}`, but evaluates to `{dtype_to_string(expr_type)}`. Full expression: `{printer.print(typed_expr.expr.root)}`"
                    )

            if len(type_errors) > 0:
                joined_errors = "\n\t".join(type_errors)
                raise TypeError(
                    f"found type errors in assign node of `{self.dsname}.{self.pipeline_name}`:\n\t{joined_errors}"
                )

        else:
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
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Drop' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->drop node'"
        if obj.columns is None or len(obj.columns) == 0:
            raise ValueError(
                f"invalid drop - {output_schema_name} must have at least one column to drop"
            )
        val_fields = input_schema.values.keys()
        for field in obj.columns:
            if field not in val_fields:
                if (
                    field == input_schema.timestamp
                    or field in input_schema.keys
                ):
                    raise ValueError(
                        f"Field `{field}` is a key or timestamp field in schema of "
                        f"drop node input {input_schema.name}. Value fields are: {list(val_fields)}"
                    )
                else:
                    raise ValueError(
                        f"Field `{field}` does not exist in schema of drop node input {input_schema.name}"
                    )

        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitSelect(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Select' after a terminal node in pipeline : `{self.pipeline_name}`."
            )

        timestamp = input_schema.timestamp
        key_fields = input_schema.keys

        output_schema_name = f"'[Pipeline:{self.pipeline_name}]->select node'"
        if obj.columns is None or len(obj.columns) == 0:
            raise ValueError(
                f"invalid select - {output_schema_name} must have at least one column to select"
            )
        if timestamp not in obj.columns:
            raise ValueError(
                f"invalid select - {output_schema_name} timestamp field : `{timestamp}` must be in columns"
            )
        for key_field in key_fields:
            if key_field not in obj.columns:
                raise ValueError(
                    f"invalid select - {output_schema_name} key field : `{key_field}` must be in columns"
                )
        for column in obj.columns:
            if column not in input_schema.fields():
                raise ValueError(
                    f"invalid select - {output_schema_name} field : `{column}` not found in {input_schema.name}"
                )

        # If all columns are selected, throw an error since the corresponding
        # drop operator will get empty columns, returning a server error.
        selected_columns = set(obj.columns)
        # Add timestamp field to the selected columns
        selected_columns.add(timestamp)
        if len(selected_columns) == len(input_schema.fields()):
            warnings.warn(
                f"invalid select - {output_schema_name} has selected all the fields in the dataset `{self.dsname}`. Please remove the select operator."
            )

        output_schema = obj.dsschema()
        output_schema.name = output_schema_name
        return output_schema

    def visitDropNull(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Dedup' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
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
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Dedup' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
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
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Explode' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        # If it is a keyed dataset throw an error. Explode over keyed
        # datasets, is not defined, since for a keyed dataset, there is only one value for each key.
        if len(input_schema.keys) > 0:
            raise TypeError(
                f"Explode over keyed datasets is not defined. Found dataset with keys `{list(input_schema.keys.keys())}` in pipeline `{self.pipeline_name}`"
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
        input_schema = copy.deepcopy(self.visit(obj.node))
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'First' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        output_schema = copy.deepcopy(obj.dsschema())
        # If it is a keyed dataset throw an error.
        # We dont allow first over keyed datasets, but only keyless streams.
        if len(input_schema.keys) > 0:
            raise TypeError(
                f"First over keyed datasets is not defined. Found dataset with keys `{list(input_schema.keys.keys())}` in pipeline `{self.pipeline_name}`"
            )

        if len(output_schema.keys) == 0:
            raise ValueError(
                f"'group_by' before 'first' in {self.pipeline_name} must specify at least one key"
            )
        output_schema.name = f"'[Pipeline:{self.pipeline_name}]->first node'"
        return output_schema

    def visitLatest(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        if input_schema.is_terminal:
            raise ValueError(
                f"Cannot add node 'Latest' after a terminal node in pipeline : `{self.pipeline_name}`."
            )
        output_schema = copy.deepcopy(obj.dsschema())
        # If it is a keyed dataset throw an error.
        # We dont allow latest over keyed datasets, but only keyless streams.
        if len(input_schema.keys) > 0:
            raise TypeError(
                f"Latest over keyed datasets is not defined. Found dataset with keys `{list(input_schema.keys.keys())}` in pipeline `{self.pipeline_name}`"
            )

        if len(output_schema.keys) == 0:
            raise ValueError(
                f"'group_by' before 'latest' in {self.pipeline_name} must specify at least one key"
            )
        output_schema.name = f"'[Pipeline:{self.pipeline_name}]->lastest node'"
        return output_schema

    def visitChangelog(self, obj) -> DSSchema:
        input_schema = copy.deepcopy(self.visit(obj.node))
        # Unkey operation is allowed on keyed datasets only.
        if len(input_schema.keys) == 0:
            raise TypeError(
                f"UnKey operation is allowed only on keyed datasets. Found dataset without keys in pipeline `{self.pipeline_name}`"
            )
        output_schema = copy.deepcopy(obj.dsschema())
        output_schema.name = (
            f"'[Pipeline:{self.pipeline_name}]->Changelog node'"
        )
        return output_schema
