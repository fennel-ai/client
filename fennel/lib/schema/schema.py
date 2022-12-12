from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Union, Any, TYPE_CHECKING

import pandas as pd
import pyarrow as pa  # type: ignore

if TYPE_CHECKING:
    Series = pd.Series
else:

    class Series:
        def __class_getitem__(cls, item):
            return item


if TYPE_CHECKING:
    DataFrame = pd.DataFrame
else:

    class DataFrame:
        def __class_getitem__(cls, item):
            return item


def _get_args(type_: Any) -> Any:
    """Get the type arguments of a type."""
    return getattr(type_, "__args__", None)


def _get_origin(type_: Any) -> Any:
    """Get the origin of a type."""
    return getattr(type_, "__origin__", None)


def _is_optional(field):
    return _get_origin(field) is Union and type(None) in _get_args(field)


def dtype_to_string(type_: Any) -> str:
    if _is_optional(type_):
        return f"Optional[{dtype_to_string(_get_args(type_)[0])}]"
    return str(type_)


@dataclass
class _Embedding:
    dim: int


if TYPE_CHECKING:
    # Some type that can take an integer and keep mypy happy :)
    Embedding = pd.Series
else:

    class Embedding:
        def __init__(self, dim: int):
            raise TypeError(
                "Embedding is a type only and is meant to be used as "
                "a type hint, for example: Embedding[32]"
            )

        def __class_getitem__(cls, dimensions: int):
            return _Embedding(dimensions)


def get_pyarrow_field(name: str, type_: Any) -> pa.lib.Field:
    """Convert a field name and python type to a pa field."""
    # typing.Optional[x] is an alias for typing.Union[x, None]
    if _get_origin(type_) is Union and type(None) == _get_args(type_)[1]:
        return pa.field(
            name, get_pyarrow_datatype(_get_args(type_)[0]), nullable=True
        )
    elif _get_origin(type_) is Union:
        x = [get_pyarrow_field(name, t) for t in _get_args(type_)]
        return pa.union(x, mode="dense")
    return pa.field(name, get_pyarrow_datatype(type_), nullable=False)


def get_pyarrow_datatype(type_: Any) -> pa.lib.DataType:
    """
    Convert a python type to a pa type (
    https://arrow.apache.org/docs/3.0/python/api/datatypes.html ).
    """
    if type_ is None:
        return None
    elif type_ is bool:
        return pa.bool_()
    elif type_ is int:
        return pa.int64()
    elif type_ is float:
        return pa.float64()
    elif type_ is str:
        return pa.string()
    elif type_ is datetime:
        return pa.timestamp("ns")
    elif _get_origin(type_) is tuple:
        raise ValueError("Tuple is not supported currently, please use List.")
    elif isinstance(type_, _Embedding):
        return pa.list_(pa.float64(), type_.dim)
    elif _get_origin(type_) is list:
        return pa.list_(get_pyarrow_datatype(_get_args(type_)[0]))
    elif _get_origin(type_) is set:
        # return pa.map_(get_pyarrow_schema(_get_args(type_)[0]), pa.null())
        raise ValueError("Set is not supported currently.")
    elif _get_origin(type_) is dict:
        if _get_args(type_)[0] is not str:
            raise ValueError("Dict keys must be strings.")
        return pa.map_(
            get_pyarrow_datatype(_get_args(type_)[0]),
            get_pyarrow_datatype(_get_args(type_)[1]),
        )
    else:
        raise ValueError(f"Cannot convert type {type_} to pa schema.")
