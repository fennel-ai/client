from __future__ import annotations

from datetime import datetime
from typing import Union, Any

import pyarrow as pa  # type: ignore


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


def get_pyarrow_field(name: str, type_: Any) -> pa.lib.Field:
    """Convert a field name and python type to a pa field."""
    # typing.Optional[x] is an alias for typing.Union[x, None]
    if _get_origin(type_) is Union and type(None) == _get_args(type_)[1]:
        return pa.field(
            name, get_pyarrow_schema(_get_args(type_)[0]), nullable=True
        )
    elif _get_origin(type_) is Union:
        x = [get_pyarrow_field(name, t) for t in _get_args(type_)]
        return pa.union(x, mode="dense")
    return pa.field(name, get_pyarrow_schema(type_), nullable=False)


def get_pyarrow_schema(type_: Any) -> pa.lib.Schema:
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
    elif type_ is bytes:
        return pa.binary()
    elif type_ is datetime:
        return pa.timestamp("ns")
    elif _get_origin(type_) is tuple:
        return pa.struct(
            [
                get_pyarrow_field(f"f{i}", t)
                for i, t in enumerate(_get_args(type_))
            ]
        )
    elif _get_origin(type_) is list:
        return pa.list_(get_pyarrow_schema(_get_args(type_)[0]))
    elif _get_origin(type_) is set:
        return pa.map_(get_pyarrow_schema(_get_args(type_)[0]), pa.null())
    elif _get_origin(type_) is dict:
        return pa.map_(
            get_pyarrow_schema(_get_args(type_)[0]),
            get_pyarrow_schema(_get_args(type_)[1]),
        )
    elif hasattr(type_, "__dataclass_fields__"):
        return pa.struct(
            [
                (k, get_pyarrow_schema(v.type))
                for k, v in type_.__dataclass_fields__.items()
            ]
        )
    else:
        raise ValueError(f"Cannot convert type {type_} to pa schema.")
