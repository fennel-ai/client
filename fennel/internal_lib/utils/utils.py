import dataclasses
from datetime import datetime
from decimal import Decimal
from typing import Any, Union

import pandas as pd

from fennel.gen.schema_pb2 import DataType


def _get_args(type_: Any) -> Any:
    """Get the type arguments of a type."""
    return getattr(type_, "__args__", None)


def _is_optional(field):
    return get_origin(field) is Union and type(None) in _get_args(field)


def _optional_inner(type_):
    return _get_args(type_)[0]


def get_origin(type_: Any) -> Any:
    """Get the origin of a type."""
    return getattr(type_, "__origin__", None)


def is_user_defined_class(cls) -> bool:
    return isinstance(cls, type) and cls.__module__ not in [
        "builtins",
        "datetime",
        "decimal",
    ]


def dtype_to_string(type_: Any) -> str:
    if _is_optional(type_):
        return f"Optional[{dtype_to_string(_optional_inner(type_))}]"
    if type_ == pd.Int64Dtype:
        return "int"
    if type_ == pd.Float64Dtype:
        return "float"
    if type_ == pd.StringDtype:
        return "str"
    if type_ == pd.BooleanDtype:
        return "bool"
    if isinstance(type_, type):
        return type_.__name__
    return str(type_)


def as_json(self):
    def to_dict(value):
        if dataclasses.is_dataclass(value):
            return value.as_json()
        elif isinstance(value, list):
            return [to_dict(v) for v in value]
        elif isinstance(value, dict):
            return {k: to_dict(v) for k, v in value.items()}
        else:
            return value

    result = {}
    for field in dataclasses.fields(self):
        value = getattr(self, field.name)
        result[field.name] = to_dict(value)
    return result


def parse_datetime(value: Union[int, str, datetime]) -> datetime:
    if isinstance(value, int):
        try:
            value = pd.to_datetime(value, unit="s", utc=True)
        except ValueError:
            try:
                value = pd.to_datetime(value, unit="ms", utc=True)
            except ValueError:
                try:
                    value = pd.to_datetime(value, unit="us", utc=True)
                except ValueError:
                    value = pd.to_datetime(value, unit="ns", utc=True)
    else:
        value = pd.to_datetime(value, utc=True)
    return value  # type: ignore


def cast_col_to_pandas(
    series: pd.Series, dtype: DataType, nullable: bool = False
) -> pd.Series:
    if not dtype.HasField("optional_type") and not nullable:
        if series.isnull().any():
            raise ValueError("Null values found in non-optional field.")
    if dtype.HasField("optional_type"):
        return cast_col_to_pandas(series, dtype.optional_type.of, True)
    elif dtype.HasField("int_type"):
        return series.astype(pd.Int64Dtype())
    elif dtype.HasField("double_type"):
        return series.astype(pd.Float64Dtype())
    elif dtype.HasField("string_type") or dtype.HasField("regex_type"):
        return series.astype(pd.StringDtype())
    elif dtype.HasField("bool_type"):
        return series.astype(pd.BooleanDtype())
    elif dtype.HasField("timestamp_type"):
        return pd.to_datetime(series)
    elif dtype.HasField("date_type"):
        return pd.to_datetime(series).dt.date
    elif dtype.HasField("one_of_type"):
        return cast_col_to_pandas(series, dtype.one_of_type.of)
    elif dtype.HasField("between_type"):
        return cast_col_to_pandas(series, dtype.between_type.dtype)
    elif dtype.HasField("decimal_type"):
        scale = dtype.decimal_type.scale
        return pd.Series(
            [
                (
                    Decimal("%0.{}f".format(scale) % float(x))
                    if not isinstance(x, Decimal)
                    else x
                )
                for x in series
            ]
        )
    else:
        return series.fillna(pd.NA)
