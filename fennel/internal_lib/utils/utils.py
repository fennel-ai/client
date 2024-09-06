import dataclasses
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Union, Dict

import numpy as np
import pandas as pd
from frozendict import frozendict
from fennel.gen import schema_pb2 as schema_proto
from fennel.gen.schema_pb2 import DataType
from fennel.internal_lib import FENNEL_STRUCT


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
    if isinstance(value, (int, float)):
        value = int(value)
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


def parse_struct_into_dict(
    value: Any, dtype: schema_proto.DataType
) -> Optional[Union[dict, list]]:
    """
    This function assumes that there's a struct somewhere in the value that needs to be converted into json.
    """
    if hasattr(value, FENNEL_STRUCT):
        try:
            return value.as_json()
        except Exception as e:
            raise TypeError(
                f"Not able parse value: {value} into json, error: {e}"
            )
    elif isinstance(value, list) or isinstance(value, np.ndarray):
        return [parse_struct_into_dict(x, dtype.array_type.of) for x in value]
    elif isinstance(value, dict) or isinstance(value, frozendict):
        return {
            key: parse_struct_into_dict(val, dtype.map_type.value)
            for key, val in value.items()
        }
    elif value is None or pd.isna(value):
        # If dtype is an optional struct type return, a dict with all fields as None
        if dtype.HasField("optional_type") and dtype.optional_type.of.HasField(
            "struct_type"
        ):
            return {
                field.name: None
                for field in dtype.optional_type.of.struct_type.fields
            }
        else:
            return None
    else:
        return value


def parse_datetime_in_value(
    value: Any, dtype: DataType, nullable: bool = False
) -> Any:
    """
    This function assumes that there's a struct somewhere in the value that needs to be converted into json.
    """
    if nullable:
        try:
            if not isinstance(
                value, (list, tuple, dict, set, np.ndarray, frozendict)
            ) and pd.isna(value):
                return pd.NA
        # ValueError error occurs when you do something like pd.isnull([1, 2, None])
        except ValueError:
            pass
    if dtype.HasField("optional_type"):
        return parse_datetime_in_value(value, dtype.optional_type.of, True)
    elif dtype.HasField("timestamp_type"):
        return parse_datetime(value)
    elif dtype.HasField("array_type"):
        return [parse_datetime_in_value(x, dtype.array_type.of) for x in value]
    elif dtype.HasField("map_type"):
        if isinstance(value, (dict, frozendict)):
            return {
                key: parse_datetime_in_value(value, dtype.map_type.value)
                for (key, value) in value.items()
            }
        elif isinstance(value, (list, np.ndarray)):
            return [
                (key, parse_datetime_in_value(value, dtype.map_type.value))
                for (key, value) in value
            ]
        else:
            return value
    elif dtype.HasField("struct_type"):
        if hasattr(value, FENNEL_STRUCT):
            try:
                value = value.as_json()
            except Exception as e:
                raise TypeError(
                    f"Not able parse value: {value} into json, error: {e}"
                )
        output: Dict[Any, Any] = {}
        for field in dtype.struct_type.fields:
            dtype = field.dtype
            name = field.name
            if not dtype.HasField("optional_type") and name not in value:
                raise ValueError(
                    f"value not found for non optional field : {field}"
                )
            if name in value:
                output[name] = parse_datetime_in_value(value[name], dtype)
        return output
    else:
        return value
