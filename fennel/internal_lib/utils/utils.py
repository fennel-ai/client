import dataclasses
from typing import Any, Union

import pandas as pd


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
