from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Union, Dict, Any, List, TYPE_CHECKING

import numpy as np
import pandas as pd

import fennel.gen.schema_pb2 as proto

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
    if isinstance(type_, type):
        return type_.__name__
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


@dataclass
class between:
    dtype: type
    min: Union[int, float]
    max: Union[int, float]
    strict_min: bool = False
    strict_max: bool = False

    def to_proto(self):
        if self.dtype is not int and self.dtype is not float:
            raise TypeError("'between' type only accepts int or float types")
        if self.dtype is int:
            scalar_type = proto.ScalarType.INT
            if type(self.min) is float:
                raise TypeError(
                    "Dtype of between is int and min param is " "float"
                )
            if type(self.max) is float:
                raise TypeError(
                    "Dtype of between is int and max param is " "float"
                )

            min = proto.Param(int_val=self.min)
            max = proto.Param(int_val=self.max)
        else:
            scalar_type = proto.ScalarType.FLOAT
            min = proto.Param(float_val=float(self.min))
            max = proto.Param(float_val=float(self.max))

        return proto.DataType(
            between_type=proto.Between(
                scalar_type=scalar_type,
                min=min,
                max=max,
                strict_min=self.strict_min,
                strict_max=self.strict_max,
            )
        )


@dataclass
class oneof:
    dtype: type
    options: List[Union[str, int]]

    def to_proto(self):
        if self.dtype is not int and self.dtype is not str:
            raise TypeError("'oneof' type only accepts int or str types")
        for x in self.options:
            if type(x) != self.dtype:
                raise TypeError(
                    "'oneof' options should match the type of "
                    f"dtype, found '{dtype_to_string(type(x))}' "
                    f"expected '{dtype_to_string(self.dtype)}'."
                )

        if self.dtype is int:
            return proto.DataType(
                one_of_type=proto.OneOf(
                    scalar_type=proto.ScalarType.INT,
                    options=[proto.Param(int_val=x) for x in self.options],
                )
            )
        elif self.dtype is str:
            return proto.DataType(
                one_of_type=proto.OneOf(
                    scalar_type=proto.ScalarType.STRING,
                    options=[proto.Param(str_val=x) for x in self.options],
                )
            )


@dataclass
class regex:
    regex: str

    def to_proto(self):
        if type(self.regex) is not str:
            raise TypeError("'regex' type only accepts str types")
        return proto.DataType(regex_type=self.regex)


def get_datatype(type_: Any) -> proto.DataType:
    # typing.Optional[x] is an alias for typing.Union[x, None]
    if _get_origin(type_) is Union and type(None) == _get_args(type_)[1]:
        dtype = get_datatype(_get_args(type_)[0])
        dtype.is_nullable = True
        return dtype
    elif type_ is int:
        return proto.DataType(scalar_type=proto.ScalarType.INT)
    elif type_ is float:
        return proto.DataType(scalar_type=proto.ScalarType.FLOAT)
    elif type_ is str:
        return proto.DataType(scalar_type=proto.ScalarType.STRING)
    elif type_ is datetime:
        return proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP)
    elif type_ is bool:
        return proto.DataType(scalar_type=proto.ScalarType.BOOLEAN)
    elif _get_origin(type_) is list:
        return proto.DataType(
            array_type=proto.ArrayType(of=get_datatype(_get_args(type_)[0]))
        )
    elif _get_origin(type_) is dict:
        if _get_args(type_)[0] is not str:
            raise ValueError("Dict keys must be strings.")
        return proto.DataType(
            map_type=proto.MapType(
                key=get_datatype(_get_args(type_)[0]),
                value=get_datatype(_get_args(type_)[1]),
            )
        )
    elif isinstance(type_, _Embedding):
        return proto.DataType(
            embedding_type=proto.EmbeddingType(embedding_size=type_.dim)
        )
    elif (
        isinstance(type_, between)
        or isinstance(type_, oneof)
        or isinstance(type_, regex)
    ):
        return type_.to_proto()
    raise ValueError(f"Cannot serialize type {type_}.")


# TODO(Aditya): Add support for nested schema checks for arrays and maps
def data_schema_check(
    schema: Dict[str, proto.DataType], df: pd.DataFrame
) -> List[ValueError]:
    exceptions = []
    # Check schema of fields with the dataframe
    for name, dtype in schema.items():
        if name not in df.columns:
            exceptions.append(
                ValueError(
                    f"Field {name} not found in dataframe. "
                    f"Please ensure the dataframe has the same schema as the "
                    f"dataset."
                )
            )

        if not dtype.is_nullable:
            if df[name].isnull().any():
                exceptions.append(
                    ValueError(
                        f"Field {name} is not nullable, but the "
                        f"column in the dataframe has null values."
                    )
                )
        else:
            if dtype == proto.DataType(
                scalar_type=proto.ScalarType.INT, is_nullable=True
            ):
                if df[name].dtype != np.int64 and df[name].dtype != np.float64:
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type int, but the "
                            f"column in the dataframe is of type "
                            f"{df[name].dtype}."
                        )
                    )
                continue

        dtype.is_nullable = False
        if dtype == proto.DataType(scalar_type=proto.ScalarType.INT):
            if df[name].dtype != np.int64 and df[name].dtype != pd.Int64Dtype():
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type int, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype == proto.DataType(scalar_type=proto.ScalarType.FLOAT):
            if (
                df[name].dtype != np.float64
                and df[name].dtype != np.int64
                and df[name].dtype != pd.Int64Dtype()
                and df[name].dtype != pd.Float64Dtype()
            ):
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type float, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype == proto.DataType(scalar_type=proto.ScalarType.STRING):
            if (
                df[name].dtype != object
                and df[name].dtype != np.str_
                and df[name].dtype != pd.StringDtype()
            ):
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type str, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype == proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP):
            if df[name].dtype != "datetime64[ns]":
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type timestamp, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype == proto.DataType(scalar_type=proto.ScalarType.BOOLEAN):
            if df[name].dtype != np.bool_:
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type bool, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype.embedding_type.embedding_size > 0:
            if df[name].dtype != object:
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type embedding, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
            # Check that the embedding is a list of floats of size embedding_size
            for i, row in df[name].items():
                if not isinstance(row, np.ndarray) and not isinstance(
                    row, list
                ):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type embedding, but the "
                            f"column in the dataframe is not a list."
                        )
                    )
                    break
                if len(row) != dtype.embedding_type.embedding_size:
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type embedding, of size "
                            f"{dtype.embedding_type.embedding_size}, but the "
                            "column in the dataframe has a list of size "
                            f"{len(row)}."
                        )
                    )
        elif dtype.array_type.of != proto.DataType():
            if df[name].dtype != object:
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type array, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
                continue
            for i, row in df[name].items():
                if not isinstance(row, np.ndarray) and not isinstance(
                    row, list
                ):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type array, but the "
                            f"column in the dataframe is not a list."
                        )
                    )
                    break
        elif dtype.map_type.key != proto.DataType():
            if df[name].dtype != object:
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type map, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
            for i, row in df[name].items():
                if not isinstance(row, dict):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type map, but the "
                            f"column in the dataframe is not a dict."
                        )
                    )
                    break
        elif dtype.between_type != proto.Between():
            bw_type = dtype.between_type
            if bw_type.scalar_type == proto.ScalarType.INT:
                if (
                    df[name].dtype != np.int64
                    and df[name].dtype != pd.Int64Dtype()
                ):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type int, but the "
                            f"column in the dataframe is of type "
                            f"{df[name].dtype}."
                        )
                    )
                    continue
                min_bound = bw_type.min.int_val
                max_bound = bw_type.max.int_val
            elif bw_type.scalar_type == proto.ScalarType.FLOAT:
                if (
                    df[name].dtype != np.float64
                    and df[name].dtype != np.int64
                    and df[name].dtype != pd.Int64Dtype()
                    and df[name].dtype != pd.Float64Dtype()
                ):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type float, but the "
                            f"column in the dataframe is of type "
                            f"{df[name].dtype}."
                        )
                    )
                    continue
                min_bound = bw_type.min.float_val  # type: ignore
                max_bound = bw_type.max.float_val  # type: ignore
            else:
                raise TypeError(
                    "'between' type only accepts int or float types"
                )
            for i, row in df[name].items():
                if (
                    row < min_bound
                    or row > max_bound
                    or (bw_type.strict_min and row == min_bound)
                    or (bw_type.strict_max and row == max_bound)
                ):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type between, but the "
                            f"value {row} is out of bounds."
                        )
                    )
                    break
        elif dtype.one_of_type != proto.OneOf():
            of_type = dtype.one_of_type
            if of_type.scalar_type == proto.ScalarType.INT:
                if (
                    df[name].dtype != np.int64
                    and df[name].dtype != pd.Int64Dtype()
                ):
                    exceptions.append(
                        ValueError(
                            f"Field {name} is of type int, but the "
                            f"column in the dataframe is of type "
                            f"{df[name].dtype}."
                        )
                    )
                    continue
                options = set(int(x.int_val) for x in of_type.options)
            elif of_type.scalar_type == proto.ScalarType.STRING:
                if (
                    df[name].dtype != object
                    and df[name].dtype != np.str_
                    and df[name].dtype != pd.StringDtype()
                ):
                    exceptions.append(
                        ValueError(
                            f"Field '{name}' is of type str, but the "
                            f"column in the dataframe is of type "
                            f"{df[name].dtype}."
                        )
                    )
                    continue
                options = set(
                    str(x.str_val) for x in of_type.options  # type: ignore
                )
            else:
                raise TypeError("oneof type only accepts int or str types")

            for i, row in df[name].items():
                if row not in options:
                    sorted_options = sorted(options)
                    exceptions.append(
                        ValueError(
                            f"Field '{name}' is of type oneof, but the "
                            f"value '{row}' is not found in the set of options "
                            f"{sorted_options}."
                        )
                    )
                    break

        elif dtype.regex_type != "":
            if (
                df[name].dtype != object
                and df[name].dtype != np.str_
                and df[name].dtype != pd.StringDtype()
            ):
                exceptions.append(
                    ValueError(
                        f"Field '{name}' is of type str, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
                continue
            regex = dtype.regex_type
            for i, row in df[name].items():
                full_match = "^" + regex + "$"
                if not re.match(full_match, row):
                    exceptions.append(
                        ValueError(
                            f"Field '{name}' is of type regex, but the "
                            f"value '{row}' does not match the regex "
                            f"{regex}."
                        )
                    )
                    break
        else:
            exceptions.append(
                ValueError(f"Field {name} has unknown data type " f"{dtype}.")
            )
    return exceptions
