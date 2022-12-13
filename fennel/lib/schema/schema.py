from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Union, Dict, Any, List, TYPE_CHECKING

import pandas as pd
import numpy as np

import fennel.gen.schema_pb2 as proto
from fennel.gen.dataset_pb2 import CreateDatasetRequest

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
    raise ValueError(f"Cannot serialize type {type_}.")


# TODO: Add support for nested schema checks for arrays and maps
def schema_check(
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
            if df[name].dtype != np.int64:
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type int, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype == proto.DataType(scalar_type=proto.ScalarType.FLOAT):
            if df[name].dtype != np.float64 and df[name].dtype != np.int64:
                exceptions.append(
                    ValueError(
                        f"Field {name} is of type float, but the "
                        f"column in the dataframe is of type "
                        f"{df[name].dtype}."
                    )
                )
        elif dtype == proto.DataType(scalar_type=proto.ScalarType.STRING):
            if df[name].dtype != object:
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
        else:
            exceptions.append(
                ValueError(f"Field {name} has unknown data type " f"{dtype}.")
            )
    return exceptions
