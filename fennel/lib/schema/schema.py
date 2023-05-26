from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import pandas as pd
from typing import Union, Any, List, TYPE_CHECKING

import fennel.gen.schema_pb2 as schema_proto

FENNEL_INPUTS = "__fennel_inputs__"
FENNEL_OUTPUTS = "__fennel_outputs__"


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


def get_dtype(dtype):
    if isinstance(dtype, oneof) or isinstance(dtype, between):
        return dtype.dtype
    if isinstance(dtype, regex):
        return str
    return dtype


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
            dtype = schema_proto.DataType(int_type=schema_proto.IntType())
            if type(self.min) is float:
                raise TypeError(
                    "Dtype of between is int and min param is " "float"
                )
            if type(self.max) is float:
                raise TypeError(
                    "Dtype of between is int and max param is " "float"
                )

            min = schema_proto.Value(int=int(self.min))
            max = schema_proto.Value(int=int(self.max))
        else:
            dtype = schema_proto.DataType(double_type=schema_proto.DoubleType())
            min = schema_proto.Value(float=float(self.min))
            max = schema_proto.Value(float=float(self.max))

        return schema_proto.DataType(
            between_type=schema_proto.Between(
                dtype=dtype,
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
            dtype = schema_proto.DataType(int_type=schema_proto.IntType())
            options = []
            for option in self.options:
                if type(option) is not int:
                    raise TypeError(
                        "Dtype of oneof is int and option is " "not int"
                    )
                options.append(schema_proto.Value(int=option))
            return schema_proto.DataType(
                one_of_type=schema_proto.OneOf(
                    of=dtype,
                    options=options,
                )
            )

        if self.dtype is str:
            dtype = schema_proto.DataType(string_type=schema_proto.StringType())
            options = []
            for option in self.options:
                if type(option) is not str:
                    raise TypeError(
                        "Dtype of oneof is str and option is " "not str"
                    )
                options.append(schema_proto.Value(string=option))
            return schema_proto.DataType(
                one_of_type=schema_proto.OneOf(
                    of=dtype,
                    options=options,
                )
            )

        raise ValueError(f"Invalid dtype {self.dtype} for oneof")


@dataclass
class regex:
    regex: str

    def to_proto(self):
        if type(self.regex) is not str:
            raise TypeError("'regex' type only accepts str types")
        return schema_proto.DataType(
            regex_type=schema_proto.RegexType(pattern=self.regex)
        )


def get_datatype(type_: Any) -> schema_proto.DataType:
    # typing.Optional[x] is an alias for typing.Union[x, None]
    if _get_origin(type_) is Union and type(None) == _get_args(type_)[1]:
        dtype = get_datatype(_get_args(type_)[0])
        return schema_proto.DataType(
            optional_type=schema_proto.OptionalType(of=dtype)
        )
    elif type_ is int:
        return schema_proto.DataType(int_type=schema_proto.IntType())
    elif type_ is float:
        return schema_proto.DataType(double_type=schema_proto.DoubleType())
    elif type_ is str:
        return schema_proto.DataType(string_type=schema_proto.StringType())
    elif type_ is datetime:
        return schema_proto.DataType(
            timestamp_type=schema_proto.TimestampType()
        )
    elif type_ is bool:
        return schema_proto.DataType(bool_type=schema_proto.BoolType())
    elif _get_origin(type_) is list:
        return schema_proto.DataType(
            array_type=schema_proto.ArrayType(
                of=get_datatype(_get_args(type_)[0])
            )
        )
    elif _get_origin(type_) is dict:
        if _get_args(type_)[0] is not str:
            raise ValueError("Dict keys must be strings.")
        return schema_proto.DataType(
            map_type=schema_proto.MapType(
                key=get_datatype(_get_args(type_)[0]),
                value=get_datatype(_get_args(type_)[1]),
            )
        )
    elif isinstance(type_, _Embedding):
        return schema_proto.DataType(
            embedding_type=schema_proto.EmbeddingType(embedding_size=type_.dim)
        )
    elif (
        isinstance(type_, between)
        or isinstance(type_, oneof)
        or isinstance(type_, regex)
    ):
        return type_.to_proto()
    raise ValueError(f"Cannot serialize type {type_}.")


# TODO(Aditya): Add support for nested schema checks for arrays and maps
def _validate_field_in_df(
    field: schema_proto.Field,
    df: pd.DataFrame,
    entity_name: str,
    is_nullable: bool = False,
):
    name = field.name
    dtype = field.dtype
    if name not in df.columns:
        raise ValueError(
            f"Field `{name}` not found in dataframe during checking schema for "
            f"`{entity_name}`. "
            f"Please ensure the dataframe has the correct schema."
        )

    # Check for the optional type
    if dtype.optional_type != schema_proto.OptionalType():
        return _validate_field_in_df(
            field=schema_proto.Field(name=name, dtype=dtype.optional_type.of),
            df=df,
            entity_name=entity_name,
            is_nullable=True,
        )

    if not is_nullable and df[name].isnull().any():
        raise ValueError(
            f"Field `{name}` is not nullable, but the "
            f"column in the dataframe has null values. Error found during"
            f"checking schema for `{entity_name}`."
        )

    if dtype == schema_proto.DataType(int_type=schema_proto.IntType()):
        if is_nullable:
            # If the dtype is nullable int64 gets converted to Float64
            if (
                df[name].dtype != np.int64
                and df[name].dtype != pd.Int64Dtype()
                and df[name].dtype != np.float64
                and df[name].dtype != pd.Float64Dtype()
            ):
                raise ValueError(
                    f"Field `{name}` is of type int, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
        else:
            if df[name].dtype != np.int64 and df[name].dtype != pd.Int64Dtype():
                raise ValueError(
                    f"Field `{name}` is of type int, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
    elif dtype == schema_proto.DataType(double_type=schema_proto.DoubleType()):
        if (
            df[name].dtype != np.float64
            and df[name].dtype != np.int64
            and df[name].dtype != pd.Int64Dtype()
            and df[name].dtype != pd.Float64Dtype()
        ):
            raise ValueError(
                f"Field `{name}` is of type float, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype == schema_proto.DataType(string_type=schema_proto.StringType()):
        if (
            df[name].dtype != object
            and df[name].dtype != np.str_
            and df[name].dtype != pd.StringDtype()
        ):
            raise ValueError(
                f"Field `{name}` is of type str, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype == schema_proto.DataType(
        timestamp_type=schema_proto.TimestampType()
    ):
        if df[name].dtype != "datetime64[ns]":
            raise ValueError(
                f"Field `{name}` is of type timestamp, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype == schema_proto.DataType(bool_type=schema_proto.BoolType()):
        if df[name].dtype != np.bool_:
            raise ValueError(
                f"Field `{name}` is of type bool, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype.embedding_type.embedding_size > 0:
        if df[name].dtype != object:
            raise ValueError(
                f"Field `{name}` is of type embedding, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        # Check that the embedding is a list of floats of size embedding_size
        for i, row in df[name].items():
            if not isinstance(row, np.ndarray) and not isinstance(row, list):
                raise ValueError(
                    f"Field `{name}` is of type embedding, but the "
                    f"column in the dataframe is not a list. Error found during "
                    f"checking schema for `{entity_name}`."
                )
            if len(row) != dtype.embedding_type.embedding_size:
                raise ValueError(
                    f"Field `{name}` is of type embedding, of size "
                    f"`{dtype.embedding_type.embedding_size}`, but the "
                    "column in the dataframe has a list of size "
                    f"`{len(row)}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
    elif dtype.array_type.of != schema_proto.DataType():
        if df[name].dtype != object:
            raise ValueError(
                f"Field `{name}` is of type array, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            if not isinstance(row, np.ndarray) and not isinstance(row, list):
                raise ValueError(
                    f"Field `{name}` is of type array, but the "
                    f"column in the dataframe is not a list. Error found during "
                    f"checking schema for `{entity_name}`."
                )
    elif dtype.map_type.key != schema_proto.DataType():
        if df[name].dtype != object:
            raise ValueError(
                f"Field `{name}` is of type map, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            if not isinstance(row, dict):
                raise ValueError(
                    f"Field `{name}` is of type map, but the "
                    f"column in the dataframe is not a dict. Error found during "
                    f"checking schema for `{entity_name}`."
                )
    elif dtype.between_type != schema_proto.Between():
        bw_type = dtype.between_type
        if bw_type.dtype == schema_proto.DataType(
            int_type=schema_proto.IntType()
        ):
            if df[name].dtype != np.int64 and df[name].dtype != pd.Int64Dtype():
                raise ValueError(
                    f"Field `{name}` is of type int, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
            min_bound = bw_type.min.int
            max_bound = bw_type.max.int
        elif bw_type.dtype == schema_proto.DataType(
            double_type=schema_proto.DoubleType()
        ):
            if (
                df[name].dtype != np.float64
                and df[name].dtype != np.int64
                and df[name].dtype != pd.Int64Dtype()
                and df[name].dtype != pd.Float64Dtype()
            ):
                raise ValueError(
                    f"Field `{name}` is of type float, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
            min_bound = bw_type.min.float  # type: ignore
            max_bound = bw_type.max.float  # type: ignore
        else:
            raise TypeError("'between' type only accepts int or float types")
        for i, row in df[name].items():
            if (
                row < min_bound
                or row > max_bound
                or (bw_type.strict_min and row == min_bound)
                or (bw_type.strict_max and row == max_bound)
            ):
                raise ValueError(
                    f"Field `{name}` is of type between, but the "
                    f"value `{row}` is out of bounds. Error found during "
                    f"checking schema for `{entity_name}`."
                )
    elif dtype.one_of_type != schema_proto.OneOf():
        of_type = dtype.one_of_type
        if of_type.of == schema_proto.DataType(int_type=schema_proto.IntType()):
            if df[name].dtype != np.int64 and df[name].dtype != pd.Int64Dtype():
                raise ValueError(
                    f"Field `{name}` is of type int, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
            options = set(int(x.int) for x in of_type.options)
        elif of_type.of == schema_proto.DataType(
            string_type=schema_proto.StringType()
        ):
            if (
                df[name].dtype != object
                and df[name].dtype != np.str_
                and df[name].dtype != pd.StringDtype()
            ):
                raise ValueError(
                    f"Field `{name}` is of type str, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
            options = set(
                str(x.string) for x in of_type.options  # type: ignore
            )
        else:
            raise TypeError("oneof type only accepts int or str types")

        for i, row in df[name].items():
            if row not in options:
                sorted_options = sorted(options)
                raise ValueError(
                    f"Field '{name}' is of type oneof, but the "
                    f"value '{row}' is not found in the set of options "
                    f"{sorted_options}. Error found during "
                    f"checking schema for `{entity_name}`."
                )

    elif dtype.regex_type != "":
        if (
            df[name].dtype != object
            and df[name].dtype != np.str_
            and df[name].dtype != pd.StringDtype()
        ):
            raise ValueError(
                f"Field `{name}` is of type str, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        regex = dtype.regex_type.pattern
        for i, row in df[name].items():
            full_match = "^" + regex + "$"
            if not re.match(full_match, row):
                raise ValueError(
                    f"Field `{name}` is of type regex, but the "
                    f"value `{row}` does not match the regex "
                    f"`{regex}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
    else:
        raise ValueError(f"Field `{name}` has unknown data type `{dtype}`.")


def data_schema_check(
    schema: schema_proto.DSSchema, df: pd.DataFrame, dataset_name=""
) -> List[ValueError]:
    exceptions = []
    fields = []
    for key in schema.keys.fields:
        fields.append(key)

    for val in schema.values.fields:
        fields.append(val)

    if schema.timestamp != "":
        fields.append(
            schema_proto.Field(
                name=schema.timestamp,
                dtype=schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            )
        )

    # Check schema of fields with the dataframe
    for field in fields:
        try:
            _validate_field_in_df(field, df, dataset_name)
        except ValueError as e:
            exceptions.append(e)
        except Exception as e:
            raise e
    return exceptions


def inputs(*inps: Any):
    if len(inps) == 0:
        raise ValueError("No inputs specified")

    def decorator(func):
        setattr(func, FENNEL_INPUTS, inps)
        return func

    return decorator


def outputs(*outs: Any):
    if len(outs) == 0:
        raise ValueError("No outputs specified")

    def decorator(func):
        setattr(func, FENNEL_OUTPUTS, outs)
        return func

    return decorator
