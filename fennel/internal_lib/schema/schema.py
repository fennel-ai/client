from __future__ import annotations

import dataclasses
import re
import typing
from datetime import datetime, date
from decimal import Decimal as PythonDecimal
from typing import (
    Union,
    Any,
    List,
    get_type_hints,
    get_args,
    Type,
    Optional,
    Tuple,
)

import numpy as np
import pandas as pd
import pyarrow as pa
from frozendict import frozendict

import fennel.gen.schema_pb2 as schema_proto
from fennel.dtypes.dtypes import (
    between,
    oneof,
    regex,
    Decimal,
    _Embedding,
    _Decimal,
)
from fennel.internal_lib.utils.utils import get_origin, is_user_defined_class

FENNEL_STRUCT = "__fennel_struct__"
FENNEL_STRUCT_SRC_CODE = "__fennel_struct_src_code__"
FENNEL_STRUCT_DEPENDENCIES_SRC_CODE = "__fennel_struct_dependencies_src_code__"


def _get_args(type_: Any) -> Any:
    """Get the type arguments of a type."""
    return getattr(type_, "__args__", None)


def get_primitive_dtype(dtype):
    """Get the primitive type of a dtype."""
    if isinstance(dtype, oneof) or isinstance(dtype, between):
        return dtype.dtype
    if isinstance(dtype, regex):
        return pd.StringDtype
    if isinstance(dtype, _Decimal):
        return Decimal
    return dtype


# Parse a json object into a python object based on the type annotation.
def parse_json(annotation, json) -> Any:
    if annotation is Any:
        return json

    if isinstance(json, frozendict):
        json = dict(json)

    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        if origin is Union:
            if len(args) != 2 or type(None) not in args:
                raise TypeError(
                    f"Union must be of the form `Union[type, None]`, "
                    f"got `{annotation}`"
                )
            if json is None or pd.isna(json):
                return None
            return parse_json(args[0], json)
        if origin is list:
            if isinstance(json, np.ndarray):
                json = json.tolist()
            # Lookups can return None also in case of List[FennelStruct]
            if not isinstance(json, list) and pd.notna(json):
                raise TypeError(f"Expected list, got `{type(json).__name__}`")
            if not isinstance(json, list) and pd.isna(json):
                return None
            return [parse_json(args[0], x) for x in json]
        if origin is dict:
            if isinstance(json, list):
                return {k: parse_json(args[1], v) for k, v in json}
            # Lookups can return None also in case Dict[Any, FennelStruct]
            if not isinstance(json, dict) and pd.notna(json):
                raise TypeError(
                    f"Expected dict or list of pairs, got `{type(json).__name__}`"
                )
            if not isinstance(json, dict) and pd.isna(json):
                return None
            return {k: parse_json(args[1], v) for k, v in json.items()}
        raise TypeError(f"Unsupported type `{origin}`")
    else:
        if is_user_defined_class(annotation):
            if not isinstance(json, dict):
                return json
            fields = {f.name: f.type for f in dataclasses.fields(annotation)}
            return annotation(
                **{f: parse_json(t, json.get(f)) for f, t in fields.items()}
            )
        if annotation is datetime:
            if isinstance(json, str):
                return datetime.fromisoformat(json)
            elif isinstance(json, datetime):
                return json
            else:
                raise TypeError(
                    f"Expected datetime or str, got `{type(json).__name__}`"
                )
        if isinstance(annotation, _Decimal):
            scale = annotation.scale
            if (
                isinstance(json, PythonDecimal)
                or isinstance(annotation, float)
                or isinstance(annotation, int)
            ):
                return PythonDecimal("%0.{}f".format(scale) % json)
            else:
                raise TypeError(
                    f"Expected decimal or float, got `{type(json).__name__}`"
                )
        if annotation is np.ndarray:
            return np.array(json)
        if annotation is pd.DataFrame:
            return pd.DataFrame(json)
        if annotation is pd.Series:
            return pd.Series(json)
        if annotation is int:
            return int(json)
        if annotation is float:
            return float(json)
        return json


def fennel_is_optional(type_):
    return (
        typing.get_origin(type_) is Union
        and type(None) is typing.get_args(type_)[1]
    )


def fennel_get_optional_inner(type_):
    return typing.get_args(type_)[0]


def get_datatype(type_: Any) -> schema_proto.DataType:
    if fennel_is_optional(type_):
        dtype = get_datatype(_get_args(type_)[0])
        return schema_proto.DataType(
            optional_type=schema_proto.OptionalType(of=dtype)
        )
    elif type_ is int or type_ is np.int64 or type_ == pd.Int64Dtype:
        return schema_proto.DataType(int_type=schema_proto.IntType())
    elif type_ is float or type_ is np.float64 or type_ == pd.Float64Dtype:
        return schema_proto.DataType(double_type=schema_proto.DoubleType())
    elif type_ is str or type_ is np.str_ or type_ == pd.StringDtype:
        return schema_proto.DataType(string_type=schema_proto.StringType())
    elif type_ is bytes or type_ is np.bytes_:
        return schema_proto.DataType(bytes_type=schema_proto.BytesType())
    elif type_ is datetime or type_ is np.datetime64:
        return schema_proto.DataType(
            timestamp_type=schema_proto.TimestampType()
        )
    elif type_ is date:
        return schema_proto.DataType(date_type=schema_proto.DateType())
    elif type_ is bool or type_ == pd.BooleanDtype:
        return schema_proto.DataType(bool_type=schema_proto.BoolType())
    elif get_origin(type_) is list:
        return schema_proto.DataType(
            array_type=schema_proto.ArrayType(
                of=get_datatype(_get_args(type_)[0])
            )
        )
    elif get_origin(type_) is dict:
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
    elif isinstance(type_, _Decimal):
        return schema_proto.DataType(
            decimal_type=schema_proto.DecimalType(scale=type_.scale)
        )
    elif is_user_defined_class(type_):
        # Iterate through all the fields in the class and get the schema for
        # each field
        fields = []
        for field_name, field_type in get_type_hints(type_).items():
            fields.append(
                schema_proto.Field(
                    name=field_name,
                    dtype=get_datatype(field_type),
                )
            )
        return schema_proto.DataType(
            struct_type=schema_proto.StructType(
                fields=fields, name=type_.__name__
            )
        )
    raise ValueError(f"Cannot serialize type {type_}.")


def get_pd_dtype(type: Type):
    """
    Convert int -> Int64, float -> Float64 and string -> String, bool -> Bool
    """
    if type == int:
        return pd.Int64Dtype
    elif type == float:
        return pd.Float64Dtype
    elif type == str:
        return pd.StringDtype
    elif type == bool:
        return pd.BooleanDtype
    elif fennel_is_optional(type):
        return Optional[get_pd_dtype(fennel_get_optional_inner(type))]
    else:
        return type


def get_python_type_from_pd(type):
    if type == pd.Int64Dtype:
        return int
    elif type == pd.Float64Dtype:
        return float
    elif type == pd.StringDtype:
        return str
    elif type == pd.BooleanDtype:
        return bool
    elif fennel_is_optional(type):
        return Optional[
            get_python_type_from_pd(fennel_get_optional_inner(type))
        ]
    return type


def convert_dtype_to_arrow_type(dtype: schema_proto.DataType) -> pa.DataType:
    if dtype.HasField("optional_type"):
        return convert_dtype_to_arrow_type(dtype.optional_type.of)
    elif dtype.HasField("int_type"):
        return pa.int64()
    elif dtype.HasField("double_type"):
        return pa.float64()
    elif dtype.HasField("string_type") or dtype.HasField("regex_type"):
        return pa.string()
    elif dtype.HasField("bytes_type"):
        return pa.binary()
    elif dtype.HasField("bool_type"):
        return pa.bool_()
    elif dtype.HasField("timestamp_type"):
        return pa.timestamp("ns", "UTC")
    elif dtype.HasField("date_type"):
        return pa.date32()
    elif dtype.HasField("decimal_type"):
        return pa.decimal128(28, dtype.decimal_type.scale)
    elif dtype.HasField("array_type"):
        return pa.list_(
            value_type=convert_dtype_to_arrow_type(dtype.array_type.of)
        )
    elif dtype.HasField("map_type"):
        key_pa_type = convert_dtype_to_arrow_type(dtype.map_type.key)
        value_pa_type = convert_dtype_to_arrow_type(dtype.map_type.value)
        return pa.map_(key_pa_type, value_pa_type, False)
    elif dtype.HasField("embedding_type"):
        embedding_size = dtype.embedding_type.embedding_size
        return pa.list_(pa.float64(), embedding_size)
    elif dtype.HasField("one_of_type"):
        return convert_dtype_to_arrow_type(dtype.one_of_type.of)
    elif dtype.HasField("between_type"):
        return convert_dtype_to_arrow_type(dtype.between_type.dtype)
    elif dtype.HasField("struct_type"):
        fields: List[Tuple[str, pa.DataType]] = []
        for field in dtype.struct_type.fields:
            fields.append(
                (field.name, convert_dtype_to_arrow_type(field.dtype))
            )
        return pa.struct(fields)
    else:
        raise TypeError(f"Invalid dtype: {dtype}.")


def validate_val_with_dtype(dtype: Type, val):
    proto_dtype = get_datatype(dtype)
    validate_val_with_proto_dtype(proto_dtype, val)


def validate_val_with_proto_dtype(dtype: schema_proto.DataType, val):
    """
    The function validates that the value is of the correct type for the given dtype.

    :param dtype:
    :param val:
    :return:
    """
    if dtype.HasField("optional_type"):
        try:
            if not isinstance(
                val, (list, tuple, dict, set, np.ndarray, frozendict)
            ) and pd.isna(val):
                return
            else:
                return validate_val_with_proto_dtype(
                    dtype.optional_type.of, val
                )
        # ValueError error occurs when you do something like pd.notna([1, 2, None])
        except ValueError:
            return validate_val_with_proto_dtype(dtype.optional_type.of, val)
    elif dtype == schema_proto.DataType(int_type=schema_proto.IntType()):
        if type(val) is not int and type(val) is not np.int64:
            raise ValueError(
                f"Expected type int, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(double_type=schema_proto.DoubleType()):
        if type(val) is not float and type(val) is not np.float64:
            raise ValueError(
                f"Expected type float, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(string_type=schema_proto.StringType()):
        if type(val) is not str:
            raise ValueError(
                f"Expected type str, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(
        timestamp_type=schema_proto.TimestampType()
    ):
        if type(val) not in [datetime, pd.Timestamp, np.datetime64]:
            raise ValueError(
                f"Expected type datetime, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(date_type=schema_proto.DateType()):
        if type(val) is not date:
            raise ValueError(
                f"Expected type date, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(bool_type=schema_proto.BoolType()):
        if type(val) is not bool:
            raise ValueError(
                f"Expected type bool, got {type(val)} for value {val}"
            )
    elif dtype.embedding_type.embedding_size > 0:
        if type(val) not in [np.ndarray, list]:
            raise ValueError(
                f"Expected type np.ndarray, got {type(val)} for value {val}"
            )
        if len(val) != dtype.embedding_type.embedding_size:
            raise ValueError(
                f"Expected embedding of size {dtype.embedding_type.embedding_size}, got {len(val)} for value {val}"
            )
    elif dtype.array_type.of != schema_proto.DataType():
        if type(val) not in [np.ndarray, list]:
            raise ValueError(
                f"Expected type list, got {type(val)} for value {val}"
            )
        # Recursively check the type of each element in the list
        for v in val:
            validate_val_with_proto_dtype(dtype.array_type.of, v)
    elif dtype.map_type.key != schema_proto.DataType():
        if type(val) not in [dict, np.ndarray, list, frozendict]:
            raise ValueError(
                f"Expected type dict/list[tuple], got {type(val)} for value {val}"
            )
        # Recursively check the type of each element in the dict
        # Check that all keys are strings
        if type(val) is dict:
            for k in val.keys():
                if type(k) is not str:
                    raise ValueError(
                        f"Expected type str, got {type(k)} for key {k}"
                    )
            for v in val.values():
                validate_val_with_proto_dtype(dtype.map_type.value, v)
        else:
            for key, value in val:
                if type(key) is not str:
                    raise ValueError(
                        f"Expected type str, got {type(key)} for key {key}"
                    )
                validate_val_with_proto_dtype(dtype.map_type.value, value)
    elif dtype.between_type != schema_proto.Between():
        bw_type = dtype.between_type
        min_bound = None
        max_bound = None
        if bw_type.dtype == schema_proto.DataType(
            int_type=schema_proto.IntType()
        ):
            if type(val) is not int:
                raise ValueError(
                    f"Expected type int, got {type(val)} for value {val}"
                )
            min_bound = bw_type.min.int
            max_bound = bw_type.max.int
        elif bw_type.dtype == schema_proto.DataType(
            double_type=schema_proto.DoubleType()
        ):
            if type(val) is not float:
                raise ValueError(
                    f"Expected type float, got {type(val)} for value {val}"
                )
            min_bound = bw_type.min.float
            max_bound = bw_type.max.float
        if min_bound > val or max_bound < val:
            raise ValueError(
                f"Value {val} is out of bounds for between type {dtype}, bounds are"
                f"[{min_bound}, {max_bound}]"
            )
    elif dtype.one_of_type != schema_proto.OneOf():
        of_type = dtype.one_of_type
        if of_type.of == schema_proto.DataType(int_type=schema_proto.IntType()):
            if type(val) is not int:
                raise ValueError(
                    f"Expected type int, got {type(val)} for value {val}"
                )
            if val not in [int(x.int) for x in of_type.options]:
                raise ValueError(
                    f"Value {val} is not in options {of_type.options} for oneof type {dtype}"
                )
        elif of_type.of == schema_proto.DataType(
            string_type=schema_proto.StringType()
        ):
            if type(val) is not str:
                raise ValueError(
                    f"Expected type str, got {type(val)} for value {val}"
                )
            if val not in [str(x.string) for x in of_type.options]:
                raise ValueError(
                    f"Value {val} is not in options {of_type.options} for oneof type {dtype}"
                )
    elif dtype.regex_type != schema_proto.RegexType():
        if type(val) is not str:
            raise ValueError(
                f"Expected type str, got {type(val)} for value {val}"
            )
        if not re.match(dtype.regex_type.pattern, val):
            raise ValueError(
                f"Value {val} does not match regex {dtype.regex_type.pattern}"
            )
    elif dtype.struct_type != schema_proto.StructType():
        if type(val) is not dict:
            # TODO(Aditya): Actually compare the structs
            return
        # Recursively check the type of each element in the dict
        for field in dtype.struct_type.fields:
            if field.name not in val:
                raise ValueError(
                    f"Field {field.name} not found in struct {dtype}"
                )
            validate_val_with_proto_dtype(field.dtype, val[field.name])
    elif dtype.decimal_type != schema_proto.DecimalType():
        if (
            not isinstance(val, PythonDecimal)
            and not isinstance(val, float)
            and not isinstance(val, int)
        ):
            raise ValueError(
                f"Expected type python Decimal or float or int, got `{type(val).__name__}` for value `{val}`"
            )
    else:
        raise ValueError(f"Unsupported dtype {dtype}")


def validate_field_in_df(
    field: schema_proto.Field,
    df: pd.DataFrame,
    entity_name: str,
    is_nullable: bool = False,
):
    name = field.name
    dtype = field.dtype
    arrow_type = convert_dtype_to_arrow_type(dtype)
    if df.shape[0] == 0:
        return
    if name not in df.columns:
        raise ValueError(
            f"Field `{name}` not found in dataframe during checking schema for "
            f"`{entity_name}`. "
            f"Please ensure the dataframe has the correct schema."
        )

    # Check for the optional type
    if dtype.optional_type != schema_proto.OptionalType():
        return validate_field_in_df(
            field=schema_proto.Field(name=name, dtype=dtype.optional_type.of),
            df=df,
            entity_name=entity_name,
            is_nullable=True,
        )
    if not is_nullable and df[name].isnull().any():
        raise ValueError(
            f"Field `{name}` is not nullable, but the "
            f"column in the dataframe has null values. Error found during "
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
                and df[name].dtype != pd.ArrowDtype(arrow_type)
            ):
                raise ValueError(
                    f"Field `{name}` is of type int, but the "
                    f"column in the dataframe is of type "
                    f"`{df[name].dtype}`. Error found during "
                    f"checking schema for `{entity_name}`."
                )
        else:
            if (
                df[name].dtype != np.int64
                and df[name].dtype != pd.Int64Dtype()
                and df[name].dtype != pd.ArrowDtype(arrow_type)
            ):
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
            and df[name].dtype != pd.ArrowDtype(arrow_type)
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
            and df[name].dtype != pd.ArrowDtype(pa.string())
        ):
            raise ValueError(
                f"Field `{name}` is of type str, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype == schema_proto.DataType(bytes_type=schema_proto.BytesType()):
        if (
            df[name].dtype != object
            and df[name].dtype != np.bytes_
            and df[name].dtype != pd.ArrowDtype(pa.binary())
        ):
            raise ValueError(
                f"Field `{name}` is of type bytes, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype == schema_proto.DataType(
        timestamp_type=schema_proto.TimestampType()
    ):
        if not (
            str(df[name].dtype) == "datetime64[ns, UTC]"
            or str(df[name].dtype) == "datetime64[us, UTC]"
        ) and df[name].dtype != pd.ArrowDtype(arrow_type):
            raise ValueError(
                f"Field `{name}` is of type timestamp, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype == schema_proto.DataType(date_type=schema_proto.DateType()):
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type date, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            validate_val_with_proto_dtype(dtype, row)
    elif dtype == schema_proto.DataType(bool_type=schema_proto.BoolType()):
        if (
            df[name].dtype != np.bool_
            and df[name].dtype != pd.BooleanDtype()
            and df[name].dtype != pd.ArrowDtype(arrow_type)
        ):
            raise ValueError(
                f"Field `{name}` is of type bool, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
    elif dtype.embedding_type.embedding_size > 0:
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type embedding, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        # Check that the embedding is a list of floats of size embedding_size
        for i, row in df[name].items():
            validate_val_with_proto_dtype(dtype, row)
    elif dtype.array_type.of != schema_proto.DataType():
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type array, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            validate_val_with_proto_dtype(dtype, row)
    elif dtype.map_type.key != schema_proto.DataType():
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type map, but the "
                f"column in the dataframe is of type "
                f"`{df[name].dtype}`. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            validate_val_with_proto_dtype(dtype, row)

    elif dtype.between_type != schema_proto.Between():
        bw_type = dtype.between_type
        if bw_type.dtype == schema_proto.DataType(
            int_type=schema_proto.IntType()
        ):
            if (
                df[name].dtype != np.int64
                and df[name].dtype != pd.Int64Dtype()
                and df[name].dtype != pd.ArrowDtype(arrow_type)
            ):
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
                and df[name].dtype != pd.ArrowDtype(arrow_type)
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
            if (
                df[name].dtype != np.int64
                and df[name].dtype != pd.Int64Dtype()
                and df[name].dtype != pd.ArrowDtype(arrow_type)
            ):
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
                and df[name].dtype != pd.ArrowDtype(arrow_type)
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
    elif dtype.regex_type.pattern != "":
        if (
            df[name].dtype != object
            and df[name].dtype != np.str_
            and df[name].dtype != pd.StringDtype()
            and df[name].dtype != pd.ArrowDtype(pa.string())
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
    elif dtype.struct_type.name != "":
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type struct, but the "
                f"column in the dataframe is not a dict. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            # Recursively check the type of each element in the dict
            if type(row) is dict:
                validate_val_with_proto_dtype(field.dtype, row)
            # TODO(Aditya) : Fix the non dict case
    elif dtype.decimal_type.scale != 0:
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type decimal, but the "
                f"column in the dataframe is not a decimal. Error found during "
                f"checking schema for `{entity_name}`."
            )
        # Recursively check the type of each element in the column
        for i, row in df[name].items():
            validate_val_with_proto_dtype(field.dtype, row)
    else:
        raise ValueError(f"Field `{name}` has unknown data type `{dtype}`.")


def is_hashable(dtype: Any) -> bool:
    primitive_type = get_primitive_dtype(dtype)
    # typing.Optional[x] is an alias for typing.Union[x, None]
    if (
        get_origin(primitive_type) is Union
        and type(None) is _get_args(primitive_type)[1]
    ):
        return is_hashable(_get_args(primitive_type)[0])
    elif primitive_type in [
        int,
        str,
        bool,
        pd.Int64Dtype,
        pd.StringDtype,
        pd.BooleanDtype,
        Decimal,
    ]:
        return True
    elif get_origin(primitive_type) is list:
        return is_hashable(_get_args(primitive_type)[0])
    elif get_origin(primitive_type) is dict:
        if _get_args(primitive_type)[0] is not str:
            raise ValueError("Dict keys must be strings.")
        return is_hashable(_get_args(primitive_type)[1])
    elif (
        isinstance(primitive_type, between)
        or isinstance(primitive_type, oneof)
        or isinstance(primitive_type, regex)
    ):
        return is_hashable(_get_args(primitive_type)[0])
    return False


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
            validate_field_in_df(field, df, dataset_name)
        except ValueError as e:
            exceptions.append(e)
        except Exception as e:
            raise e
    return exceptions
