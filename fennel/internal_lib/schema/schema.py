from __future__ import annotations

import dataclasses
import re
import typing
from datetime import datetime, date
from decimal import Decimal as PythonDecimal
from typing import Dict
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
from fennel.internal_lib.utils.utils import (
    get_origin,
    is_user_defined_class,
    parse_struct_into_dict,
    parse_datetime_in_value,
)

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

    # If json is not an array
    if (
        json is None
        or json is pd.NA
        or (isinstance(json, (int, float)) and np.isnan(json))
    ):
        return pd.NA

    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        if origin is Union:
            if len(args) != 2 or type(None) not in args:
                raise TypeError(
                    f"Union must be of the form `Union[type, None]`, "
                    f"got `{annotation}`"
                )
            if json is None or (
                isinstance(json, (int, float)) and pd.isna(json)
            ):
                return pd.NA
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


def convert_dtype_to_arrow_type_with_nullable(
    dtype: schema_proto.DataType, nullable: bool = False
) -> pa.DataType:
    if dtype.HasField("optional_type"):
        return convert_dtype_to_arrow_type_with_nullable(
            dtype.optional_type.of, True
        )
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
            value_type=convert_dtype_to_arrow_type_with_nullable(
                dtype.array_type.of, nullable
            )
        )
    elif dtype.HasField("map_type"):
        key_pa_type = convert_dtype_to_arrow_type_with_nullable(
            dtype.map_type.key, nullable=False
        )
        value_pa_type = convert_dtype_to_arrow_type_with_nullable(
            dtype.map_type.value, nullable
        )
        return pa.map_(key_pa_type, value_pa_type, False)
    elif dtype.HasField("embedding_type"):
        embedding_size = dtype.embedding_type.embedding_size
        return pa.list_(pa.float64(), embedding_size)
    elif dtype.HasField("one_of_type"):
        return convert_dtype_to_arrow_type_with_nullable(
            dtype.one_of_type.of, nullable
        )
    elif dtype.HasField("between_type"):
        return convert_dtype_to_arrow_type_with_nullable(
            dtype.between_type.dtype, nullable
        )
    elif dtype.HasField("struct_type"):
        fields: List[Tuple[str, pa.DataType]] = []
        for field in dtype.struct_type.fields:
            inner = convert_dtype_to_arrow_type_with_nullable(
                field.dtype, nullable
            )
            field = pa.field(field.name, inner, nullable)
            fields.append(field)
        return pa.struct(fields)
    else:
        raise TypeError(f"Invalid dtype: {dtype}.")


def convert_dtype_to_arrow_type(dtype: schema_proto.DataType) -> pa.DataType:
    return convert_dtype_to_arrow_type_with_nullable(dtype)


def check_val_is_null(val: Any) -> bool:
    if isinstance(val, (list, tuple, dict, set, np.ndarray, frozendict)):
        return False
    try:
        if pd.isna(val):
            return True
        else:
            return False
    except ValueError:
        return False


def validate_val_with_dtype(dtype: Type, val):
    proto_dtype = get_datatype(dtype)
    validate_val_with_proto_dtype(proto_dtype, val)


def validate_val_with_proto_dtype(
    dtype: schema_proto.DataType, val: Any, nullable: bool = False
):
    """
    The function validates that the value is of the correct type for the given dtype.

    :param dtype: Proto Dtype of the val
    :param val: Value
    :param nullable: If value can be nullable or not
    :return:
    """
    if nullable and check_val_is_null(val):
        return True
    if dtype.HasField("optional_type"):
        return validate_val_with_proto_dtype(dtype.optional_type.of, val, True)
    elif dtype == schema_proto.DataType(int_type=schema_proto.IntType()):
        if not isinstance(val, (int, np.int64)):
            raise ValueError(
                f"Expected type int, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(double_type=schema_proto.DoubleType()):
        if not isinstance(val, (float, np.float64, np.float32, np.float16)):
            raise ValueError(
                f"Expected type float, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(string_type=schema_proto.StringType()):
        if not isinstance(val, str):
            raise ValueError(
                f"Expected type str, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(
        timestamp_type=schema_proto.TimestampType()
    ):
        if not isinstance(val, (datetime, pd.Timestamp, np.datetime64, int)):
            raise ValueError(
                f"Expected type datetime, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(date_type=schema_proto.DateType()):
        if not isinstance(val, date):
            raise ValueError(
                f"Expected type date, got {type(val)} for value {val}"
            )
    elif dtype == schema_proto.DataType(bool_type=schema_proto.BoolType()):
        if not isinstance(val, bool):
            raise ValueError(
                f"Expected type bool, got {type(val)} for value {val}"
            )
    elif dtype.embedding_type.embedding_size > 0:
        if not isinstance(val, (np.ndarray, list)):
            raise ValueError(
                f"Expected type np.ndarray, got {type(val)} for value {val}"
            )
        if len(val) != dtype.embedding_type.embedding_size:
            raise ValueError(
                f"Expected embedding of size {dtype.embedding_type.embedding_size}, got {len(val)} for value {val}"
            )
    elif dtype.array_type.of != schema_proto.DataType():
        if not isinstance(val, (np.ndarray, list)):
            raise ValueError(
                f"Expected type list, got {type(val)} for value {val}"
            )
        # Recursively check the type of each element in the list
        for v in val:
            validate_val_with_proto_dtype(dtype.array_type.of, v)
    elif dtype.map_type.key != schema_proto.DataType():
        if not isinstance(val, (dict, np.ndarray, list, frozendict)):
            raise ValueError(
                f"Expected type dict/list[tuple], got {type(val)} for value {val}"
            )
        # Recursively check the type of each element in the dict
        # Check that all keys are strings
        if isinstance(val, (dict, frozendict)):
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
            if not isinstance(val, (int, np.int64)):
                raise ValueError(
                    f"Expected type int, got {type(val)} for value {val}"
                )
            min_bound = bw_type.min.int
            max_bound = bw_type.max.int
        elif bw_type.dtype == schema_proto.DataType(
            double_type=schema_proto.DoubleType()
        ):
            if not isinstance(val, (float, np.float64)):
                raise ValueError(
                    f"Expected type float, got {type(val)} for value {val}"
                )
            min_bound = bw_type.min.float
            max_bound = bw_type.max.float
        if min_bound > val or max_bound < val:
            raise ValueError(
                f"Value {val} is out of bounds for between type, bounds are "
                f"[{min_bound}, {max_bound}]"
            )
    elif dtype.one_of_type != schema_proto.OneOf():
        of_type = dtype.one_of_type
        if of_type.of == schema_proto.DataType(int_type=schema_proto.IntType()):
            if not isinstance(val, (int, np.int64)):
                raise ValueError(
                    f"Expected type int, got {type(val)} for value {val}"
                )
            if val not in [int(x.int) for x in of_type.options]:
                raise ValueError(
                    f"Value {val} is not in options {[x.int for x in of_type.options]}"
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
                    f"Value {val} is not in options {[x.string for x in of_type.options]}"
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
        if hasattr(val, FENNEL_STRUCT):
            val = val.as_json()
        if not isinstance(val, (dict, frozendict)):
            raise ValueError(
                f"Expected type fennel struct or dict, got `{type(val).__name__}` for value `{val}`"
            )
        # Recursively check the type of each element in the dict
        for field in dtype.struct_type.fields:
            if field.name not in val:
                raise ValueError(
                    f"Field {field.name} not found in struct {dtype}"
                )
            validate_val_with_proto_dtype(field.dtype, val[field.name])
    elif dtype.decimal_type != schema_proto.DecimalType():
        if not isinstance(val, (PythonDecimal, float, int)):
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
        for i, row in df[name].items():
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(
                    f"Field `{name}` is of type embedding, but {e}."
                )
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
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(f"Field `{name}` is of type array, but {e}.")
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
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(f"Field `{name}` is of type map, but {e}.")
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
        else:
            raise TypeError("'between' type only accepts int or float types")
        for i, row in df[name].items():
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(f"Field `{name}` is of type between, but {e}.")
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
        else:
            raise TypeError("oneof type only accepts int or str types")
        for i, row in df[name].items():
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(f"Field `{name}` is of type oneof, but {e}.")
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
        for i, row in df[name].items():
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(
                    f"Field `{name}` is of type regex[str], but {e}."
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
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(f"Field `{name}` is of type struct, but {e}.")
    elif dtype.decimal_type.scale != 0:
        if df[name].dtype != object and df[name].dtype != pd.ArrowDtype(
            arrow_type
        ):
            raise ValueError(
                f"Field `{name}` is of type decimal, but the "
                f"column in the dataframe is not a decimal. Error found during "
                f"checking schema for `{entity_name}`."
            )
        for i, row in df[name].items():
            try:
                validate_val_with_proto_dtype(dtype, row, is_nullable)
            except ValueError as e:
                raise ValueError(f"Field `{name}` is of type decimal, but {e}.")
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


def from_proto(data_type: schema_proto.DataType) -> Any:
    field = data_type.WhichOneof("dtype")
    if field == "int_type":
        return int
    elif field == "double_type":
        return float
    elif field == "string_type":
        return str
    elif field == "bool_type":
        return bool
    elif field == "timestamp_type":
        return datetime
    elif field == "date_type":
        return date
    elif field == "bytes_type":
        return bytes
    elif field == "array_type":
        element_type = from_proto(data_type.array_type.of)
        return List[element_type]  # type: ignore
    elif field == "map_type":
        key_type = from_proto(data_type.map_type.key)
        value_type = from_proto(data_type.map_type.value)
        return Dict[key_type, value_type]  # type: ignore
    elif field == "struct_type":
        fields = [
            (f.name, from_proto(f.dtype)) for f in data_type.struct_type.fields
        ]
        # Dynamically create the dataclass with the specified fields
        return dataclasses.make_dataclass(data_type.struct_type.name, fields)
    elif field == "optional_type":
        return Optional[from_proto(data_type.optional_type.of)]
    elif field == "decimal_type":
        return Decimal
    elif field == "regex_type":
        return regex(data_type.regex_type.pattern)
    elif field == "embedding_type":
        return _Embedding(data_type.embedding_type.embedding_size)
    elif field == "between_type":
        dtype = from_proto(data_type.between_type.dtype)
        min_value = data_type.between_type.min
        max_value = data_type.between_type.max
        min = min_value.int if hasattr(min_value, "int") else min_value.float
        max = max_value.int if hasattr(max_value, "int") else max_value.float
        return between(
            dtype=dtype,
            min=min,
            max=max,
            strict_min=data_type.between_type.strict_min,
            strict_max=data_type.between_type.strict_max,
        )
    elif field == "one_of_type":
        dtype = from_proto(data_type.one_of_type.of)
        options = [
            option.int if hasattr(option, "int") else option.string
            for option in data_type.one_of_type.options
        ]
        return oneof(dtype=dtype, options=options)
    else:
        raise ValueError(f"Unsupported data type field: {field}")


def cast_col_to_arrow_dtype(
    series: pd.Series, dtype: schema_proto.DataType
) -> pd.Series:
    """
    This function casts dtype of pd.Series object into pd.ArrowDtype depending on the DataType proto.
    """
    if not dtype.HasField("optional_type"):
        if series.isnull().any():
            raise ValueError("Null values found in non-optional field.")

    # Let's convert structs into json, this is done because arrow
    # dtype conversion fails with fennel struct
    # Parse datetime values
    if check_dtype_has_struct_type(dtype):
        series = series.apply(lambda x: parse_struct_into_dict(x, dtype))
    series = series.apply(lambda x: parse_datetime_in_value(x, dtype))
    arrow_type = convert_dtype_to_arrow_type_with_nullable(dtype)
    return series.astype(pd.ArrowDtype(arrow_type))


def check_dtype_has_struct_type(dtype: schema_proto.DataType) -> bool:
    if dtype.HasField("struct_type"):
        return True
    elif dtype.HasField("optional_type"):
        return check_dtype_has_struct_type(dtype.optional_type.of)
    elif dtype.HasField("array_type"):
        return check_dtype_has_struct_type(dtype.array_type.of)
    elif dtype.HasField("map_type"):
        return check_dtype_has_struct_type(dtype.map_type.value)
    return False
