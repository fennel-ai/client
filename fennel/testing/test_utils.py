import json
from math import isnan
from typing import Any, Union, List

import numpy as np
import pandas as pd
import pyarrow as pa
from frozendict import frozendict
from google.protobuf.json_format import MessageToDict

from fennel._vendor import jsondiff  # type: ignore
from fennel._vendor.requests import Response  # type: ignore
from fennel.dtypes.dtypes import FENNEL_STRUCT
from fennel.gen.dataset_pb2 import Operator, Filter, Transform, Assign
from fennel.gen.featureset_pb2 import Extractor
from fennel.gen.pycode_pb2 import PyCode
from fennel.gen.schema_pb2 import DSSchema, DataType, Field
from fennel.internal_lib.schema import convert_dtype_to_arrow_type
from fennel.internal_lib.utils import parse_datetime

FENNEL_DELETE_TIMESTAMP = "__fennel_delete_timestamp__"
FENNEL_LOOKUP = "__fennel_lookup_exists__"
FENNEL_ORDER = "__fennel_order__"
FENNEL_TIMESTAMP = "__fennel_timestamp__"


class FakeResponse(Response):
    def __init__(self, status_code: int, content: str):
        self.status_code = status_code

        self.encoding = "utf-8"
        if status_code == 200:
            self._ok = True
            self._content = json.dumps({}).encode("utf-8")
            return
        self._content = json.dumps({"error": f"{content}"}, indent=2).encode(
            "utf-8"
        )


def error_message(actual: Any, expected: Any) -> str:
    expected_dict = MessageToDict(expected)
    actual_dict = MessageToDict(actual)
    # Don't delete the following line. It is used to debug the test in
    # case of failure.
    print(actual_dict)
    return jsondiff.diff(expected_dict, actual_dict, syntax="symmetric")


def erase_extractor_pycode(extractor: Extractor) -> Extractor:
    new_extractor = Extractor(
        name=extractor.name,
        version=extractor.version,
        datasets=extractor.datasets,
        inputs=extractor.inputs,
        features=extractor.features,
        metadata=extractor.metadata,
        feature_set_name=extractor.feature_set_name,
        pycode=PyCode(),
    )
    return new_extractor


def erase_operator_pycode(operator: Operator) -> Operator:
    if operator.HasField("filter"):
        return Operator(
            id=operator.id,
            name=operator.name,
            pipeline_name=operator.pipeline_name,
            dataset_name=operator.dataset_name,
            is_root=operator.is_root,
            filter=Filter(
                operand_id=operator.filter.operand_id,
                pycode=PyCode(source_code=""),
            ),
        )
    if operator.HasField("transform"):
        return Operator(
            id=operator.id,
            name=operator.name,
            pipeline_name=operator.pipeline_name,
            dataset_name=operator.dataset_name,
            is_root=operator.is_root,
            transform=Transform(
                operand_id=operator.transform.operand_id,
                schema=operator.transform.schema,
                pycode=PyCode(source_code=""),
            ),
        )
    if operator.HasField("assign"):
        return Operator(
            id=operator.id,
            name=operator.name,
            pipeline_name=operator.pipeline_name,
            dataset_name=operator.dataset_name,
            is_root=operator.is_root,
            assign=Assign(
                operand_id=operator.assign.operand_id,
                column_name=operator.assign.column_name,
                output_type=operator.assign.output_type,
                pycode=PyCode(source_code=""),
            ),
        )
    raise ValueError(f"Operator {operator} has no pycode field")


def almost_equal(a: float, b: float, epsilon: float = 1e-6) -> bool:
    if isnan(a) and isnan(b):
        return True
    return abs(a - b) < epsilon


def check_dtype_has_struct_type(dtype: DataType) -> bool:
    if dtype.HasField("struct_type"):
        return True
    elif dtype.HasField("optional_type"):
        return check_dtype_has_struct_type(dtype.optional_type.of)
    elif dtype.HasField("array_type"):
        return check_dtype_has_struct_type(dtype.array_type.of)
    elif dtype.HasField("map_type"):
        return check_dtype_has_struct_type(dtype.map_type.value)
    return False


def parse_struct_into_dict(value: Any) -> Union[dict, list]:
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
        return [parse_struct_into_dict(x) for x in value]
    elif isinstance(value, dict) or isinstance(value, frozendict):
        return {key: parse_struct_into_dict(val) for key, val in value.items()}
    else:
        return value


def cast_col_to_arrow_dtype(series: pd.Series, dtype: DataType) -> pd.Series:
    """
    This function casts dtype of pd.Series object into pd.ArrowDtype depending on the DataType proto.
    """
    if not dtype.HasField("optional_type"):
        if series.isnull().any():
            raise ValueError("Null values found in non-optional field.")

    # Let's convert structs into json, this is done because arrow
    # dtype conversion fails with fennel struct
    if check_dtype_has_struct_type(dtype):
        series = series.apply(lambda x: parse_struct_into_dict(x))
    arrow_type = convert_dtype_to_arrow_type(dtype)
    return series.astype(pd.ArrowDtype(arrow_type))


def cast_col_to_pandas_dtype(
    series: pd.Series, dtype: DataType, nullable: bool = False
) -> pd.Series:
    """
    This function casts dtype of pd.Series having dtype as pd.ArrowDtype into pandas dtype
    depending on the DataType proto.
    """
    if dtype.HasField("optional_type"):
        return cast_col_to_pandas_dtype(series, dtype.optional_type.of, True)
    elif dtype.HasField("int_type"):
        return series.astype(pd.Int64Dtype())
    elif dtype.HasField("double_type"):
        return series.astype(pd.Float64Dtype())
    elif dtype.HasField("string_type") or dtype.HasField("regex_type"):
        return series.astype(pd.StringDtype())
    elif dtype.HasField("bytes_type"):
        return series.astype(object)
    elif dtype.HasField("bool_type"):
        return series.astype(pd.BooleanDtype())
    elif dtype.HasField("timestamp_type"):
        return pd.to_datetime(series.apply(parse_datetime), utc=True)
    elif dtype.HasField("one_of_type"):
        return cast_col_to_pandas_dtype(series, dtype.one_of_type.of, nullable)
    elif dtype.HasField("between_type"):
        return cast_col_to_pandas_dtype(
            series, dtype.between_type.dtype, nullable
        )
    elif (
        dtype.HasField("array_type")
        or dtype.HasField("map_type")
        or dtype.HasField("struct_type")
        or dtype.HasField("embedding_type")
    ):
        return series.apply(
            lambda x: convert_val_to_pandas_dtype(x, dtype, nullable)
        ).astype(object)
    else:
        return series


def convert_val_to_pandas_dtype(
    value: Any, data_type: DataType, nullable: bool
) -> Any:
    """
    This function casts value coming from a pd.Series having dtype as pd.ArrowDtype into pandas dtype
    depending on the DataType proto.
    """
    if nullable:
        try:
            if not isinstance(
                value, (list, tuple, dict, set, np.ndarray)
            ) and pd.isna(value):
                return pd.NA
        # ValueError error occurs when you do something like pd.isnull([1, 2, None])
        except ValueError:
            pass

    if data_type.HasField("optional_type"):
        return convert_val_to_pandas_dtype(
            value, data_type.optional_type.of, True
        )
    elif data_type.HasField("int_type"):
        return int(value)
    elif data_type.HasField("double_type"):
        return float(value)
    elif data_type.HasField("string_type") or data_type.HasField("regex_type"):
        return str(value)
    elif data_type.HasField("bytes_type"):
        return bytes(value)
    elif data_type.HasField("bool_type"):
        return bool(value)
    elif data_type.HasField("timestamp_type"):
        return parse_datetime(value)
    elif data_type.HasField("decimal_type"):
        return value
    elif data_type.HasField("between_type"):
        return convert_val_to_pandas_dtype(
            value, data_type.between_type.dtype, nullable
        )
    elif data_type.HasField("one_of_type"):
        return convert_val_to_pandas_dtype(
            value, data_type.one_of_type.of, nullable
        )
    elif data_type.HasField("map_type"):
        return {
            val[0]: convert_val_to_pandas_dtype(
                val[1], data_type.map_type.value, False
            )
            for val in value
        }
    elif data_type.HasField("embedding_type"):
        return value.tolist() if isinstance(value, np.ndarray) else list(value)
    elif data_type.HasField("array_type"):
        return [
            convert_val_to_pandas_dtype(x, data_type.array_type.of, False)
            for x in value
        ]
    elif data_type.HasField("struct_type"):
        fields = data_type.struct_type.fields
        output = {}
        for field in fields:
            output[field.name] = convert_val_to_pandas_dtype(
                value[field.name], field.dtype, nullable
            )
        return output


def proto_to_dtype(proto_dtype) -> str:
    if proto_dtype.HasField("int_type"):
        return "int"
    elif proto_dtype.HasField("double_type"):
        return "float"
    elif proto_dtype.HasField("string_type"):
        return "string"
    elif proto_dtype.HasField("bool_type"):
        return "bool"
    elif proto_dtype.HasField("timestamp_type"):
        return "timestamp"
    elif proto_dtype.HasField("optional_type"):
        return f"optional({proto_to_dtype(proto_dtype.optional_type.of)})"
    else:
        return str(proto_dtype)


def cast_df_to_arrow_dtype(
    df: pd.DataFrame, fields: List[Field]
) -> pd.DataFrame:
    """
    This helper function casts Pandas DatFrame columns to arrow dtype using list of Field proto.
    This is mostly used after each operation in mock client like:
    1. Operators
    2. Aggregations
    """
    for f in fields:
        try:
            series = cast_col_to_arrow_dtype(df[f.name], f.dtype)
            series.name = f.name
            df[f.name] = series
        except Exception as e:
            raise ValueError(
                f"Failed to cast column `{f.name}` of type `{proto_to_dtype(f.dtype)}` to arrow dtype. Error: {e}"
            )
    return df


def cast_df_to_pandas_dtype(
    df: pd.DataFrame, fields: List[Field]
) -> pd.DataFrame:
    """
    This helper function casts Pandas DatFrame columns to pandas dtype using list of Field proto.
    This is mostly used before passing the dataframe to user for custom py function like:
    1. Assign, Filter, Transform
    """
    for f in fields:
        try:
            series = cast_col_to_pandas_dtype(df[f.name], f.dtype)
            series.name = f.name
            df[f.name] = series
        except Exception as e:
            raise ValueError(
                f"Failed to cast column `{f.name}` of type `{proto_to_dtype(f.dtype)}` to pandas dtype. Error: {e}"
            )
    return df


def cast_df_to_schema(
    df: pd.DataFrame,
    dsschema: DSSchema,
) -> pd.DataFrame:
    """
    This helper function is used to cast the dataframe logged by user in the mock client to pd.ArrowDtype.
    """
    # Handle fields in keys and values
    fields = list(dsschema.keys.fields) + list(dsschema.values.fields)
    df = df.copy()
    df = df.reset_index(drop=True)
    for f in fields:
        if f.name not in df.columns:
            raise ValueError(
                f"Field `{f.name}` not found in dataframe while logging to dataset"
            )
        try:
            series = cast_col_to_arrow_dtype(df[f.name], f.dtype)
            series.name = f.name
            df[f.name] = series
        except Exception as e:
            raise ValueError(
                f"Failed to cast data logged to column `{f.name}` of type `{proto_to_dtype(f.dtype)}`: {e}"
            )
    if dsschema.timestamp not in df.columns:
        raise ValueError(
            f"Timestamp column `{dsschema.timestamp}` not found in dataframe while logging to dataset"
        )
    try:
        df[dsschema.timestamp] = pd.to_datetime(
            df[dsschema.timestamp].apply(lambda x: parse_datetime(x)), utc=True
        ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))
    except Exception as e:
        raise ValueError(
            f"Failed to cast data logged to timestamp column {dsschema.timestamp}: {e}"
        )
    return df


def add_deletes(
    df: pd.DataFrame, key_fields: List[str], ts_col: str
) -> pd.DataFrame:
    """
    This function adds delete rows to the dataframe based on the keys provided.
    """
    if len(key_fields) == 0:
        raise ValueError("Cannot add deletes to a dataset with no key fields")

    if FENNEL_DELETE_TIMESTAMP in df.columns:
        return df

    # Sort the dataframe by timestamp
    sorted_df = df.sort_values(ts_col)
    # Iterate over the dataframe and add deletes for each row, with the timestamp
    # set to the timestamp of the next row for the same key

    # Stores the previous index for each key
    last_index_for_key = {}
    # Initialize the delete timestamps list with length of the dataframe
    delete_timestamps = [None] * len(sorted_df)

    for i in range(len(sorted_df)):
        row = sorted_df.iloc[i]
        row_key_fields = []
        for key_field in key_fields:
            row_key_fields.append(row.loc[key_field])
        key = hash(tuple(row_key_fields))
        if key not in last_index_for_key:
            last_index_for_key[key] = i
        else:
            last_index = last_index_for_key[key]
            # Add the timestamp of the current row as the delete timestamp for the last row
            # Subtract 1 microsecond to ensure that the delete timestamp is strictly less than the
            # timestamp of the next row
            del_ts = row[ts_col] - pd.Timedelta("1us")
            delete_timestamps[last_index] = del_ts
            last_index_for_key[key] = i

    # Add the delete timestamp as a hidden column to the dataframe
    sorted_df[FENNEL_DELETE_TIMESTAMP] = delete_timestamps
    # Cast the timestamp column to arrow timestamp type
    sorted_df[FENNEL_DELETE_TIMESTAMP] = sorted_df[
        FENNEL_DELETE_TIMESTAMP
    ].astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))
    return sorted_df
