import json
from datetime import datetime

from math import isnan
from typing import Any, Union, List
import pandas as pd
import numpy as np
from fennel._vendor.requests import Response  # type: ignore
from fennel._vendor import jsondiff  # type: ignore
from google.protobuf.json_format import MessageToDict
from fennel.gen.schema_pb2 import DSSchema
from fennel.gen.dataset_pb2 import Operator, Filter, Transform, Assign
from fennel.gen.featureset_pb2 import Extractor
from fennel.gen.pycode_pb2 import PyCode


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


def cast_col_to_dtype(series: pd.Series, dtype) -> pd.Series:
    if not dtype.HasField("optional_type"):
        if series.isnull().any():
            raise ValueError("Null values found in non-optional field.")

    if dtype.HasField("int_type"):
        if series.dtype == pd.Float64Dtype():
            # Cast to float64 numpy. We need to do this, because pandas has a wierd bug,
            # where series of type float64 will throw an error if the floats are not ints ( expected ).
            # For example, series([1.2, 2.4, 3.3]) will throw an error.
            # BUT series of type pd.Float64Dtype() will not throw an error, but get rounded off.
            # So we cast to float64 numpy and then cast to pd.Int64Dtype()
            series = series.astype(np.float64)
        return pd.to_numeric(series).astype(pd.Int64Dtype())
    elif dtype.HasField("double_type"):
        return pd.to_numeric(series).astype(pd.Float64Dtype())
    elif dtype.HasField("string_type") or dtype.HasField("regex_type"):
        return pd.Series([str(x) for x in series]).astype(pd.StringDtype())
    elif dtype.HasField("bool_type"):
        return series.astype(pd.BooleanDtype())
    elif dtype.HasField("timestamp_type"):
        return pd.to_datetime(series.apply(lambda x: parse_datetime(x)))
    elif dtype.HasField("optional_type"):
        # Those fields which are not null should be casted to the right type
        if series.notnull().any():
            # collect the non-null values
            tmp_series = series[series.notnull()]
            non_null_idx = tmp_series.index
            tmp_series = cast_col_to_dtype(
                tmp_series,
                dtype.optional_type.of,
            )
            tmp_series.index = non_null_idx
            # set the non-null values with the casted values using the index
            series.loc[non_null_idx] = tmp_series
            series.replace({np.nan: None}, inplace=True)
            if callable(tmp_series.dtype):
                series = series.astype(tmp_series.dtype())
            else:
                series = series.astype(tmp_series.dtype)
            return series
    elif dtype.HasField("one_of_type"):
        return cast_col_to_dtype(series, dtype.one_of_type.of)
    elif dtype.HasField("between_type"):
        return cast_col_to_dtype(series, dtype.between_type.dtype)
    return series


def parse_datetime(value: Union[int, str, datetime]) -> datetime:
    if isinstance(value, int):
        try:
            return pd.to_datetime(value, unit="s")
        except ValueError:
            try:
                return pd.to_datetime(value, unit="ms")
            except ValueError:
                return pd.to_datetime(value, unit="us")
    return pd.to_datetime(value)


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


def cast_df_to_schema(
    df: pd.DataFrame, dsschema: DSSchema, pre_proc_cols: List[str] = []
) -> pd.DataFrame:
    # Handle fields in keys and values
    fields = list(dsschema.keys.fields) + list(dsschema.values.fields)
    df = df.copy()
    df = df.reset_index(drop=True)
    for f in fields:
        if f.name not in df.columns:
            if f.name in pre_proc_cols:
                continue
            raise ValueError(
                f"Field `{f.name}` not found in dataframe while logging to dataset"
            )
        try:
            series = cast_col_to_dtype(df[f.name], f.dtype)
            series.name = f.name
            df[f.name] = series
        except Exception as e:
            raise ValueError(
                f"Failed to cast data logged to column `{f.name}` of type `{proto_to_dtype(f.dtype)}`: {e}"
            )
    if dsschema.timestamp not in df.columns:
        if dsschema.timestamp in pre_proc_cols:
            return df
        raise ValueError(
            f"Timestamp column `{dsschema.timestamp}` not found in dataframe while logging to dataset"
        )
    try:
        df[dsschema.timestamp] = pd.to_datetime(
            df[dsschema.timestamp].apply(lambda x: parse_datetime(x))
        ).astype("datetime64[ns]")
    except Exception as e:
        raise ValueError(
            f"Failed to cast data logged to timestamp column {dsschema.timestamp}: {e}"
        )
    return df
