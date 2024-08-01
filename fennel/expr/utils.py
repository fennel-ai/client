from typing import List, Dict

import pandas as pd
from fennel.internal_lib.schema.schema import from_proto
import pyarrow as pa
from fennel.expr.expr import Expr
from fennel.expr.serializer import ExprSerializer
from fennel_data_lib import compute, compile as data_lib_compile
from fennel.internal_lib.schema import get_datatype
import fennel.gen.schema_pb2 as schema_proto


def convert_object(obj):
    if isinstance(obj, list):
        result = [convert_object(i) for i in obj]
    elif isinstance(obj, dict):
        result = {}
        for key in obj:
            result[key] = convert_object(obj[key])
    elif hasattr(obj, "as_json"):
        result = obj.as_json()
    else:
        result = obj
    return result


def convert_objects(df):
    for col in df.columns:
        df[col] = df[col].apply(convert_object)
    return df


def pd_to_pa(pd_data, schema=None):
    # Schema unspecified - as in the case with lookups
    if not schema:
        if isinstance(pd_data, pd.Series):
            pd_data = pd_data.apply(convert_object)
            return pa.Array.from_pandas(pd_data)
        elif isinstance(pd_data, pd.DataFrame):
            pd_data = convert_objects(pd_data)
            return pa.RecordBatch.from_pandas(pd_data, preserve_index=False)
        else:
            raise ValueError("only pd.Series or pd.Dataframe expected")

    # Single column expected
    if isinstance(schema, pa.Field):
        # extra columns may have been provided
        if isinstance(pd_data, pd.DataFrame):
            if schema.name not in pd_data:
                raise ValueError(
                    f"Dataframe does not contain column {schema.name}"
                )
            # df -> series
            pd_data = pd_data[schema.name]

        if not isinstance(pd_data, pd.Series):
            raise ValueError("only pd.Series or pd.Dataframe expected")
        pd_data = pd_data.apply(convert_object)
        return pa.Array.from_pandas(pd_data, type=schema.type)

    # Multiple columns case: use the columns we need
    result_df = pd.DataFrame()
    for col in schema.names:
        if col not in pd_data:
            raise ValueError(f"Dataframe does not contain column {col}")
        result_df[col] = pd_data[col].apply(convert_object)
    return pa.RecordBatch.from_pandas(
        result_df, preserve_index=False, schema=schema
    )


def pa_to_pd(pa_data):
    return pa_data.to_pandas(types_mapper=pd.ArrowDtype)


def compute_expr(expr: Expr, df: pd.DataFrame, schema: Dict) -> pd.Series:
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    proto_bytes = proto_expr.SerializeToString()
    df_pa = pd_to_pa(df)
    proto_schema = {}
    for key, value in schema.items():
        proto_schema[key] = get_datatype(value).SerializeToString()
    arrow_col = compute(proto_bytes, df_pa, proto_schema)
    return pa_to_pd(arrow_col)


def compile(expr: Expr, schema: Dict) -> bytes:
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    proto_bytes = proto_expr.SerializeToString()
    proto_schema = {}
    for key, value in schema.items():
        proto_schema[key] = get_datatype(value).SerializeToString()
    type_bytes = data_lib_compile(proto_bytes, proto_schema)
    datatype = schema_proto.DataType()
    datatype.ParseFromString(type_bytes)
    return from_proto(datatype)
