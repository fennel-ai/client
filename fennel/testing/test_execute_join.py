import pandas as pd
import pytest
from datetime import datetime, timezone
from fennel.testing.execute_join import table_table_join
from fennel.testing.test_utils import FENNEL_DELETE_TIMESTAMP, FENNEL_LOOKUP
from fennel.testing.executor import NodeRet
import fennel.gen.schema_pb2 as schema_proto
from fennel.gen.schema_pb2 import Field, DataType


def compare_dataframes(df1, df2):
    # Use pd.isnull() to check for NaT/None equivalency, then compare
    mask = (pd.isnull(df1) & pd.isnull(df2)) | (df1 == df2)
    return mask.all().all()


# LHS value from [1, 4], [6, 9], [10, inf]
# RHS value from [2, 3], [5, 7], [8, 11]
# Two keys have identical data
def test_basic_table_left_join_with_two_keys():
    # Left dataframe with customer data
    left_df = pd.DataFrame(
        {
            "customer_id": [1, 1, 1, 2, 2, 2],
            "timestamp": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc),
            ],
            "name": ["A", "B", "C", "D", "E", "F"],
            FENNEL_DELETE_TIMESTAMP: [
                datetime(2024, 1, 4, tzinfo=timezone.utc),
                datetime(2024, 1, 9, tzinfo=timezone.utc),
                None,
                datetime(2024, 1, 4, tzinfo=timezone.utc),
                datetime(2024, 1, 9, tzinfo=timezone.utc),
                None,
            ],
        }
    )
    left_node = NodeRet(
        df=left_df,
        timestamp_field="timestamp",
        key_fields=["customer_id"],
        fields=[
            Field(
                name="name",
                dtype=schema_proto.DataType(
                    string_type=schema_proto.StringType()
                ),
            ),
            Field(
                name="timestamp",
                dtype=schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            ),
            Field(
                name="customer_id",
                dtype=schema_proto.DataType(int_type=schema_proto.IntType()),
            ),
        ],
    )

    # Right dataframe with order data and upserts
    right_df = pd.DataFrame(
        {
            "cust_id": [1, 1, 1, 2, 2, 2],
            "timestamp": [
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
            ],
            "order_value": ["1", "2", "3", "4", "5", "6"],
            FENNEL_DELETE_TIMESTAMP: [
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
            ],
        }
    )
    right_node = NodeRet(
        df=right_df,
        timestamp_field="timestamp",
        key_fields=["cust_id"],
        fields=[
            Field(
                name="cust_id",
                dtype=schema_proto.DataType(int_type=schema_proto.IntType()),
            ),
            Field(
                name="timestamp",
                dtype=schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            ),
            Field(
                name="order_value",
                dtype=schema_proto.DataType(
                    string_type=schema_proto.StringType()
                ),
            ),
        ],
    )

    result = table_table_join(
        left_node,
        right_node,
        how="left",
        left_on=["customer_id"],
        right_on=["cust_id"],
    )

    # Verify the results
    expected_df = pd.DataFrame(
        {
            "customer_id": [
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
                2,
            ],
            "name": [
                "A",
                "A",
                "A",
                "B",
                "B",
                "B",
                "C",
                "C",
                "D",
                "D",
                "D",
                "E",
                "E",
                "E",
                "F",
                "F",
            ],
            "timestamp": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
            ],
            "order_value": [
                None,
                "1",
                None,
                "2",
                None,
                "3",
                "3",
                None,
                None,
                "4",
                None,
                "5",
                None,
                "6",
                "6",
                None,
            ],
            FENNEL_DELETE_TIMESTAMP: [
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 4, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 9, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
                None,
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 4, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 9, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
                None,
            ],
        }
    )
    expected_df = expected_df.sort_values(
        by=["customer_id", "name", "timestamp"]
    ).reset_index(drop=True)
    result = result.sort_values(
        by=["customer_id", "name", "timestamp"]
    ).reset_index(drop=True)
    expected_df = expected_df[result.columns]
    result = result[result.columns]
    assert compare_dataframes(result, expected_df)


# LHS value from [1, 4], [6, 9], [10, inf]
# RHS value from [2, 3], [5, 7], [8, 11]
# Two keys have identical data
def test_inner_join_table():
    # Left dataframe with customer data
    left_df = pd.DataFrame(
        {
            "customer_id": [1, 1, 1],
            "timestamp": [
                datetime(2024, 1, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc),
            ],
            "name": ["A", "B", "C"],
            FENNEL_DELETE_TIMESTAMP: [
                datetime(2024, 1, 4, tzinfo=timezone.utc),
                datetime(2024, 1, 9, tzinfo=timezone.utc),
                None,
            ],
        }
    )
    left_node = NodeRet(
        df=left_df,
        timestamp_field="timestamp",
        key_fields=["customer_id"],
        fields=[
            Field(
                name="name",
                dtype=schema_proto.DataType(
                    string_type=schema_proto.StringType()
                ),
            ),
            Field(
                name="timestamp",
                dtype=schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            ),
            Field(
                name="customer_id",
                dtype=schema_proto.DataType(int_type=schema_proto.IntType()),
            ),
        ],
    )

    # Right dataframe with order data and upserts
    right_df = pd.DataFrame(
        {
            "cust_id": [1, 1, 1],
            "timestamp": [
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 5, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
            ],
            "order_value": ["1", "2", "3"],
            FENNEL_DELETE_TIMESTAMP: [
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
            ],
        }
    )
    right_node = NodeRet(
        df=right_df,
        timestamp_field="timestamp",
        key_fields=["cust_id"],
        fields=[
            Field(
                name="cust_id",
                dtype=schema_proto.DataType(int_type=schema_proto.IntType()),
            ),
            Field(
                name="timestamp",
                dtype=schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            ),
            Field(
                name="order_value",
                dtype=schema_proto.DataType(
                    string_type=schema_proto.StringType()
                ),
            ),
        ],
    )

    result = table_table_join(
        left_node,
        right_node,
        how="inner",
        left_on=["customer_id"],
        right_on=["cust_id"],
    )

    # Verify the results
    expected_df = pd.DataFrame(
        {
            "customer_id": [1, 1, 1, 1],
            "name": [
                "A",
                "B",
                "B",
                "C",
            ],
            "timestamp": [
                datetime(2024, 1, 2, tzinfo=timezone.utc),
                datetime(2024, 1, 6, tzinfo=timezone.utc),
                datetime(2024, 1, 8, tzinfo=timezone.utc),
                datetime(2024, 1, 10, tzinfo=timezone.utc),
            ],
            "order_value": [
                "1",
                "2",
                "3",
                "3",
            ],
            FENNEL_DELETE_TIMESTAMP: [
                datetime(2024, 1, 3, tzinfo=timezone.utc),
                datetime(2024, 1, 7, tzinfo=timezone.utc),
                datetime(2024, 1, 9, tzinfo=timezone.utc),
                datetime(2024, 1, 11, tzinfo=timezone.utc),
            ],
        }
    )
    expected_df = expected_df.sort_values(
        by=["customer_id", "name", "timestamp"]
    ).reset_index(drop=True)
    result = result.sort_values(
        by=["customer_id", "name", "timestamp"]
    ).reset_index(drop=True)
    expected_df = expected_df[result.columns]
    result = result[result.columns]
    assert compare_dataframes(result, expected_df)
