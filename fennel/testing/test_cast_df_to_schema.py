from datetime import datetime
from typing import Optional

import pandas as pd
import pytest

from fennel import dataset
from fennel.dtypes import between, oneof, regex
from fennel.internal_lib.to_proto.to_proto import fields_to_dsschema
from fennel.testing.test_utils import cast_df_to_schema


# Example tests
def test_cast_int():
    @dataset
    class TestDataset:
        int_field: int
        created_ts: datetime

    df = pd.DataFrame(
        {
            "int_field": ["1", "2", "3"],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    assert all(result_df["int_field"] == pd.Series([1, 2, 3], dtype="Int64"))


def test_cast_string():
    @dataset
    class TestDataset:
        string_field: str
        created_ts: datetime

    df = pd.DataFrame(
        {
            "string_field": [123, 456, 789],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    assert all(result_df["string_field"] == pd.Series(["123", "456", "789"]))


def test_cast_optional_string():
    @dataset
    class TestDataset:
        int_field: Optional[int]
        created_ts: datetime

    df = pd.DataFrame(
        {
            "int_field": ["123", None, "789"],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    expected = pd.Series([123, None, 789], name="int_field", dtype="Int64")
    for i in range(3):
        if pd.isna(expected.iloc[i]):
            assert pd.isna(result_df["int_field"].iloc[i])
        else:
            assert result_df["int_field"].iloc[i] == expected.iloc[i]


def test_cast_bool():
    @dataset
    class TestDataset:
        bool_field: bool
        created_ts: datetime

    df = pd.DataFrame(
        {
            "bool_field": [1, 0, 1],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    assert all(result_df["bool_field"] == pd.Series([True, False, True]))


def test_cast_type_restrictions():
    @dataset
    class TestDataset:
        age: between(int, min=0, max=100)
        gender: oneof(str, ["male", "female"])
        email: regex("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]")
        created_ts: datetime

    df = pd.DataFrame(
        {
            "age": ["21", "22", "23"],
            "gender": [1, 2, 3],
            "email": [1223423, 1223423, 1223423],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    assert all(result_df["age"] == pd.Series([21, 22, 23], dtype="Int64"))
    assert all(result_df["gender"] == pd.Series(["1", "2", "3"]))
    assert all(
        result_df["email"] == pd.Series(["1223423", "1223423", "1223423"])
    )


def test_cast_timestamp():
    @dataset
    class TestDataset:
        created_ts: datetime

    df = pd.DataFrame(
        {
            "created_ts": ["2021-01-01", "2021-01-02"],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    expected_timestamps = pd.Series(
        [datetime(2021, 1, 1), datetime(2021, 1, 2)]
    )
    assert all(result_df["created_ts"] == expected_timestamps)
    assert result_df["created_ts"].dtype == expected_timestamps.dtype


def test_cast_timestamp_with_timezone():
    @dataset
    class TestDataset:
        created_ts: datetime

    df = pd.DataFrame(
        {
            "created_ts": [
                "2021-01-01T00:00:00.000Z",
                "2021-01-02T00:00:00.000Z",
            ],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    result_df = cast_df_to_schema(df, schema)
    expected_timestamps = pd.Series(
        [datetime(2021, 1, 1), datetime(2021, 1, 2)]
    )
    assert all(result_df["created_ts"] == expected_timestamps)
    assert result_df["created_ts"].dtype == expected_timestamps.dtype


def test_cast_invalid_timestamp():
    @dataset
    class TestDataset:
        created_ts: datetime

    df = pd.DataFrame(
        {
            "created_ts": [
                "2021-01-01T00:00:00.000Z",
                "2021-01-02T00:00:00.000Z",
                "2021-01-02T00:00:00.000Z",
                "not a timestamp",
            ],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    with pytest.raises(ValueError) as e:
        cast_df_to_schema(df, schema)
    assert (
        str(e.value)
        == """Failed to cast data logged to timestamp column created_ts: Unknown string format: not a timestamp present at position 0"""
    )


def test_null_in_non_optional_field():
    @dataset
    class TestDataset:
        non_optional_field: int
        created_ts: datetime

    df = pd.DataFrame(
        {
            "non_optional_field": [1, None, 2],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    with pytest.raises(ValueError) as e:
        cast_df_to_schema(df, schema)
    assert (
        str(e.value)
        == """Failed to cast data logged to column `non_optional_field` of type `int`: Null values found in non-optional field."""
    )


def test_cast_failure_for_incorrect_type():
    @dataset
    class TestDataset:
        int_field: int
        created_ts: datetime

    df = pd.DataFrame(
        {
            "int_field": ["not_an_int", "123", "456"],
            "created_ts": [datetime.now() for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    with pytest.raises(ValueError) as e:
        cast_df_to_schema(df, schema)
    assert (
        str(e.value)
        == """Failed to cast data logged to column `int_field` of type `int`: Unable to parse string "not_an_int" at position 0"""
    )
