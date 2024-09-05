from datetime import datetime, timezone
from typing import Optional, List

import pandas as pd
import pyarrow as pa
import pytest

from fennel.datasets import dataset, field
from fennel.dtypes import between, oneof, regex, struct
from fennel.featuresets import featureset, feature as F
from fennel.gen import schema_pb2 as schema_proto
from fennel.internal_lib.schema import get_datatype
from fennel.internal_lib.to_proto.to_proto import fields_to_dsschema
from fennel.internal_lib.utils.utils import parse_datetime_in_value
from fennel.testing import mock
from fennel.testing.test_utils import (
    cast_df_to_schema,
    cast_col_to_arrow_dtype,
    cast_col_to_pandas_dtype,
)

__owner__ = "nitin@fennel.ai"


# Example tests
def test_cast_int():
    @dataset
    class TestDataset:
        int_field: int
        created_ts: datetime

    df = pd.DataFrame(
        {
            "int_field": ["1", "2", "3"],
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
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
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
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
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
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
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
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
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
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
        [
            datetime(2021, 1, 1, tzinfo=timezone.utc),
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        ]
    ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))
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
        [
            datetime(2021, 1, 1, tzinfo=timezone.utc),
            datetime(2021, 1, 2, tzinfo=timezone.utc),
        ]
    ).astype(pd.ArrowDtype(pa.timestamp("ns", "UTC")))
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
        == """Failed to cast data logged to timestamp column created_ts: Unknown datetime string format, unable to parse: not a timestamp, at position 0"""
    )


def test_null_in_non_optional_field():
    @dataset
    class TestDataset:
        non_optional_field: int
        created_ts: datetime

    df = pd.DataFrame(
        {
            "non_optional_field": [1, None, 2],
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
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
            "created_ts": [datetime.now(timezone.utc) for _ in range(3)],
        }
    )
    schema = fields_to_dsschema(TestDataset.fields)
    with pytest.raises(ValueError) as e:
        cast_df_to_schema(df, schema)
    assert (
        str(e.value)
        == """Failed to cast data logged to column `int_field` of type `int`: Unable to parse string "not_an_int" at position 0"""
    )


def test_cast_col_to_arrow_dtype():
    """
    Testing casting to arrow dtype from DataType proto
    """

    # 1. Test Casting of a struct
    data = pd.Series(
        [
            {
                "a": 1,
                "b": 1.1,
                "c": datetime.now(timezone.utc),
                "d": "name1",
                "e": True,
            },
            {
                "a": 2,
                "b": 2.1,
                "c": datetime.now(timezone.utc),
                "d": "name2",
                "e": False,
            },
            {
                "a": 3,
                "b": 1.1,
                "c": datetime.now(timezone.utc),
                "d": "name3",
                "e": True,
            },
        ],
        name="testing",
    )
    fields = [
        schema_proto.Field(
            name="a",
            dtype=schema_proto.DataType(int_type=schema_proto.IntType()),
        ),
        schema_proto.Field(
            name="b",
            dtype=schema_proto.DataType(double_type=schema_proto.DoubleType()),
        ),
        schema_proto.Field(
            name="c",
            dtype=schema_proto.DataType(
                timestamp_type=schema_proto.TimestampType()
            ),
        ),
        schema_proto.Field(
            name="d",
            dtype=schema_proto.DataType(string_type=schema_proto.StringType()),
        ),
        schema_proto.Field(
            name="e",
            dtype=schema_proto.DataType(bool_type=schema_proto.BoolType()),
        ),
    ]
    data_type = schema_proto.DataType(
        struct_type=schema_proto.StructType(
            name="test_struct",
            fields=fields,
        )
    )
    casted_data = cast_col_to_arrow_dtype(data, data_type)

    # 2. Casting of a map
    data = pd.Series(
        [
            {
                "a": 1,
                "b": 2,
            },
            {
                "a": 2,
                "b": 3,
            },
            {
                "a": 3,
                "b": 4,
            },
        ],
        name="testing",
    )
    data_type = schema_proto.DataType(
        map_type=schema_proto.MapType(
            key=schema_proto.DataType(string_type=schema_proto.StringType()),
            value=schema_proto.DataType(int_type=schema_proto.IntType()),
        )
    )
    casted_data = cast_col_to_arrow_dtype(data, data_type)
    assert str(casted_data.dtype) == "map<string, int64>[pyarrow]"

    # 3. Casting of an Embedding
    data = pd.Series(
        [
            [1, 1, 2, 3, 4],
            [1, 1, 2, 3, 4],
            [1, 1, 2, 3, 4],
        ],
        name="testing",
    )
    data_type = schema_proto.DataType(
        embedding_type=schema_proto.EmbeddingType(embedding_size=5)
    )
    casted_data = cast_col_to_arrow_dtype(data, data_type)
    assert str(casted_data.dtype) == "fixed_size_list<item: double>[5][pyarrow]"

    # 4. Casting of a complex object i.e List[struct]
    data = pd.Series(
        [
            [
                {
                    "a": 1,
                    "b": 1.1,
                    "c": datetime.now(timezone.utc),
                    "d": "name1",
                    "e": True,
                },
                {
                    "a": 1,
                    "b": 1.1,
                    "c": datetime.now(timezone.utc),
                    "d": "name1",
                    "e": True,
                },
            ],
            [
                {
                    "a": 1,
                    "b": 1.1,
                    "c": datetime.now(timezone.utc),
                    "d": "name1",
                    "e": True,
                },
                {
                    "a": 1,
                    "b": 1.1,
                    "c": datetime.now(timezone.utc),
                    "d": "name1",
                    "e": True,
                },
            ],
        ],
        name="testing",
    )
    data_type = schema_proto.DataType(
        array_type=schema_proto.ArrayType(
            of=schema_proto.DataType(
                struct_type=schema_proto.StructType(fields=fields)
            )
        )
    )
    casted_data = cast_col_to_arrow_dtype(data, data_type)


def test_invalid_cast_col_to_arrow_dtype():
    """
    Testing invalid casting returns some error
    """

    # 1. Test Casting of a struct
    data = pd.Series(
        [
            {
                "a": 1,
                "b": 1.1,
                "c": "fdfd",
            },
            {
                "a": 2,
                "b": 2.1,
                "c": datetime.now(timezone.utc),
            },
            {
                "a": 3,
                "b": 1.1,
                "c": datetime.now(timezone.utc),
            },
        ],
        name="testing",
    )
    fields = [
        schema_proto.Field(
            name="a",
            dtype=schema_proto.DataType(int_type=schema_proto.IntType()),
        ),
        schema_proto.Field(
            name="b",
            dtype=schema_proto.DataType(double_type=schema_proto.DoubleType()),
        ),
        schema_proto.Field(
            name="c",
            dtype=schema_proto.DataType(
                timestamp_type=schema_proto.TimestampType()
            ),
        ),
    ]
    data_type = schema_proto.DataType(
        struct_type=schema_proto.StructType(
            name="test_struct",
            fields=fields,
        )
    )
    with pytest.raises(Exception) as e:
        cast_col_to_arrow_dtype(data, data_type)
    assert (
        str(e.value)
        == "Unknown datetime string format, unable to parse: fdfd, at position 0"
    )

    # 2. Casting of a map
    data = pd.Series(
        [
            {
                "a": 1,
                "b": "2.2",
            },
            {
                "a": 2,
                "b": "3.2",
            },
            {
                "a": 3,
                "b": "4.2",
            },
        ],
        name="testing",
    )
    data_type = schema_proto.DataType(
        map_type=schema_proto.MapType(
            key=schema_proto.DataType(string_type=schema_proto.StringType()),
            value=schema_proto.DataType(int_type=schema_proto.IntType()),
        )
    )
    with pytest.raises(Exception) as e:
        cast_col_to_arrow_dtype(data, data_type)
    assert (
        str(e.value)
        == "Unsupported cast from struct<a: int64, b: string> to map using function cast_map"
    )

    # 3. Casting of an Embedding
    data = pd.Series(
        [
            [1, 1, 2, 3, 4, 5],
            [1, 1, 2, 3, 4, 6],
            [1, 1, 2, 3, 4, 8],
        ],
        name="testing",
    )
    data_type = schema_proto.DataType(
        embedding_type=schema_proto.EmbeddingType(embedding_size=5)
    )
    with pytest.raises(ValueError) as e:
        cast_col_to_arrow_dtype(data, data_type)
    assert (
        str(e.value)
        == "ListType can only be casted to FixedSizeListType if the lists are all the expected size."
    )


def test_cast_col_to_pandas_dtype():
    """
    Testing casting pd.Series of arrow dtype to pandas dtype.
    """
    value = [
        {
            "a": 1,
            "b": {"a": 1, "b": 2, "c": 3},
            "c": [1, 2, 3, 4],
            "d": b"hello world",
        }
    ]
    parsed_value = [
        {
            "a": 1,
            "b": {"a": 1, "b": 2, "c": 3},
            "c": [1, 2, 3, 4],
            "d": b"hello world",
            "e": pd.NA,
        }
    ]
    data = pd.Series([value], name="testing")
    data_type = schema_proto.DataType(
        array_type=schema_proto.ArrayType(
            of=schema_proto.DataType(
                struct_type=schema_proto.StructType(
                    fields=[
                        schema_proto.Field(
                            name="a",
                            dtype=schema_proto.DataType(
                                int_type=schema_proto.IntType()
                            ),
                        ),
                        schema_proto.Field(
                            name="b",
                            dtype=schema_proto.DataType(
                                map_type=schema_proto.MapType(
                                    key=schema_proto.DataType(
                                        string_type=schema_proto.StringType()
                                    ),
                                    value=schema_proto.DataType(
                                        int_type=schema_proto.IntType()
                                    ),
                                )
                            ),
                        ),
                        schema_proto.Field(
                            name="c",
                            dtype=schema_proto.DataType(
                                array_type=schema_proto.ArrayType(
                                    of=schema_proto.DataType(
                                        int_type=schema_proto.IntType()
                                    )
                                )
                            ),
                        ),
                        schema_proto.Field(
                            name="d",
                            dtype=schema_proto.DataType(
                                bytes_type=schema_proto.BytesType()
                            ),
                        ),
                        schema_proto.Field(
                            name="e",
                            dtype=schema_proto.DataType(
                                optional_type=schema_proto.OptionalType(
                                    of=schema_proto.DataType(
                                        timestamp_type=schema_proto.TimestampType()
                                    )
                                )
                            ),
                        ),
                    ]
                )
            )
        )
    )

    arrow_dtype_data = cast_col_to_arrow_dtype(data, data_type)
    pandas_dtype_data = cast_col_to_pandas_dtype(arrow_dtype_data, data_type)

    assert pandas_dtype_data.dtype == object
    assert pandas_dtype_data.tolist()[0] == parsed_value


def test_optional_timestamp_cast_col_to_pandas_dtype():
    """
    Testing casting pd.Series of arrow dtype to pandas dtype.
    """
    value = [1, 2, 3, None, None]
    parsed_value = [
        datetime.fromtimestamp(1, tz=timezone.utc),
        datetime.fromtimestamp(2, tz=timezone.utc),
        datetime.fromtimestamp(3, tz=timezone.utc),
        pd.NaT,
        pd.NaT,
    ]
    data = pd.Series(value, name="testing")
    data_type = schema_proto.DataType(
        optional_type=schema_proto.OptionalType(
            of=schema_proto.DataType(
                timestamp_type=schema_proto.TimestampType()
            )
        )
    )

    arrow_dtype_data = cast_col_to_arrow_dtype(data, data_type)
    pandas_dtype_data = cast_col_to_pandas_dtype(arrow_dtype_data, data_type)

    assert pandas_dtype_data.dtype == "datetime64[ns, UTC]"
    assert pandas_dtype_data.tolist() == parsed_value


@mock
def test_casting_empty_dataframe(client):
    @dataset(index=True)
    class UserPhone:
        user_id: int = field(key=True)
        phone_number: int
        created_at: datetime = field(timestamp=True)
        updated_at: datetime

    @featureset
    class UserFeatures:
        user_id: int
        latest_phone_update: Optional[datetime] = F(UserPhone.updated_at)

    client.commit(
        datasets=[UserPhone], featuresets=[UserFeatures], message="first-commit"
    )

    feature_df = client.query(
        outputs=[UserFeatures.latest_phone_update],
        inputs=[UserFeatures.user_id],
        input_dataframe=pd.DataFrame({"UserFeatures.user_id": [1, 2]}),
    )
    assert feature_df["UserFeatures.latest_phone_update"].tolist() == [
        pd.NaT,
        pd.NaT,
    ]


def test_parse_datetime_in_value():
    dtype = get_datatype(List[datetime])
    value = [0, 1, 3, 4, 5]
    parse_value = parse_datetime_in_value(value, dtype)
    assert parse_value == [
        datetime.fromtimestamp(0, tz=timezone.utc),
        datetime.fromtimestamp(1, tz=timezone.utc),
        datetime.fromtimestamp(3, tz=timezone.utc),
        datetime.fromtimestamp(4, tz=timezone.utc),
        datetime.fromtimestamp(5, tz=timezone.utc),
    ]

    dtype = get_datatype(List[Optional[datetime]])
    value = [0, 1, None, 4, 5]
    parse_value = parse_datetime_in_value(value, dtype)
    assert parse_value == [
        datetime.fromtimestamp(0, tz=timezone.utc),
        datetime.fromtimestamp(1, tz=timezone.utc),
        pd.NA,
        datetime.fromtimestamp(4, tz=timezone.utc),
        datetime.fromtimestamp(5, tz=timezone.utc),
    ]

    dtype = get_datatype(Optional[datetime])
    value = None
    parse_value = parse_datetime_in_value(value, dtype)
    assert parse_value is pd.NA

    dtype = get_datatype(datetime)
    value = 1
    parse_value = parse_datetime_in_value(value, dtype)
    assert parse_value == datetime.fromtimestamp(1, tz=timezone.utc)

    @struct
    class A:
        name: str
        birthdate: Optional[datetime]

    dtype = get_datatype(List[A])
    value = [
        A(name=1, birthdate=1),
        A(name=1, birthdate=None),
        A(name=1, birthdate=datetime.fromtimestamp(0, tz=timezone.utc)),
        {"name": 1, "birthdate": 1},
        {"name": 1, "birthdate": None},
        {"name": 1, "birthdate": datetime.fromtimestamp(0, tz=timezone.utc)},
    ]
    parse_value = parse_datetime_in_value(value, dtype)
    assert parse_value == [
        {"name": 1, "birthdate": datetime.fromtimestamp(1, tz=timezone.utc)},
        {"name": 1, "birthdate": pd.NA},
        {"name": 1, "birthdate": datetime.fromtimestamp(0, tz=timezone.utc)},
        {"name": 1, "birthdate": datetime.fromtimestamp(1, tz=timezone.utc)},
        {"name": 1, "birthdate": pd.NA},
        {"name": 1, "birthdate": datetime.fromtimestamp(0, tz=timezone.utc)},
    ]
