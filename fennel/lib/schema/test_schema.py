from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytest

import fennel.gen.schema_pb2 as proto
from fennel.lib.schema.schema import (
    get_datatype,
    data_schema_check,
    between,
    oneof,
    regex,
)


def test_get_data_type():
    assert get_datatype(int) == proto.DataType(int_type=proto.IntType())
    assert get_datatype(Optional[int]) == proto.DataType(
        optional_type=proto.OptionalType(
            of=proto.DataType(int_type=proto.IntType())
        )
    )
    x: float = 1.0
    assert get_datatype(type(x)) == proto.DataType(
        double_type=proto.DoubleType()
    )
    x: bool = True
    assert get_datatype(type(x)) == proto.DataType(bool_type=proto.BoolType())
    x: str = "hello"
    assert get_datatype(type(x)) == proto.DataType(
        string_type=proto.StringType()
    )
    x: datetime = datetime.now()

    assert get_datatype(type(x)) == proto.DataType(
        timestamp_type=proto.TimestampType()
    )
    assert get_datatype(List[int]) == proto.DataType(
        array_type=proto.ArrayType(of=proto.DataType(int_type=proto.IntType()))
    )
    assert get_datatype(Dict[str, float]) == proto.DataType(
        map_type=proto.MapType(
            key=proto.DataType(string_type=proto.StringType()),
            value=proto.DataType(double_type=proto.DoubleType()),
        )
    )
    assert get_datatype(Dict[str, Dict[str, List[float]]]) == proto.DataType(
        map_type=proto.MapType(
            key=proto.DataType(string_type=proto.StringType()),
            value=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(string_type=proto.StringType()),
                    value=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                )
            ),
        )
    )
    assert get_datatype(List[Dict[str, float]]) == proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(string_type=proto.StringType()),
                    value=proto.DataType(double_type=proto.DoubleType()),
                )
            )
        )
    )
    assert get_datatype(List[Dict[str, List[float]]]) == proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(string_type=proto.StringType()),
                    value=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                )
            )
        )
    )


def test_additional_dtypes():
    assert get_datatype(int) == proto.DataType(int_type=proto.IntType())
    assert get_datatype(between(int, 1, 5)) == proto.DataType(
        between_type=proto.Between(
            dtype=proto.DataType(int_type=proto.IntType()),
            min=proto.Value(int=1),
            max=proto.Value(int=5),
        )
    )
    assert get_datatype(between(int, 1, 5, False, True)) == proto.DataType(
        between_type=proto.Between(
            dtype=proto.DataType(int_type=proto.IntType()),
            min=proto.Value(int=1),
            max=proto.Value(int=5),
            strict_min=False,
            strict_max=True,
        )
    )
    assert get_datatype(between(float, 1, 5, True, False)) == proto.DataType(
        between_type=proto.Between(
            dtype=proto.DataType(double_type=proto.DoubleType()),
            min=proto.Value(float=1),
            max=proto.Value(float=5),
            strict_min=True,
            strict_max=False,
        )
    )
    assert get_datatype(oneof(str, ["male", "female"])) == proto.DataType(
        one_of_type=proto.OneOf(
            of=proto.DataType(string_type=proto.StringType()),
            options=[
                proto.Value(string="male"),
                proto.Value(string="female"),
            ],
        )
    )
    assert get_datatype(oneof(int, [1, 2, 3])) == proto.DataType(
        one_of_type=proto.OneOf(
            of=proto.DataType(int_type=proto.IntType()),
            options=[
                proto.Value(int=1),
                proto.Value(int=2),
                proto.Value(int=3),
            ],
        )
    )
    assert get_datatype(regex("[a-z]+")) == proto.DataType(
        regex_type=proto.RegexType(pattern="[a-z]+")
    )


def test_additional_dtypes_invalid():
    with pytest.raises(TypeError) as e:
        get_datatype(between(int, 1, 5.0))
    assert str(e.value) == "Dtype of between is int and max param is float"
    with pytest.raises(TypeError) as e:
        get_datatype(between(str, 1, 5))
    assert str(e.value) == "'between' type only accepts int or float types"
    with pytest.raises(TypeError) as e:
        get_datatype(oneof(float, [1, 2, 3]))
    assert str(e.value) == "'oneof' type only accepts int or str types"
    with pytest.raises(TypeError) as e:
        get_datatype(oneof(int, [1.2, 2.3, 3]))
    assert (
        str(e.value) == "'oneof' options should match the type of dtype, "
        "found 'float' expected 'int'."
    )
    with pytest.raises(TypeError) as e:
        get_datatype(oneof(str, [1, 2, 3]))
    assert (
        str(e.value) == "'oneof' options should match the type of dtype, "
        "found 'int' expected 'str'."
    )

    with pytest.raises(TypeError) as e:
        get_datatype(regex(1))
    assert str(e.value) == "'regex' type only accepts str types"


def test_valid_schema():
    now = datetime.now()
    yesterday = now - timedelta(days=1)

    data = [
        [18232, "Ross", 32, "USA", True, now],
        [18234, "Monica", 64, "Chile", False, yesterday],
    ]
    columns = ["user_id", "name", "age", "country", "Truth", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="user_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="name",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
                proto.Field(
                    name="age",
                    dtype=proto.DataType(
                        optional_type=proto.OptionalType(
                            of=proto.DataType(int_type=proto.IntType())
                        )
                    ),
                ),
                proto.Field(
                    name="country",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
                proto.Field(
                    name="Truth",
                    dtype=proto.DataType(bool_type=proto.BoolType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data.append([18234, "Monica", None, "Chile", True, yesterday])
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data = [
        [18232, np.array([1, 2, 3, 4]), np.array([1, 2, 3]), 10, now],
        [
            18234,
            np.array([1, 2.2, 0.213, 0.343]),
            np.array([0.87, 2, 3]),
            9,
            now,
        ],
        [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
    ]
    columns = [
        "doc_id",
        "bert_embedding",
        "fast_text_embedding",
        "num_words",
        "timestamp",
    ]
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="doc_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="bert_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=4)
                    ),
                ),
                proto.Field(
                    name="fast_text_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=3)
                    ),
                ),
                proto.Field(
                    name="num_words",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data = [
        [
            {"a": 1, "b": 2, "c": 3},
        ]
    ]
    dsschema = proto.DSSchema(
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="X",
                    dtype=proto.DataType(
                        map_type=proto.MapType(
                            key=proto.DataType(string_type=proto.StringType()),
                            value=proto.DataType(int_type=proto.IntType()),
                        )
                    ),
                )
            ]
        )
    )
    df = pd.DataFrame(data, columns=["X"])
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0


def test_invalid_schema():
    now = datetime.now()
    yesterday = now - timedelta(days=1)

    data = [
        [18232, "Ross", "32", "USA", now],
        [18234, "Monica", 64, "Chile", yesterday],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="user_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="name",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
                proto.Field(
                    name="age",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
                proto.Field(
                    name="country",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 1

    data = [
        [18232, np.array([1, 2, 3]), np.array([1, 2, 3]), 10, now],
        [
            18234,
            np.array([1, 2.2, 0.213, 0.343]),
            np.array([0.87, 2, 3]),
            9,
            now,
        ],
        [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
    ]
    columns = [
        "doc_id",
        "bert_embedding",
        "fast_text_embedding",
        "num_words",
        "timestamp",
    ]
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="doc_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="bert_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=4)
                    ),
                ),
                proto.Field(
                    name="fast_text_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=3)
                    ),
                ),
                proto.Field(
                    name="num_words",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 1

    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="doc_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="bert_embedding",
                    dtype=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                ),
                proto.Field(
                    name="fast_text_embedding",
                    dtype=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                ),
                proto.Field(
                    name="num_words",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data = [[[123]]]
    dsschema = proto.DSSchema(
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="X",
                    dtype=proto.DataType(
                        map_type=proto.MapType(
                            key=proto.DataType(string_type=proto.StringType()),
                            value=proto.DataType(int_type=proto.IntType()),
                        )
                    ),
                )
            ]
        )
    )
    df = pd.DataFrame(data, columns=["X"])
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 1


def test_invalid_schema_additional_types():
    now = datetime.now()
    data = [
        [18232, "Ross9", 212, "transgender", 5, now],
    ]
    columns = ["user_id", "name", "age", "gender", "grade", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="user_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="name",
                    dtype=proto.DataType(
                        regex_type=proto.RegexType(
                            pattern="[A-Za-z]+[0-9]",
                        )
                    ),
                ),
                proto.Field(
                    name="age",
                    dtype=proto.DataType(
                        between_type=proto.Between(
                            dtype=proto.DataType(int_type=proto.IntType()),
                            min=proto.Value(int=1),
                            max=proto.Value(int=100),
                        )
                    ),
                ),
                proto.Field(
                    name="gender",
                    dtype=proto.DataType(
                        one_of_type=proto.OneOf(
                            of=proto.DataType(string_type=proto.StringType()),
                            options=[
                                proto.Value(string="male"),
                                proto.Value(string="female"),
                            ],
                        )
                    ),
                ),
                proto.Field(
                    name="gender",
                    dtype=proto.DataType(
                        one_of_type=proto.OneOf(
                            of=proto.DataType(int_type=proto.IntType()),
                            options=[
                                proto.Value(int=1),
                                proto.Value(int=2),
                                proto.Value(int=3),
                            ],
                        )
                    ),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 3
