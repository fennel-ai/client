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
    assert get_datatype(int) == proto.DataType(scalar_type=proto.ScalarType.INT)
    assert get_datatype(Optional[int]) == proto.DataType(
        scalar_type=proto.ScalarType.INT, is_nullable=True
    )
    x: float = 1.0
    assert get_datatype(type(x)) == proto.DataType(
        scalar_type=proto.ScalarType.FLOAT
    )
    x: bool = True
    assert get_datatype(type(x)) == proto.DataType(
        scalar_type=proto.ScalarType.BOOLEAN
    )
    x: str = "hello"
    assert get_datatype(type(x)) == proto.DataType(
        scalar_type=proto.ScalarType.STRING
    )
    x: datetime = datetime.now()

    assert get_datatype(type(x)) == proto.DataType(
        scalar_type=proto.ScalarType.TIMESTAMP
    )
    assert get_datatype(List[int]) == proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(scalar_type=proto.ScalarType.INT)
        )
    )
    assert get_datatype(Dict[str, float]) == proto.DataType(
        map_type=proto.MapType(
            key=proto.DataType(scalar_type=proto.ScalarType.STRING),
            value=proto.DataType(scalar_type=proto.ScalarType.FLOAT),
        )
    )
    assert get_datatype(Dict[str, Dict[str, List[float]]]) == proto.DataType(
        map_type=proto.MapType(
            key=proto.DataType(scalar_type=proto.ScalarType.STRING),
            value=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(scalar_type=proto.ScalarType.STRING),
                    value=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(
                                scalar_type=proto.ScalarType.FLOAT
                            )
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
                    key=proto.DataType(scalar_type=proto.ScalarType.STRING),
                    value=proto.DataType(scalar_type=proto.ScalarType.FLOAT),
                )
            )
        )
    )
    assert get_datatype(List[Dict[str, List[float]]]) == proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(scalar_type=proto.ScalarType.STRING),
                    value=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(
                                scalar_type=proto.ScalarType.FLOAT
                            )
                        )
                    ),
                )
            )
        )
    )


def test_additional_dtypes():
    assert get_datatype(int) == proto.DataType(scalar_type=proto.ScalarType.INT)
    assert get_datatype(between(int, 1, 5)) == proto.DataType(
        between_type=proto.Between(
            scalar_type=proto.ScalarType.INT,
            min=proto.Param(int_val=1),
            max=proto.Param(int_val=5),
        )
    )
    assert get_datatype(between(int, 1, 5, False, True)) == proto.DataType(
        between_type=proto.Between(
            scalar_type=proto.ScalarType.INT,
            min=proto.Param(int_val=1),
            max=proto.Param(int_val=5),
            strict_min=False,
            strict_max=True,
        )
    )
    assert get_datatype(between(float, 1, 5, True, False)) == proto.DataType(
        between_type=proto.Between(
            scalar_type=proto.ScalarType.FLOAT,
            min=proto.Param(float_val=1),
            max=proto.Param(float_val=5),
            strict_min=True,
            strict_max=False,
        )
    )
    assert get_datatype(oneof(str, ["male", "female"])) == proto.DataType(
        one_of_type=proto.OneOf(
            scalar_type=proto.ScalarType.STRING,
            options=[
                proto.Param(str_val="male"),
                proto.Param(str_val="female"),
            ],
        )
    )
    assert get_datatype(oneof(int, [1, 2, 3])) == proto.DataType(
        one_of_type=proto.OneOf(
            scalar_type=proto.ScalarType.INT,
            options=[
                proto.Param(int_val=1),
                proto.Param(int_val=2),
                proto.Param(int_val=3),
            ],
        )
    )
    assert get_datatype(regex("[a-z]+")) == proto.DataType(regex_type="[a-z]+")


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
    schema = {
        "user_id": proto.DataType(scalar_type=proto.ScalarType.INT),
        "name": proto.DataType(scalar_type=proto.ScalarType.STRING),
        "age": proto.DataType(
            scalar_type=proto.ScalarType.INT, is_nullable=True
        ),
        "country": proto.DataType(scalar_type=proto.ScalarType.STRING),
        "Truth": proto.DataType(scalar_type=proto.ScalarType.BOOLEAN),
        "timestamp": proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP),
    }
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 0

    data.append([18234, "Monica", None, "Chile", True, yesterday])
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(schema, df)
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
    schema = {
        "doc_id": proto.DataType(scalar_type=proto.ScalarType.INT),
        "bert_embedding": proto.DataType(
            embedding_type=proto.EmbeddingType(embedding_size=4)
        ),
        "fast_text_embedding": proto.DataType(
            embedding_type=proto.EmbeddingType(embedding_size=3)
        ),
        "num_words": proto.DataType(scalar_type=proto.ScalarType.INT),
        "timestamp": proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP),
    }
    df = pd.DataFrame(data, columns=schema.keys())
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 0

    schema["bert_embedding"] = proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(scalar_type=proto.ScalarType.FLOAT)
        )
    )
    schema["fast_text_embedding"] = proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(scalar_type=proto.ScalarType.FLOAT)
        )
    )
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 0

    data = [
        [
            {"a": 1, "b": 2, "c": 3},
        ]
    ]
    schema = {
        "X": proto.DataType(
            map_type=proto.MapType(
                key=proto.DataType(scalar_type=proto.ScalarType.STRING),
                value=proto.DataType(scalar_type=proto.ScalarType.INT),
            )
        )
    }
    df = pd.DataFrame(data, columns=schema.keys())
    exceptions = data_schema_check(schema, df)
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
    schema = {
        "user_id": proto.DataType(scalar_type=proto.ScalarType.INT),
        "name": proto.DataType(scalar_type=proto.ScalarType.STRING),
        "age": proto.DataType(
            scalar_type=proto.ScalarType.INT, is_nullable=True
        ),
        "country": proto.DataType(scalar_type=proto.ScalarType.STRING),
        "timestamp": proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP),
    }
    exceptions = data_schema_check(schema, df)
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
    schema = {
        "doc_id": proto.DataType(scalar_type=proto.ScalarType.INT),
        "bert_embedding": proto.DataType(
            embedding_type=proto.EmbeddingType(embedding_size=4)
        ),
        "fast_text_embedding": proto.DataType(
            embedding_type=proto.EmbeddingType(embedding_size=3)
        ),
        "num_words": proto.DataType(scalar_type=proto.ScalarType.INT),
        "timestamp": proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP),
    }
    df = pd.DataFrame(data, columns=schema.keys())
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 1

    schema["bert_embedding"] = proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(scalar_type=proto.ScalarType.FLOAT)
        )
    )
    schema["fast_text_embedding"] = proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(scalar_type=proto.ScalarType.FLOAT)
        )
    )
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 0

    data = [[[123]]]
    schema = {
        "X": proto.DataType(
            map_type=proto.MapType(
                key=proto.DataType(scalar_type=proto.ScalarType.STRING),
                value=proto.DataType(scalar_type=proto.ScalarType.INT),
            )
        )
    }
    df = pd.DataFrame(data, columns=schema.keys())
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 1


def test_invalid_schema_additional_types():
    now = datetime.now()
    data = [
        [18232, "Ross9", 212, "transgender", 5, now],
    ]
    columns = ["user_id", "name", "age", "gender", "grade", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    schema = {
        "user_id": proto.DataType(scalar_type=proto.ScalarType.INT),
        "name": proto.DataType(regex_type="[A-Za-z]+[0-9]"),
        "age": proto.DataType(
            between_type=proto.Between(
                scalar_type=proto.ScalarType.INT,
                min=proto.Param(int_val=1),
                max=proto.Param(int_val=100),
            )
        ),
        "gender": proto.DataType(
            one_of_type=proto.OneOf(
                scalar_type=proto.ScalarType.STRING,
                options=[
                    proto.Param(str_val="male"),
                    proto.Param(str_val="female"),
                ],
            )
        ),
        "grade": proto.DataType(
            one_of_type=proto.OneOf(
                scalar_type=proto.ScalarType.INT,
                options=[
                    proto.Param(int_val=1),
                    proto.Param(int_val=2),
                    proto.Param(int_val=3),
                ],
            )
        ),
        "timestamp": proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP),
    }
    exceptions = data_schema_check(schema, df)
    assert len(exceptions) == 3
