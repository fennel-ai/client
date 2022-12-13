from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Union

import pandas as pd
import numpy as np

from fennel.lib.schema.schema import get_datatype, schema_check
import fennel.gen.schema_pb2 as proto


def test_GetDataType():
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
    exceptions = schema_check(schema, df)
    assert len(exceptions) == 0

    data.append([18234, "Monica", None, "Chile", True, yesterday])
    df = pd.DataFrame(data, columns=columns)
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
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
    exceptions = schema_check(schema, df)
    assert len(exceptions) == 1
