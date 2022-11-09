from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union

import pyarrow as pa

from fennel.lib.schema.schema import get_pyarrow_field, get_pyarrow_schema


def test_PyArrowSchemaConversion():
    assert get_pyarrow_schema(int) == pa.int64()
    assert get_pyarrow_field("x", Optional[int]) == pa.field(
        "x", pa.int64(), nullable=True
    )
    x: float = 1.0
    assert get_pyarrow_schema(type(x)) == pa.float64()
    x: bool = True
    assert get_pyarrow_schema(type(x)) == pa.bool_()
    x: str = "hello"
    assert get_pyarrow_schema(type(x)) == pa.string()
    x: bytes = b"hello"
    assert get_pyarrow_schema(type(x)) == pa.binary()
    x: datetime = datetime.now()
    assert get_pyarrow_schema(type(x)) == pa.timestamp("ns")
    assert get_pyarrow_field("x", Union[int, str]) == pa.union(
        [
            pa.field("x", pa.int64(), nullable=False),
            pa.field("x", pa.string(), nullable=False),
        ],
        mode="dense",
    )
    assert get_pyarrow_schema(Tuple[int]) == pa.struct(
        [pa.field("f0", pa.int64(), nullable=False)]
    )
    assert get_pyarrow_schema(Tuple[int, str]) == pa.struct(
        [
            pa.field("f0", pa.int64(), nullable=False),
            pa.field("f1", pa.string(), nullable=False),
        ]
    )
    assert get_pyarrow_schema(List[int]) == pa.list_(pa.int64())
    assert get_pyarrow_schema(List[int]) == pa.list_(pa.int64())
    assert get_pyarrow_schema(Set[int]) == pa.map_(pa.int64(), pa.null())
    assert get_pyarrow_schema(Dict[int, int]) == pa.map_(pa.int64(), pa.int64())
    assert get_pyarrow_schema(Dict[str, float]) == pa.map_(
        pa.string(), pa.float64()
    )
    assert get_pyarrow_schema(Dict[str, Dict[int, List[float]]]) == pa.map_(
        pa.string(), pa.map_(pa.int64(), pa.list_(pa.float64()))
    )
    assert get_pyarrow_schema(List[Dict[str, float]]) == pa.list_(
        pa.map_(pa.string(), pa.float64())
    )
    assert get_pyarrow_schema(List[Dict[str, List[float]]]) == pa.list_(
        pa.map_(pa.string(), pa.list_(pa.float64()))
    )
    assert get_pyarrow_schema(
        Tuple[List[Dict[str, float]], Dict[str, List[Dict[int, int]]]]
    ) == pa.struct(
        [
            pa.field(
                "f0",
                pa.list_(pa.map_(pa.string(), pa.float64())),
                nullable=False,
            ),
            pa.field(
                "f1",
                pa.map_(
                    pa.string(),
                    pa.list_(
                        pa.map_(
                            pa.int64(),
                            pa.int64(),
                        )
                    ),
                ),
                nullable=False,
            ),
        ]
    )
