from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Union

import pyarrow as pa

from fennel.lib.schema.schema import (
    get_pyarrow_field,
    get_pyarrow_datatype,
)


def test_PyArrowSchemaConversion():
    assert get_pyarrow_datatype(int) == pa.int64()
    assert get_pyarrow_field("x", Optional[int]) == pa.field(
        "x", pa.int64(), nullable=True
    )
    x: float = 1.0
    assert get_pyarrow_datatype(type(x)) == pa.float64()
    x: bool = True
    assert get_pyarrow_datatype(type(x)) == pa.bool_()
    x: str = "hello"
    assert get_pyarrow_datatype(type(x)) == pa.string()
    x: datetime = datetime.now()

    assert get_pyarrow_datatype(type(x)) == pa.timestamp("ns")
    assert get_pyarrow_field("x", Union[int, str]) == pa.union(
        [
            pa.field("x", pa.int64(), nullable=False),
            pa.field("x", pa.string(), nullable=False),
        ],
        mode="dense",
    )
    assert get_pyarrow_datatype(List[int]) == pa.list_(pa.int64())
    assert get_pyarrow_datatype(List[int]) == pa.list_(pa.int64())
    assert get_pyarrow_datatype(Dict[str, float]) == pa.map_(
        pa.string(), pa.float64()
    )
    assert get_pyarrow_datatype(Dict[str, Dict[str, List[float]]]) == pa.map_(
        pa.string(), pa.map_(pa.string(), pa.list_(pa.float64()))
    )
    assert get_pyarrow_datatype(List[Dict[str, float]]) == pa.list_(
        pa.map_(pa.string(), pa.float64())
    )
    assert get_pyarrow_datatype(List[Dict[str, List[float]]]) == pa.list_(
        pa.map_(pa.string(), pa.list_(pa.float64()))
    )
