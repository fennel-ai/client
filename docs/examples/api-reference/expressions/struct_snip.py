import pytest
from typing import Optional, List
import pandas as pd


def test_get():
    # docsnip get
    from fennel.expr import col
    from fennel.dtypes import struct

    @struct
    class MyStruct:
        f1: int
        f2: bool

    # docsnip-highlight next-line
    expr = col("x").struct.get("f1")
    assert expr.typeof(schema={"x": MyStruct}) == int

    # error to get a field that does not exist
    expr = col("x").struct.get("z")
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": MyStruct})

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [MyStruct(1, True), MyStruct(2, False), None],
        }
    )
    schema = {"x": Optional[MyStruct]}
    expr = col("x").struct.get("f1")
    assert expr.eval(df, schema=schema).tolist() == [1, 2, pd.NA]
    # /docsnip
