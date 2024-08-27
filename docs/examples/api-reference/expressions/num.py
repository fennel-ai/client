import pytest
from typing import Optional
import pandas as pd

def test_abs():
    # docsnip abs
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").abs()  # equivalent to col("x").num.abs()

    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1, -2, pd.NA], dtype=pd.Int64Dtype())})
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [1, 2, pd.NA]

    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})
    # /docsnip


def test_floor():
    # docsnip floor
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").floor() # equivalent to col("x").num.floor()
    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == int
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [1, -3, pd.NA]

    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})
    # /docsnip


def test_ceil():
    # docsnip ceil
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").ceil() # equivalent to col("x").num.ceil()
    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == int
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [2, -2, pd.NA]

    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})
    # /docsnip