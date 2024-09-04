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


def test_round():
    # docsnip round
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").round() # equivalent to col("x").num.round()

    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == int
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [1, -2, pd.NA]

    # can also explicit specify the number of decimals
    # docsnip-highlight next-line
    expr = col("x").round(1)

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([1.12, -2.37, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [1.1, -2.4, pd.NA]

    df = pd.DataFrame({"x": pd.Series([1, -2, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [1.0, -2.0, pd.NA]

    # /docsnip

    # invalid number of decimals
    with pytest.raises(Exception):
        expr = col("x").round(-1)
        
    with pytest.raises(Exception):
        expr = col("x").round(1.1)