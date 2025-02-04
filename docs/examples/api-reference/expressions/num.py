import pytest
import numpy as np
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
    expr = col("x").floor()  # equivalent to col("x").num.floor()
    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == int
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        1,
        -3,
        pd.NA,
    ]

    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})
    # /docsnip


def test_ceil():
    # docsnip ceil
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").ceil()  # equivalent to col("x").num.ceil()
    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == int
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        2,
        -2,
        pd.NA,
    ]

    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})
    # /docsnip


def test_round():
    # docsnip round
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").round()  # equivalent to col("x").num.round()

    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == int
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        1,
        -2,
        pd.NA,
    ]

    # can also explicit specify the number of decimals
    # docsnip-highlight next-line
    expr = col("x").round(1)

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([1.12, -2.37, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        1.1,
        -2.4,
        pd.NA,
    ]

    df = pd.DataFrame({"x": pd.Series([1, -2, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        1.0,
        -2.0,
        pd.NA,
    ]

    # /docsnip

    # invalid number of decimals
    with pytest.raises(Exception):
        expr = col("x").round(-1)

    with pytest.raises(Exception):
        expr = col("x").round(1.1)


def test_to_string():
    # docsnip to_string
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.to_string()

    # type is str or optional str
    assert expr.typeof(schema={"x": int}) == str
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[str]
    assert expr.typeof(schema={"x": float}) == str
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[str]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, None])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        "1.1",
        "-2.3",
        pd.NA,
    ]
    # /docsnip


def test_sqrt():
    # docsnip sqrt
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.sqrt()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, 4.0])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        1.0488088481701516,
        pd.NA,  # this is nan in pandas, sqrt of negative number
        2.0,
    ]
    # /docsnip


def test_log():
    # docsnip log
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.log(base=2.0)

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([1.1, -2.3, 4.0])})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        0.13750352374993502,
        pd.NA,  # nan in pandas, log of negative number
        2.0,
    ]
    # /docsnip


def test_pow():
    # docsnip pow
    from fennel.expr import col, lit

    # docsnip-highlight next-line
    expr = col("x").num.pow(lit(2))

    assert expr.typeof(schema={"x": int}) == int
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([1, 2, 4])})
    assert expr.eval(df, schema={"x": int}).tolist() == [
        1,
        4,
        16,
    ]

    # negative integer exponent raises error if base is also an integer
    with pytest.raises(Exception):
        expr = lit(2).num.pow(lit(-2))
        expr.eval(df, schema={"x": int})

    # but works if either base or exponent is a float
    expr = lit(2).num.pow(lit(-2.0))
    assert expr.eval(df, schema={"x": int}).tolist() == [
        0.25,
        0.25,
        0.25,
    ]
    # /docsnip


def test_sin():
    # docsnip sin
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.sin()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([0, np.pi / 2, np.pi])})
    assert expr.eval(df, schema={"x": float}).tolist() == [
        pytest.approx(0.0),
        pytest.approx(1.0),
        pytest.approx(0.0),
    ]
    # /docsnip


def test_cos():
    # docsnip cos
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.cos()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([0, np.pi / 2, np.pi])})
    assert expr.eval(df, schema={"x": float}).tolist() == [
        pytest.approx(1.0),
        pytest.approx(0.0),
        pytest.approx(-1.0),
    ]
    # /docsnip


def test_tan():
    # docsnip tan
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.tan()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([0, np.pi / 4, np.pi])})
    assert expr.eval(df, schema={"x": float}).tolist() == [
        pytest.approx(0.0),
        pytest.approx(1.0),
        pytest.approx(0.0),
    ]
    # /docsnip


def test_arcsin():
    # docsnip arcsin
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.arcsin()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([0, 1, -1, 2])})
    assert expr.eval(df, schema={"x": float}).tolist() == [
        0.0,
        pytest.approx(np.pi / 2),
        pytest.approx(-np.pi / 2),
        pd.NA,  # nan in pandas, arcsin of number greater than 1
    ]
    # /docsnip


def test_arccos():
    # docsnip arccos
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.arccos()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([0, 1, -1, 2])})
    assert expr.eval(df, schema={"x": float}).tolist() == [
        pytest.approx(np.pi / 2),
        pytest.approx(0.0),
        pytest.approx(np.pi),
        pd.NA,  # nan in pandas, arccos of number greater than 1
    ]
    # /docsnip


def test_arctan():
    # docsnip arctan
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").num.arctan()

    assert expr.typeof(schema={"x": int}) == float
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[float]
    assert expr.typeof(schema={"x": float}) == float
    assert expr.typeof(schema={"x": Optional[float]}) == Optional[float]

    df = pd.DataFrame({"x": pd.Series([0, 1, -1])})
    assert expr.eval(df, schema={"x": float}).tolist() == [
        0.0,
        pytest.approx(np.pi / 4),
        pytest.approx(-np.pi / 4),
    ]
    # /docsnip
