from typing import Optional
import pytest


def test_typeof():
    # docsnip expr_binary_arithmetic
    import pandas as pd
    from fennel.expr import lit, col

    expr = col("x") + col("y")
    assert expr.typeof(schema={"x": int, "y": int}) == int
    assert expr.typeof(schema={"x": int, "y": float}) == float
    assert expr.typeof(schema={"x": float, "y": float}) == float
    assert (
        expr.typeof(schema={"x": Optional[float], "y": int}) == Optional[float]
    )

    df = pd.DataFrame({"x": [1, 2, None]})
    expr = lit(1) + col("x")
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [2, 3, pd.NA]

    expr = lit(1) - col("x")
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [0, -1, pd.NA]

    expr = lit(1) * col("x")
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [1, 2, pd.NA]

    expr = lit(1) / col("x")
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [
        1,
        0.5,
        pd.NA,
    ]
    # /docsnip
