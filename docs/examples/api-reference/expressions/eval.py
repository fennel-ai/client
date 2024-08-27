from typing import Optional
import pytest

def test_typeof():
    # docsnip expr_typeof
    from fennel.expr import lit, col

    expr = lit(1) + col("amount")
    # type of 1 + col('amount') changes based on the type of 'amount'
    assert expr.typeof(schema={"amount": int}) == int
    assert expr.typeof(schema={"amount": float}) == float
    assert expr.typeof(schema={"amount": Optional[int]}) == Optional[int]
    assert expr.typeof(schema={"amount": Optional[float]}) == Optional[float]


    # typeof raises an error if the schema is not provided
    with pytest.raises(ValueError):
        expr.typeof(schema={})

    # of when the expression won't be valid with the schema
    with pytest.raises(ValueError):
        expr.typeof(schema={"amount": str})

    # no need to provide schema if the expression is constant
    const = lit(1)
    assert const.typeof(schema={}) == int
    # /docsnip


def test_eval():
    # docsnip expr_eval
    import pandas as pd
    from fennel.expr import lit, col

    expr = lit(1) + col("amount")
    # value of 1 + col('amount') changes based on the type of 'amount'
    df = pd.DataFrame({"amount": [1, 2, 3]})
    assert expr.eval(df, schema={"amount": int}).tolist() == [2, 3, 4]

    df = pd.DataFrame({"amount": [1.0, 2.0, 3.0]})
    assert expr.eval(df, schema={"amount": float}).tolist() == [2.0, 3.0, 4.0]

    # raises an error if the schema is not provided
    with pytest.raises(TypeError):
        expr.eval(df)

    # dataframe doesn't have the required column even though schema is provided
    df = pd.DataFrame({"other": [1, 2, 3]})
    with pytest.raises(KeyError):
        expr.eval(df, schema={"amount": int})
        
    # /docsnip