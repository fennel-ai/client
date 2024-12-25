from datetime import datetime

import pytest
from typing import Optional, List
import pandas as pd


def test_unary_not():
    # docsnip expr_unary_not
    from fennel.expr import lit

    # docsnip-highlight next-line
    expr = ~lit(True)
    assert expr.typeof() == bool

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": [1, 2, 3]})
    assert expr.eval(df, schema={"x": int}).tolist() == [False, False, False]
    # /docsnip


def test_col():
    # docsnip expr_col
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x") + col("y")

    # type of col("x") + col("y") changes based on the type of 'x' and 'y'
    assert expr.typeof(schema={"x": int, "y": float}) == float

    # okay if additional columns are provided
    assert expr.typeof(schema={"x": int, "y": float, "z": str}) == float

    # raises an error if the schema is not provided
    with pytest.raises(ValueError):
        expr.typeof(schema={})
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    with pytest.raises(ValueError):
        expr.typeof(schema={"z": int, "y": float})

    # can be evaluated with a dataframe
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3], "y": [1.0, 2.0, 3.0]})
    assert expr.eval(df, schema={"x": int, "y": float}).tolist() == [
        2.0,
        4.0,
        6.0,
    ]
    # /docsnip


def test_when_then():
    # docsnip expr_when_then
    from fennel.expr import when, col, InvalidExprException

    # docsnip-highlight next-line
    expr = when(col("x")).then(1).otherwise(0)

    # type depends on the type of the then and otherwise values
    assert expr.typeof(schema={"x": bool}) == int

    # raises an error if the schema is not provided
    with pytest.raises(ValueError):
        expr.typeof(schema={})
    # also when the predicate is not boolean
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})

    # can be evaluated with a dataframe
    import pandas as pd

    df = pd.DataFrame({"x": [True, False, True]})
    assert expr.eval(df, schema={"x": bool}).tolist() == [1, 0, 1]

    # not valid if only when is provided
    with pytest.raises(InvalidExprException):
        expr = when(col("x"))
        expr.typeof(schema={"x": bool})

    # if otherwise is not provided, it defaults to None
    expr = when(col("x")).then(1)
    assert expr.typeof(schema={"x": bool}) == Optional[int]
    # /docsnip


def test_isnull():
    # docsnip expr_isnull
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").isnull()

    # type of isnull is always boolean
    assert expr.typeof(schema={"x": Optional[int]}) == bool

    # also works for non-optional types, where it's always False
    assert expr.typeof(schema={"x": float}) == bool

    # raises an error if the schema is not provided
    with pytest.raises(ValueError):
        expr.typeof(schema={})

    # can be evaluated with a dataframe
    import pandas as pd

    df = pd.DataFrame({"x": pd.Series([1, 2, None], dtype=pd.Int64Dtype())})
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [
        False,
        False,
        True,
    ]
    # /docsnip


def test_fillnull():
    # docsnip expr_fillnull
    from fennel.expr import col, lit

    # docsnip-highlight next-line
    expr = col("x").fillnull(lit(10))

    # type of fillnull depends both on type of 'x' and the literal 1
    assert expr.typeof(schema={"x": Optional[int]}) == int
    assert expr.typeof(schema={"x": float}) == float

    # raises an error if the schema is not provided
    with pytest.raises(ValueError):
        expr.typeof(schema={})

    # can be evaluated with a dataframe
    import pandas as pd

    expr = col("x").fillnull(lit(10))
    df = pd.DataFrame({"x": pd.Series([1, 2, None], dtype=pd.Int64Dtype())})
    assert expr.eval(df, schema={"x": Optional[float]}).tolist() == [
        1.0,
        2.0,
        10.0,
    ]
    # /docsnip


def test_lit():
    # docsnip expr_lit
    from fennel.expr import lit, col

    # docsnip-highlight next-line
    expr = lit(1)

    # lits don't need a schema to be evaluated
    assert expr.typeof() == int

    # can be evaluated with a dataframe
    expr = col("x") + lit(1)
    df = pd.DataFrame({"x": pd.Series([1, 2, None], dtype=pd.Int64Dtype())})
    assert expr.eval(df, schema={"x": Optional[int]}).tolist() == [2, 3, pd.NA]
    # /docsnip


def test_now():
    # docsnip expr_now
    from fennel.expr import now, col

    # docsnip-highlight next-line
    expr = now().dt.since(col("birthdate"), "year")

    assert (
        expr.typeof(schema={"birthdate": Optional[datetime]}) == Optional[int]
    )

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {"birthdate": [datetime(1997, 12, 24), datetime(2001, 1, 21), None]}
    )
    assert expr.eval(df, schema={"birthdate": Optional[datetime]}).tolist() == [
        27,
        23,
        pd.NA,
    ]
    # /docsnip


def test_repeat():
    # docsnip repeat
    from fennel.expr import repeat, col

    # docsnip-highlight next-line
    expr = repeat(col("x"), col("y"))

    assert expr.typeof(schema={"x": bool, "y": int}) == List[bool]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": [True, False, True], "y": [1, 2, 3]})
    assert expr.eval(df, schema={"x": bool, "y": int}).tolist() == [
        [True],
        [False, False],
        [True, True, True],
    ]
    # /docsnip


def test_zip():
    # docsnip zip
    from fennel.lib.schema import struct
    from fennel.expr import col

    @struct
    class MyStruct:
        a: int
        b: float

    # docsnip-highlight next-line
    expr = MyStruct.zip(a=col("x"), b=col("y"))

    expected = List[MyStruct]
    schema = {"x": List[int], "y": List[float]}
    assert expr.matches_type(expected, schema)

    # note that output is truncated to the length of the shortest list
    df = pd.DataFrame(
        {"x": [[1, 2], [3, 4], []], "y": [[1.0, 2.0], [3.0], [4.0]]}
    )
    assert expr.eval(
        df, schema={"x": List[int], "y": List[float]}
    ).tolist() == [
        [MyStruct(a=1, b=1.0), MyStruct(a=2, b=2.0)],
        [MyStruct(a=3, b=3.0)],
        [],
    ]
    # /docsnip
