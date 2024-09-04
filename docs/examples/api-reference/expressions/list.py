import pytest
from typing import Optional, List
import pandas as pd


def test_len():
    # docsnip len
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.len()

    # len works for any list type or optional list type
    assert expr.typeof(schema={"x": List[int]}) == int
    assert expr.typeof(schema={"x": Optional[List[float]]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": [[1, 2, 3], [4, 5], [], None]})
    schema = {"x": Optional[List[int]]}
    assert expr.eval(df, schema=schema).tolist() == [3, 2, 0, pd.NA]

    # schema of column must be list of something
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_has_null():
    # docsnip has_null
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.hasnull()

    # len works for any list type or optional list type
    assert expr.typeof(schema={"x": List[int]}) == bool
    assert expr.typeof(schema={"x": Optional[List[float]]}) == Optional[bool]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": [[1, 2, 3], [4, 5, None], [], None]})
    schema = {"x": Optional[List[Optional[int]]]}
    assert expr.eval(df, schema=schema).tolist() == [False, True, False, pd.NA]

    # schema of column must be list of something
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_contains():
    # docsnip contains
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.contains(col("y"))

    # contains works for only list types
    assert expr.typeof(schema={"x": List[int], "y": int}) == bool
    assert (
        expr.typeof(schema={"x": Optional[List[float]], "y": float})
        == Optional[bool]
    )

    # however doesn't work if item is not of the same type as the list elements
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": List[int], "y": str})

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [[1, 2, 3], [4, 5, None], [4, 5, None], None, []],
            "y": [1, 5, 3, 4, None],
        }
    )
    schema = {"x": Optional[List[Optional[int]]], "y": Optional[int]}
    assert expr.eval(df, schema=schema).tolist() == [
        True,
        True,
        pd.NA,
        pd.NA,
        False,
    ]

    # schema of column must be list of something
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_at():
    # docsnip at
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.at(col("y"))

    # contains works for only list types, index can be int/optional[int]
    assert expr.typeof(schema={"x": List[int], "y": int}) == Optional[int]
    assert expr.typeof(schema={"x": List[str], "y": int}) == Optional[str]

    schema = {"x": Optional[List[float]], "y": float}
    with pytest.raises(Exception):
        expr.typeof(schema=schema)

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [[1, 2, 3], [4, 5, None], [4, 5, None], None],
            "y": [1, 5, 0, 4],
        }
    )
    schema = {"x": Optional[List[Optional[int]]], "y": int}
    assert expr.eval(df, schema=schema).tolist() == [2, pd.NA, 4, pd.NA]

    # schema of column must be list of something
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_at_negative():
    # docsnip at_negative
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.at(col("y"))

    # negative indices until -len(list) are allowed and do reverse indexing
    # beyond that, start returning None like other out-of-bounds indices
    df = pd.DataFrame(
        {
            "x": [[1, 2, 3], [4, 5, None], [4, 5, None], None],
            "y": [-1, -5, -2, -4],
        }
    )
    schema = {"x": Optional[List[Optional[int]]], "y": int}
    assert expr.eval(df, schema=schema).tolist() == [3, pd.NA, 5, pd.NA]
    # /docsnip
