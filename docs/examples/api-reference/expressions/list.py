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


def test_list_sum():
    # docsnip sum
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.sum()

    # works for lists of int/float or their optional versions
    assert expr.typeof(schema={"x": List[int]}) == int
    assert expr.typeof(schema={"x": Optional[List[float]]}) == Optional[float]

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame({"x": [[1, 2, 3], [4, 5, None], [], None]})
    schema = {"x": Optional[List[Optional[int]]]}
    assert expr.eval(df, schema=schema).tolist() == [6, pd.NA, 0, pd.NA]
    # /docsnip


def test_list_min():
    # docsnip min
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.min()

    # works for lists of int/float or their optional versions
    assert expr.typeof(schema={"x": List[int]}) == Optional[int]
    assert expr.typeof(schema={"x": Optional[List[float]]}) == Optional[float]

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame({"x": [[1, 2, 3], [4, 5, None], [], None]})
    schema = {"x": Optional[List[Optional[int]]]}
    assert expr.eval(df, schema=schema).tolist() == [1, pd.NA, pd.NA, pd.NA]
    # /docsnip


def test_list_max():
    # docsnip max
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.max()

    # works for lists of int/float or their optional versions
    assert expr.typeof(schema={"x": List[int]}) == Optional[int]
    assert expr.typeof(schema={"x": Optional[List[float]]}) == Optional[float]

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame({"x": [[1, 2, 3], [4, 5, None], [], None]})
    schema = {"x": Optional[List[Optional[int]]]}
    assert expr.eval(df, schema=schema).tolist() == [3, pd.NA, pd.NA, pd.NA]
    # /docsnip


def test_list_mean():
    # docsnip mean
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.mean()

    # works for lists of int/float or their optional versions
    assert expr.typeof(schema={"x": List[int]}) == Optional[float]
    assert expr.typeof(schema={"x": Optional[List[float]]}) == Optional[float]

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame({"x": [[1, 2, 3], [4, 5, None], [], None]})
    schema = {"x": Optional[List[Optional[int]]]}
    assert expr.eval(df, schema=schema).tolist() == [2.0, pd.NA, pd.NA, pd.NA]
    # /docsnip


def test_list_all():
    # docsnip all
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.all()

    # works for lists of int/float or their optional versions
    assert expr.typeof(schema={"x": List[bool]}) == bool
    assert expr.typeof(schema={"x": List[Optional[bool]]}) == Optional[bool]
    assert (
        expr.typeof(schema={"x": Optional[List[Optional[bool]]]})
        == Optional[bool]
    )

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame(
        {"x": [[True, True], [True, False], [], None, [True, None]]}
    )
    schema = {"x": Optional[List[Optional[bool]]]}
    assert expr.eval(df, schema=schema).tolist() == [
        True,
        False,
        True,
        pd.NA,
        pd.NA,
    ]
    # /docsnip


def test_list_any():
    # docsnip any
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").list.any()

    # works for lists of int/float or their optional versions
    assert expr.typeof(schema={"x": List[bool]}) == bool
    assert expr.typeof(schema={"x": List[Optional[bool]]}) == Optional[bool]
    assert (
        expr.typeof(schema={"x": Optional[List[Optional[bool]]]})
        == Optional[bool]
    )

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame(
        {"x": [[True, True], [True, False], [], None, [True, None]]}
    )
    schema = {"x": Optional[List[Optional[bool]]]}
    assert expr.eval(df, schema=schema).tolist() == [
        True,
        True,
        False,
        pd.NA,
        True,
    ]
    # /docsnip


def test_list_filter():
    # docsnip filter
    from fennel.expr import col, var

    # docsnip-highlight next-line
    expr = col("x").list.filter("x", var("x") % 2 == 0)

    # works as long as predicate is valid and evaluates to bool
    assert expr.typeof(schema={"x": List[int]}) == List[int]
    assert expr.typeof(schema={"x": List[float]}) == List[float]

    with pytest.raises(Exception):
        expr.typeof(schema={"x": List[str]})

    # can be evaluated as well
    df = pd.DataFrame({"x": [[1, 2, 3], [], [1, 2, -2], None, [1, 3]]})
    schema = {"x": Optional[List[int]]}
    assert expr.eval(df, schema=schema).tolist() == [
        [2],
        [],
        [2, -2],
        pd.NA,
        [],
    ]
    # /docsnip


def test_list_map():
    # docsnip map
    from fennel.expr import col, var

    # docsnip-highlight next-line
    expr = col("x").list.map("x", var("x") % 2)

    # works as long as predicate is valid
    assert expr.typeof(schema={"x": List[int]}) == List[int]
    assert expr.typeof(schema={"x": List[Optional[int]]}) == List[Optional[int]]

    # can be evaluated as well
    df = pd.DataFrame({"x": [[1, 2, 3], [], [1, 2, None], None, [1, 3]]})
    schema = {"x": Optional[List[Optional[int]]]}
    expected = [[1, 0, 1], [], [1, 0, pd.NA], pd.NA, [1, 1]]
    assert expr.eval(df, schema=schema).tolist() == expected
    # /docsnip
