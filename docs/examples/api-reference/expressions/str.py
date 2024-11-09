import pytest
from typing import Optional, List
import pandas as pd


def test_concact():
    # docsnip concat
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.concat(col("y"))

    assert expr.typeof(schema={"x": str, "y": str}) == str
    assert expr.typeof(schema={"x": str, "y": Optional[str]}) == Optional[str]
    assert expr.typeof(schema={"x": Optional[str], "y": str}) == Optional[str]
    assert (
        expr.typeof(schema={"x": Optional[str], "y": Optional[str]})
        == Optional[str]
    )

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": ["hello", "world", "some", None],
            "y": [" world", " hello", None, None],
        }
    )
    schema = {"x": Optional[str], "y": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [
        "hello world",
        "world hello",
        pd.NA,
        pd.NA,
    ]

    # schema of both columns must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})

    with pytest.raises(Exception):
        expr.typeof(schema={"x": str, "y": int})
    # /docsnip


def test_contains():
    # docsnip contains
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.contains(col("y"))

    assert expr.typeof(schema={"x": str, "y": str}) == bool
    assert expr.typeof(schema={"x": str, "y": Optional[str]}) == Optional[bool]
    assert expr.typeof(schema={"x": Optional[str], "y": str}) == Optional[bool]
    assert (
        expr.typeof(schema={"x": Optional[str], "y": Optional[str]})
        == Optional[bool]
    )

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": ["hello", "world", "some", None],
            "y": ["ell", "random", None, None],
        }
    )
    schema = {"x": Optional[str], "y": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [True, False, pd.NA, pd.NA]

    # schema of both columns must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})

    with pytest.raises(Exception):
        expr.typeof(schema={"x": str, "y": int})
    # /docsnip


def test_startswith():
    # docsnip startswith
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.startswith(col("y"))

    assert expr.typeof(schema={"x": str, "y": str}) == bool
    assert expr.typeof(schema={"x": str, "y": Optional[str]}) == Optional[bool]
    assert expr.typeof(schema={"x": Optional[str], "y": str}) == Optional[bool]
    assert (
        expr.typeof(schema={"x": Optional[str], "y": Optional[str]})
        == Optional[bool]
    )

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": ["hello", "world", "some", None],
            "y": ["he", "rld", None, None],
        }
    )
    schema = {"x": Optional[str], "y": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [True, False, pd.NA, pd.NA]

    # schema of both columns must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})

    with pytest.raises(Exception):
        expr.typeof(schema={"x": str, "y": int})
    # /docsnip


def test_endswith():
    # docsnip endswith
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.endswith(col("y"))

    assert expr.typeof(schema={"x": str, "y": str}) == bool
    assert expr.typeof(schema={"x": str, "y": Optional[str]}) == Optional[bool]
    assert expr.typeof(schema={"x": Optional[str], "y": str}) == Optional[bool]
    assert (
        expr.typeof(schema={"x": Optional[str], "y": Optional[str]})
        == Optional[bool]
    )

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": ["hello", "world", "some", None],
            "y": ["lo", "wor", None, None],
        }
    )
    schema = {"x": Optional[str], "y": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [True, False, pd.NA, pd.NA]

    # schema of both columns must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": str})

    with pytest.raises(Exception):
        expr.typeof(schema={"x": str, "y": int})
    # /docsnip


def test_lower():
    # docsnip lower
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.lower()

    assert expr.typeof(schema={"x": str}) == str
    assert expr.typeof(schema={"x": Optional[str]}) == Optional[str]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": ["HeLLo", "World", "some", None]})
    schema = {"x": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [
        "hello",
        "world",
        "some",
        pd.NA,
    ]

    # schema of column must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_upper():
    # docsnip upper
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.upper()

    assert expr.typeof(schema={"x": str}) == str
    assert expr.typeof(schema={"x": Optional[str]}) == Optional[str]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": ["HeLLo", "World", "some", None]})
    schema = {"x": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [
        "HELLO",
        "WORLD",
        "SOME",
        pd.NA,
    ]

    # schema of column must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_len():
    # docsnip len
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").str.len()

    assert expr.typeof(schema={"x": str}) == int
    assert expr.typeof(schema={"x": Optional[str]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": ["hello", "world", "some", None]})
    schema = {"x": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [5, 5, 4, pd.NA]

    # schema of column must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})
    # /docsnip


def test_parse_basic():
    # docsnip parse_basic
    from fennel.expr import col, lit

    # docsnip-highlight next-line
    expr = col("x").str.parse(list[int])

    assert expr.typeof(schema={"x": str}) == List[int]
    assert expr.typeof(schema={"x": Optional[str]}) == Optional[List[int]]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": ["[1, 2, 3]", "[4, 5]", None]})
    schema = {"x": Optional[str]}
    assert expr.eval(df, schema=schema).tolist() == [[1, 2, 3], [4, 5], pd.NA]

    # schema of column must be str
    with pytest.raises(ValueError):
        expr.typeof(schema={"x": int})

    # can use this to parse several common types
    df = pd.DataFrame({"x": ["1"]})
    schema = {"x": str}
    cases = [
        ("1", int, 1),
        ("1.1", float, 1.1),
        ("true", bool, True),
        ("false", bool, False),
        ('"hi"', str, "hi"),
    ]
    for case in cases:
        expr = lit(case[0]).str.parse(case[1])
        assert expr.eval(df, schema).tolist() == [case[2]]
    # /docsnip


def test_parse_invalid():
    # docsnip parse_invalid
    from fennel.expr import col, lit

    invalids = [
        ("False", bool),  # "False" is not valid json, "false" is
        ("hi", str),  # "hi" is not valid json, "\"hi\"" is
        ("[1, 2, 3", List[int]),
        ("1.1.1", float),
    ]
    for invalid in invalids:
        expr = lit(invalid[0]).str.parse(invalid[1])
        df = pd.DataFrame({"x": ["1"]})
        schema = {"x": str}
        with pytest.raises(Exception):
            expr.eval(df, schema)
    # /docsnip


def test_parse_struct():
    # docsnip parse_struct
    from fennel.expr import col, lit
    from fennel.dtypes import struct

    @struct
    class MyStruct:
        x: int
        y: Optional[bool]

    cases = [
        ('{"x": 1, "y": true}', MyStruct(1, True)),
        ('{"x": 2, "y": null}', MyStruct(2, None)),
        ('{"x": 3}', MyStruct(3, None)),
    ]
    for case in cases:
        expr = lit(case[0]).str.parse(MyStruct)
        df = pd.DataFrame({"x": ["1"]})
        schema = {"x": str}
        found = expr.eval(df, schema).tolist()
        assert len(found) == 1
        assert found[0].x == case[1].x
    # /docsnip

    # can also parse a list of structs
    df = pd.DataFrame(
        {"x": ['[{"x": 1, "y": true}, {"x": 2, "y": null}, null]']}
    )
    schema = {"x": str}
    target = List[Optional[MyStruct]]
    expr = col("x").str.parse(target)
    found = expr.eval(df, schema).tolist()
    assert len(found) == 1
    assert len(found[0]) == 3

    assert found[0][0].x == 1
    assert found[0][0].y == True  # noqa
    assert found[0][1].x == 2
    assert pd.isna(found[0][1].y)
    # /docsnip


def test_strptime():
    # docsnip strptime
    from fennel.expr import col
    from datetime import datetime

    # docsnip-highlight next-line
    expr = col("x").str.strptime("%Y-%m-%d")

    assert expr.typeof(schema={"x": str}) == datetime
    assert expr.typeof(schema={"x": Optional[str]}) == Optional[datetime]

    df = pd.DataFrame({"x": ["2021-01-01", "2021-02-01", None]})
    schema = {"x": Optional[str]}
    assert expr.eval(df, schema).tolist() == [
        pd.Timestamp(2021, 1, 1, tz="UTC"),
        pd.Timestamp(2021, 2, 1, tz="UTC"),
        pd.NaT,
    ]

    # can also provide a timezone
    expr = col("x").str.strptime("%Y-%m-%d", timezone="Asia/Tokyo")

    assert expr.eval(df, schema).tolist() == [
        pd.Timestamp(2021, 1, 1, tz="Asia/Tokyo"),
        pd.Timestamp(2021, 2, 1, tz="Asia/Tokyo"),
        pd.NaT,
    ]

    # error on invalid format - %L is not a valid format
    expr = col("x").str.strptime("%Y-%m-%d %L)")
    with pytest.raises(Exception):
        expr.eval(df, schema)

    # error on invalid timezone
    expr = col("x").str.strptime("%Y-%m-%d", timezone="invalid")
    with pytest.raises(Exception):
        expr.eval(df, schema)
    # /docsnip


def test_json_extract():
    # docsnip json_extract
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("s").str.json_extract("$.x.y")

    # return type is always Optional[str]
    assert expr.typeof(schema={"s": str}) == Optional[str]
    assert expr.typeof(schema={"s": Optional[str]}) == Optional[str]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {"s": ['{"x": {"y": "hello"}}', '{"x": {"y": 1}}', "{}", None]}
    )
    schema = {"s": Optional[str]}
    # NOTE that the integer value 1 is returned as a string and not an int
    # also invalid paths (e.g. "$.x.y" in case 3 of "{}") return null
    assert expr.eval(df, schema).tolist() == ["hello", "1", pd.NA, pd.NA]
    # /docsnip


def test_split():
    # docsnip split
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("s").str.split(",")

    assert expr.typeof(schema={"s": str}) == List[str]
    assert expr.typeof(schema={"s": Optional[str]}) == Optional[List[str]]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"s": ["a,b,c", "d,e", "f", None]})
    schema = {"s": Optional[str]}
    assert expr.eval(df, schema).tolist() == [
        ["a", "b", "c"],
        ["d", "e"],
        ["f"],
        pd.NA,
    ]
    # /docsnip
