import pytest
from typing import Optional, List
import pandas as pd
from datetime import datetime


def test_year():
    # docsnip year
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.year

    # year works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [2024, 2024, 2024]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="US/Eastern").year
    assert expr.eval(df, schema=schema).tolist() == [2023, 2024, 2024]
    # /docsnip


def test_month():
    # docsnip month
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.month

    # month works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [1, 1, 1]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="US/Eastern").month
    assert expr.eval(df, schema=schema).tolist() == [12, 1, 1]
    # /docsnip


def test_from_epoch():
    # docsnip from_epoch
    from fennel.expr import col, from_epoch

    # docsnip-highlight next-line
    expr = from_epoch(col("x"), unit="second")

    # from_epoch works for any int or optional int type
    assert expr.typeof(schema={"x": int}) == datetime
    assert expr.typeof(schema={"x": Optional[int]}) == Optional[datetime]

    # can be evaluated with a dataframe
    df = pd.DataFrame({"x": [1714857600, 1714857601, 1714857602]})
    schema = {"x": int}
    expected = [
        pd.Timestamp("2024-05-04 21:20:00", tz="UTC"),
        pd.Timestamp("2024-05-04 21:20:01", tz="UTC"),
        pd.Timestamp("2024-05-04 21:20:02", tz="UTC"),
    ]
    assert expr.eval(df, schema=schema).tolist() == expected
    # /docsnip


def test_day():
    # docsnip day
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.day

    # day works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [1, 1, 1]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="US/Eastern").day
    assert expr.eval(df, schema=schema).tolist() == [31, 1, 1]
    # /docsnip


def test_hour():
    # docsnip hour
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.hour

    # hour works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [0, 10, 20]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="US/Eastern").hour
    assert expr.eval(df, schema=schema).tolist() == [19, 5, 15]
    # /docsnip


def test_minute():
    # docsnip minute
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.minute

    # minute works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [0, 0, 20]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="US/Eastern").minute
    assert expr.eval(df, schema=schema).tolist() == [0, 0, 20]
    # /docsnip


def test_second():
    # docsnip second
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.second

    # second works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:01", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:02", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:03", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [1, 2, 3]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="Asia/Kathmandu").second
    assert expr.eval(df, schema=schema).tolist() == [1, 2, 3]
    # /docsnip


def test_since_epoch():
    # docsnip since_epoch
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.since_epoch()

    # since_epoch works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == int
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[int]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    expected = [1704067200, 1704103200, 1704140400]
    assert expr.eval(df, schema=schema).tolist() == expected

    # can also change the unit of time
    # docsnip-highlight next-line
    expr = col("x").dt.since_epoch(unit="minute")
    assert expr.eval(df, schema=schema).tolist() == [
        28401120,
        28401720,
        28402340,
    ]
    # /docsnip

    expr = col("x").dt.since_epoch(unit="day")
    assert expr.eval(df, schema=schema).tolist() == [
        19723,
        19723,
        19723,
    ]

    expr = col("x").dt.since_epoch(unit="hour")
    assert expr.eval(df, schema=schema).tolist() == [
        473352,
        473362,
        473372,
    ]
    expr = col("x").dt.since_epoch(unit="millisecond")
    assert expr.eval(df, schema=schema).tolist() == [
        1704067200000,
        1704103200000,
        1704140400000,
    ]

    expr = col("x").dt.since_epoch(unit="microsecond")
    assert expr.eval(df, schema=schema).tolist() == [
        1704067200000000,
        1704103200000000,
        1704140400000000,
    ]

    expr = col("x").dt.since_epoch(unit="week")
    assert expr.eval(df, schema=schema).tolist() == [
        2817,
        2817,
        2817,
    ]

    with pytest.raises(ValueError):
        col("x").dt.since_epoch(unit="nanosecond")


def test_since():
    # docsnip since
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.since(col("y"))

    # since works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime, "y": datetime}) == int
    assert (
        expr.typeof(schema={"x": Optional[datetime], "y": datetime})
        == Optional[int]
    )

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-01 20:20:00", tz="UTC"),
            ],
            "y": [
                pd.Timestamp("2023-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2023-01-02 10:00:00", tz="UTC"),
                pd.Timestamp("2023-01-03 20:20:00", tz="UTC"),
            ],
        }
    )
    schema = {"x": datetime, "y": datetime}
    expected = [31536000, 31449600, 31363200]
    assert expr.eval(df, schema=schema).tolist() == expected

    # can also change the unit of time
    # docsnip-highlight next-line
    expr = col("x").dt.since(col("y"), unit="minute")
    assert expr.eval(df, schema=schema).tolist() == [
        525600,
        524160,
        522720,
    ]
    # /docsnip

    expr = col("x").dt.since(col("y"), unit="day")
    assert expr.eval(df, schema=schema).tolist() == [
        365,
        364,
        363,
    ]

    expr = col("x").dt.since(col("y"), unit="hour")
    assert expr.eval(df, schema=schema).tolist() == [
        8760,
        8736,
        8712,
    ]


def test_strftime():
    # docsnip strftime
    from fennel.expr import col

    # docsnip-highlight next-line
    expr = col("x").dt.strftime("%Y-%m-%d")

    # strftime works for any datetime type or optional datetime type
    assert expr.typeof(schema={"x": datetime}) == str
    assert expr.typeof(schema={"x": Optional[datetime]}) == Optional[str]

    # can be evaluated with a dataframe
    df = pd.DataFrame(
        {
            "x": [
                pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
                pd.Timestamp("2024-01-02 10:00:00", tz="UTC"),
                pd.Timestamp("2024-01-03 20:20:00", tz="UTC"),
            ]
        }
    )
    schema = {"x": datetime}
    assert expr.eval(df, schema=schema).tolist() == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
    ]

    # also works with timezone aware datetimes
    # docsnip-highlight next-line
    expr = col("x").dt.with_tz(timezone="US/Eastern").strftime("%Y-%m-%d")
    assert expr.eval(df, schema=schema).tolist() == [
        "2023-12-31",
        "2024-01-02",
        "2024-01-03",
    ]
    # /docsnip


def test_datetime():
    # docsnip datetime
    # docsnip-highlight next-line
    from fennel.expr import datetime as dt

    # docsnip-highlight next-line
    expr = dt(year=2024, month=1, day=1)

    # datetime works for any datetime type or optional datetime type
    assert expr.typeof() == datetime

    # can be evaluated with a dataframe
    df = pd.DataFrame({"dummy": [1, 2, 3]})
    assert expr.eval(df, schema={"dummy": int}).tolist() == [
        pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
        pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
        pd.Timestamp("2024-01-01 00:00:00", tz="UTC"),
    ]
    # can provide timezone
    # docsnip-highlight next-line
    expr = dt(year=2024, month=1, day=1, timezone="US/Eastern")
    assert expr.eval(df, schema={"dummy": int}).tolist() == [
        pd.Timestamp("2024-01-01 00:00:00", tz="US/Eastern"),
        pd.Timestamp("2024-01-01 00:00:00", tz="US/Eastern"),
        pd.Timestamp("2024-01-01 00:00:00", tz="US/Eastern"),
    ]
    # /docsnip
