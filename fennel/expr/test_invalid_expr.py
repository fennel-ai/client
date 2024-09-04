import pytest
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional, List
from fennel.datasets import dataset

from fennel.dtypes.dtypes import struct
from fennel.expr import col, when, lit
from fennel.expr.expr import InvalidExprException
from fennel.expr.visitor import ExprPrinter, FetchReferences
from fennel.expr.serializer import ExprSerializer
from google.protobuf.json_format import ParseDict  # type: ignore
from fennel.gen.expr_pb2 import Expr
from fennel.testing.test_utils import error_message


# Datetime test cases


def test_invalid_datetime():
    expr = col("a").str.strptime("%Y-%m-%d", "America/NonYork")
    df = pd.DataFrame(
        {"a": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04"]}
    )
    with pytest.raises(ValueError) as e:
        expr.eval(df, {"a": str})

    assert (
        str(e.value)
        == "Failed to compile expression: invalid timezone: America/NonYork"
    )

    df = pd.DataFrame(
        {
            "a": [
                1,
                2,
                3,
            ]
        }
    )
    expr = col("a").str.strptime("%Y-%m-%d", "America/New_York")
    with pytest.raises(ValueError) as e:
        expr.eval(df, {"a": str})
    assert (
        str(e.value)
        == 'Failed to evaluate expression: failed to eval expression: col(a).str.parse_datetime("%Y-%m-%d", timezone=""America/New_York""), error: invalid operation: conversion from `str` to `datetime[μs, America/New_York]` failed in column \'a\' for 3 out of 3 values: ["1", "2", "3"]'
    )

    with pytest.raises(ValueError) as e:
        expr.eval(df, {"a": int})
    assert (
        str(e.value)
        == """Failed to compile expression: invalid expression: expected string type for function 'Strptime { format: "%Y-%m-%d", timezone: Some("America/New_York") }' but found Int"""
    )


# Missing then for a when


def test_missing_then():
    expr = when(col("a") == 1)
    df = pd.DataFrame({"a": [1, 2, 3]})
    with pytest.raises(InvalidExprException) as e:
        expr.eval(df, {"a": int})
    assert str(e.value) == "THEN clause missing for WHEN clause col('a') == 1"

    with pytest.raises(AttributeError) as e:
        expr = when(col("a") == 1).when(col("a") == 2)
    assert str(e.value) == "'When' object has no attribute 'when'"

    with pytest.raises(AttributeError) as e:
        expr = when(col("a") == 1).otherwise(lit(0))
    assert str(e.value) == "'When' object has no attribute 'otherwise'"


@struct
class A:
    x: int
    y: int
    z: str


def test_struct():
    with pytest.raises(InvalidExprException) as e:
        _ = col("a").struct.get(col("b"))

    assert (
        str(e.value)
        == "invalid field access for struct, expected string but got col('b')"
    )


def test_invalid_parse():
    with pytest.raises(ValueError) as e:
        expr = col("a").str.parse(int)
        df = pd.DataFrame({"a": ['"A"', '"B"', '"C"']})
        expr.eval(df, {"a": str})
    assert (
        str(e.value)
        == "Failed to evaluate expression: failed to convert polars array to fennel array for type 'Int'"
    )
