import inspect
import pytest
import random
import pandas as pd
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Dict, Optional, List
from fennel.datasets import dataset

from fennel.dtypes.dtypes import struct
from fennel.expr import col, when, lit
from fennel.expr.expr import TimeUnit, from_epoch, make_struct
from fennel.expr.visitor import ExprPrinter, FetchReferences
from fennel.expr.serializer import ExprSerializer
from google.protobuf.json_format import ParseDict  # type: ignore
from fennel.gen.expr_pb2 import Expr
from fennel.internal_lib.utils.utils import is_user_defined_class
from fennel.testing.test_utils import error_message


def test_const_expr():
    expr = lit(1, int)
    assert expr.typeof({}) == int
    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    df2 = expr.eval(df, {"a": int})
    assert df2.tolist() == [1, 1, 1, 1]


def test_basic_expr1():
    expr = (col("num") + col("d")).isnull()
    df = pd.DataFrame({"num": [1, 2, 3, 4], "d": [5, 6, 7, 8]})
    assert expr.typeof({"num": int, "d": int}) == bool
    ret = expr.eval(df, {"num": int, "d": int})
    assert ret.tolist() == [False, False, False, False]
    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"num", "d"}


def test_unary_expr():
    invert = ~col("a")
    assert invert.typeof({"a": bool}) == bool
    df = pd.DataFrame({"a": [True, False, True, False]})
    ret = invert.eval(df, {"a": bool})
    assert ret.tolist() == [False, True, False, True]
    ref_extractor = FetchReferences()
    ref_extractor.visit(invert.root)
    assert ref_extractor.refs == {"a"}

    negate = -col("a")
    assert negate.typeof({"a": int}) == int
    assert negate.typeof({"a": float}) == float
    assert negate.typeof({"a": Optional[float]}) == Optional[float]
    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    ret = negate.eval(df, {"a": int})
    assert ret.tolist() == [-1, -2, -3, -4]
    ref_extractor = FetchReferences()
    ref_extractor.visit(negate.root)
    assert ref_extractor.refs == {"a"}


def test_basic_expr2():
    expr = col("a") + col("b") + 3
    printer = ExprPrinter()
    expected = "((col('a') + col('b')) + 3)"
    assert expected == printer.print(expr.root)
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    d = {
        "binary": {
            "left": {
                "binary": {
                    "left": {"ref": {"name": "a"}},
                    "right": {"ref": {"name": "b"}},
                }
            },
            "right": {
                "jsonLiteral": {"literal": "3", "dtype": {"intType": {}}}
            },
        }
    }
    expected_expr = ParseDict(d, Expr())
    assert expected_expr == proto_expr, error_message(proto_expr, expected_expr)

    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})

    @dataset
    class TestDataset:
        a: int
        b: int
        t: datetime

    ret = expr.eval(df, {"a": int, "b": int})
    assert ret.tolist() == [9, 11, 13, 15]
    assert expr.typeof({"a": int, "b": int}) == int

    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"a", "b"}


def test_math_expr():
    expr = (col("a").num.floor() + 3.2).num.ceil()
    printer = ExprPrinter()
    expected = "CEIL((FLOOR(col('a')) + 3.2))"
    assert expected == printer.print(expr.root)
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    d = {
        "mathFn": {
            "operand": {
                "binary": {
                    "left": {
                        "mathFn": {
                            "operand": {"ref": {"name": "a"}},
                            "fn": {"floor": {}},
                        }
                    },
                    "right": {
                        "jsonLiteral": {
                            "literal": "3.2",
                            "dtype": {"doubleType": {}},
                        }
                    },
                }
            },
            "fn": {"ceil": {}},
        }
    }
    expected_expr = ParseDict(d, Expr())
    assert expected_expr == proto_expr, error_message(proto_expr, expected_expr)
    df = pd.DataFrame({"a": [1.4, 2.9, 3.1, 4.8], "b": ["a", "b", "c", "d"]})
    ret = expr.eval(df, {"a": float})
    assert ret.tolist() == [5, 6, 7, 8]
    assert expr.typeof({"a": float}) == int

    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"a"}

    expr = (
        when(col("a").num.floor() > 5)
        .then(col("b"))
        .when(col("a") > 3)
        .then(col("a"))
        .otherwise(1 + col("d"))
    )
    df = pd.DataFrame(
        {
            "a": [1.4, 3.2, 6.1, 4.8],
            "b": [100, 200, 300, 400],
            "d": [1, 2, 3, 4],
        }
    )
    ret = expr.eval(df, {"a": float, "b": int, "d": int})
    assert ret.tolist() == [2.0, 3.2, 300, 4.8]
    assert expr.typeof({"a": float, "b": int, "d": int}) == float

    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"a", "b", "d"}


def test_bool_expr():
    expr = (col("a") == 5) | ((col("b") == "random") & (col("c") == 3.2))
    printer = ExprPrinter()
    expected = """((col('a') == 5) or ((col('b') == "random") and (col('c') == 3.2)))"""
    assert expected == printer.print(expr.root)

    df = pd.DataFrame(
        {
            "a": [4, 5, 3, 4],
            "b": ["radfsfom", "random", "random", "random"],
            "c": [3.2, 1.2, 3.4, 3.2],
        }
    )
    ret = expr.eval(df, {"a": int, "b": str, "c": float})
    assert ret.tolist() == [False, True, False, True]
    assert expr.typeof({"a": int, "b": str, "c": float}) == bool

    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"a", "b", "c"}


def test_str_expr():
    expr = (col("a").str.concat(col("b"))).str.lower().len().ceil()
    printer = ExprPrinter()
    expected = "CEIL(LEN(LOWER(col('a') + col('b'))))"
    assert expected == printer.print(expr.root)
    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"a", "b"}

    expr = (
        when(
            ((col("a").str.concat(col("b"))).str.upper()).str.contains(col("c"))
        )
        .then(col("b"))
        .otherwise("No Match")
    )
    expected = """WHEN CONTAINS(UPPER(col('a') + col('b')), col('c')) THEN col('b') ELSE "No Match\""""
    assert expected == printer.print(expr.root)
    ref_extractor = FetchReferences()
    assert ref_extractor.fetch(expr.root) == {"a", "b", "c"}
    df = pd.DataFrame(
        {
            "a": ["p", "BRandomS", "CRandomStrin", "tqz"],
            "b": ["aa", "tring", "g", "d"],
            "c": [
                "RANDOMSTRING",
                "RANDOMSTRING",
                "RANDOMSTRING",
                "RANDOMSTRING",
            ],
        }
    )
    ret = expr.eval(df, {"a": str, "b": str, "c": str})
    assert ret.tolist() == [
        "No Match",
        "tring",
        "g",
        "No Match",
    ]
    assert expr.typeof({"a": str, "b": str, "c": str}) == str
    expr = (
        when(col("a").str.contains("p"))
        .then(col("b"))
        .when(col("b").str.contains("b"))
        .then(col("a"))
        .when(col("c").str.contains("C"))
        .then(col("c"))
        .otherwise("No Match")
    )
    expected = """WHEN CONTAINS(col('a'), "p") THEN col('b') WHEN CONTAINS(col('b'), "b") THEN col('a') WHEN CONTAINS(col('c'), "C") THEN col('c') ELSE "No Match\""""
    assert expected == printer.print(expr.root)
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    d = {
        "case": {
            "whenThen": [
                {
                    "when": {
                        "stringFn": {
                            "string": {"ref": {"name": "a"}},
                            "fn": {
                                "contains": {
                                    "element": {
                                        "jsonLiteral": {
                                            "literal": '"p"',
                                            "dtype": {"stringType": {}},
                                        }
                                    }
                                }
                            },
                        }
                    },
                    "then": {"ref": {"name": "b"}},
                },
                {
                    "when": {
                        "stringFn": {
                            "string": {"ref": {"name": "b"}},
                            "fn": {
                                "contains": {
                                    "element": {
                                        "jsonLiteral": {
                                            "literal": '"b"',
                                            "dtype": {"stringType": {}},
                                        }
                                    }
                                }
                            },
                        }
                    },
                    "then": {"ref": {"name": "a"}},
                },
                {
                    "when": {
                        "stringFn": {
                            "string": {"ref": {"name": "c"}},
                            "fn": {
                                "contains": {
                                    "element": {
                                        "jsonLiteral": {
                                            "literal": '"C"',
                                            "dtype": {"stringType": {}},
                                        }
                                    }
                                }
                            },
                        }
                    },
                    "then": {"ref": {"name": "c"}},
                },
            ],
            "otherwise": {
                "jsonLiteral": {
                    "literal": '"No Match"',
                    "dtype": {"stringType": {}},
                }
            },
        }
    }
    expected_expr = ParseDict(d, Expr())
    assert expected_expr == proto_expr, error_message(proto_expr, expected_expr)

    df = pd.DataFrame(
        {
            "a": ["p", "q", "r", "t"],
            "b": ["a", "b", "c", "d"],
            "c": ["A", "B", "C", "D"],
        }
    )
    ret = expr.eval(df, {"a": str, "b": str, "c": str})
    assert expr.typeof({"a": str, "b": str, "c": str}) == str


def test_dict_op():
    expr = (col("a").dict.get("x") + col("a").dict.get("y")).num.ceil() + col(
        "a"
    ).dict.len()
    printer = ExprPrinter()
    expected = (
        """(CEIL((col('a').get("x") + col('a').get("y"))) + LEN(col('a')))"""
    )
    ref_extractor = FetchReferences()
    ref_extractor.visit(expr.root)
    assert ref_extractor.refs == {"a"}
    assert expected == printer.print(expr.root)
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    d = {
        "binary": {
            "left": {
                "mathFn": {
                    "operand": {
                        "binary": {
                            "left": {
                                "dictFn": {
                                    "dict": {"ref": {"name": "a"}},
                                    "fn": {
                                        "get": {
                                            "field": {
                                                "jsonLiteral": {
                                                    "literal": '"x"',
                                                    "dtype": {"stringType": {}},
                                                }
                                            }
                                        }
                                    },
                                }
                            },
                            "right": {
                                "dictFn": {
                                    "dict": {"ref": {"name": "a"}},
                                    "fn": {
                                        "get": {
                                            "field": {
                                                "jsonLiteral": {
                                                    "literal": '"y"',
                                                    "dtype": {"stringType": {}},
                                                }
                                            }
                                        }
                                    },
                                }
                            },
                        }
                    },
                    "fn": {"ceil": {}},
                }
            },
            "right": {
                "dictFn": {"dict": {"ref": {"name": "a"}}, "fn": {"len": {}}}
            },
        }
    }
    expected_expr = ParseDict(d, Expr())
    assert expected_expr == proto_expr, error_message(proto_expr, expected_expr)
    assert expr.typeof({"a": Dict[str, int]}) == int


@dataclass
class ExprTestCase:
    expr: Expr
    df: pd.DataFrame
    schema: Dict[str, Any]

    # Expected Results
    display: str
    refs: set
    eval_result: List[Any]
    expected_dtype: Optional[Any]

    # Proto test are optional
    proto_json: Optional[Dict]


def compare_values(received, expected, dtype):
    """
    Compares two lists of values. If the dtype is a user defined class, we compare
    the fields of the class. Otherwise, we compare the values directly.
    """
    if is_user_defined_class(dtype):
        for act, exp in zip(received, expected):
            for field in fields(dtype):
                r = getattr(act, field.name)
                e = getattr(exp, field.name)
                if not is_user_defined_class(field.type):
                    if (
                        not isinstance(e, list)
                        and not isinstance(r, list)
                        and pd.isna(e)
                        and pd.isna(r)
                    ):
                        continue
                    assert (
                        r == e
                    ), f"Expected {e}, got {r} for field {field.name} in struct {act}"
                else:
                    compare_values([r], [e], field.type)
    else:
        assert (
            list(received) == expected
        ), f"Expected {expected}, got {received} for dtype {dtype}"


def check_test_case(test_case: ExprTestCase):
    # Test print test case
    if test_case.display:
        printer = ExprPrinter()
        assert printer.print(test_case.expr) == test_case.display

    # Test FetchReferences test case
    ref_extractor = FetchReferences()
    assert ref_extractor.fetch(test_case.expr) == test_case.refs

    # Test ExprSerializer
    if test_case.proto_json:
        expected_expr = ParseDict(test_case.proto_json, Expr())
        serializer = ExprSerializer()
        assert (
            serializer.serialize(test_case.expr) == expected_expr
        ), error_message(serializer.serialize(test_case.expr), expected_expr)

    # Test type inference
    # If it is a dataclass, we check if all fields are present
    if is_user_defined_class(test_case.expected_dtype):
        if not hasattr(
            test_case.expr.typeof(test_case.schema), "__annotations__"
        ):
            assert False, "Expected a dataclass"
        for field in fields(test_case.expected_dtype):
            assert (
                field.name
                in test_case.expr.typeof(test_case.schema).__annotations__
            )
    else:
        assert (
            test_case.expr.typeof(test_case.schema) == test_case.expected_dtype
        )

    # Test eval
    ret = test_case.expr.eval(test_case.df, test_case.schema)
    compare_values(ret, test_case.eval_result, test_case.expected_dtype)


# Datetime test cases


def test_datetime_expr():
    test_case = ExprTestCase(
        expr=(col("a").str.strptime("%Y-%m-%d")),
        df=pd.DataFrame(
            {"a": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04"]}
        ),
        schema={"a": str},
        display="STRPTIME(col('a'), %Y-%m-%d, UTC)",
        refs={"a"},
        eval_result=[
            pd.Timestamp("2021-01-01 00:00:00+0000", tz="UTC"),
            pd.Timestamp("2021-01-02 00:00:00+0000", tz="UTC"),
            pd.Timestamp("2021-01-03 00:00:00+0000", tz="UTC"),
            pd.Timestamp("2021-01-04 00:00:00+0000", tz="UTC"),
        ],
        expected_dtype=datetime,
        proto_json=None,
    )
    check_test_case(test_case)
    test_case = ExprTestCase(
        expr=(col("a").str.strptime("%Y-%m-%d", "America/New_York")),
        df=pd.DataFrame(
            {"a": ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04"]}
        ),
        schema={"a": str},
        display="STRPTIME(col('a'), %Y-%m-%d, America/New_York)",
        refs={"a"},
        eval_result=[
            pd.Timestamp("2021-01-01 05:00:00+0000", tz="UTC"),
            pd.Timestamp("2021-01-02 05:00:00+0000", tz="UTC"),
            pd.Timestamp("2021-01-03 05:00:00+0000", tz="UTC"),
            pd.Timestamp("2021-01-04 05:00:00+0000", tz="UTC"),
        ],
        expected_dtype=datetime,
        proto_json=None,
    )
    check_test_case(test_case)


@struct
class B:
    p: int
    q: str


@struct
class A:
    x: int
    y: int
    z: str


@struct
class Nested:
    a: A
    b: B
    c: List[int]


@struct
class OptionalA:
    w: int
    x: Optional[int]
    y: Optional[int]
    z: Optional[str]


def test_parse():
    cases = [
        ExprTestCase(
            expr=(col("a").str.parse(int)),
            df=pd.DataFrame({"a": ["1", "2", "3", "4"]}),
            schema={"a": str},
            display="PARSE(col('a'), <class 'int'>)",
            refs={"a"},
            eval_result=[1, 2, 3, 4],
            expected_dtype=int,
            proto_json=None,
        ),
        # Parse a struct
        ExprTestCase(
            expr=(col("a").str.parse(A)),
            df=pd.DataFrame(
                {
                    "a": [
                        '{"x": 1, "y": 2, "z": "a"}',
                        '{"x": 2, "y": 3, "z": "b"}',
                    ]
                }
            ),
            schema={"a": str},
            display="PARSE(col('a'), <class 'fennel.expr.test_expr.A'>)",
            refs={"a"},
            eval_result=[A(1, 2, "a"), A(2, 3, "b")],
            expected_dtype=A,
            proto_json=None,
        ),
        # Parse a list of integers
        ExprTestCase(
            expr=(col("a").str.parse(List[int])),
            df=pd.DataFrame({"a": ["[1, 2, 3]", "[4, 5, 6]"]}),
            schema={"a": str},
            display="PARSE(col('a'), typing.List[int])",
            refs={"a"},
            eval_result=[[1, 2, 3], [4, 5, 6]],
            expected_dtype=List[int],
            proto_json=None,
        ),
        # Parse a nested struct
        ExprTestCase(
            expr=(col("a").str.parse(Nested)),
            df=pd.DataFrame(
                {
                    "a": [
                        '{"a": {"x": 1, "y": 2, "z": "a"}, "b": {"p": 1, "q": "b"}, "c": [1, 2, 3]}'
                    ]
                }
            ),
            schema={"a": str},
            display="PARSE(col('a'), <class 'fennel.expr.test_expr.Nested'>)",
            refs={"a"},
            eval_result=[Nested(A(1, 2, "a"), B(1, "b"), [1, 2, 3])],
            expected_dtype=Nested,
            proto_json=None,
        ),
        # Parse floats
        ExprTestCase(
            expr=(col("a").str.parse(float)),
            df=pd.DataFrame({"a": ["1.1", "2.2", "3.3", "4.4"]}),
            schema={"a": str},
            display="PARSE(col('a'), <class 'float'>)",
            refs={"a"},
            eval_result=[1.1, 2.2, 3.3, 4.4],
            expected_dtype=float,
            proto_json=None,
        ),
        # Parse bool
        ExprTestCase(
            expr=(col("a").str.parse(bool)),
            df=pd.DataFrame({"a": ["true", "false", "true", "false"]}),
            schema={"a": str},
            display="PARSE(col('a'), <class 'bool'>)",
            refs={"a"},
            eval_result=[True, False, True, False],
            expected_dtype=bool,
            proto_json=None,
        ),
        # Parse strings
        ExprTestCase(
            expr=(col("a").str.parse(str)),
            df=pd.DataFrame({"a": ['"a1"', '"b"', '"c"', '"d"']}),
            schema={"a": str},
            display="PARSE(col('a'), <class 'str'>)",
            refs={"a"},
            eval_result=["a1", "b", "c", "d"],
            expected_dtype=str,
            proto_json=None,
        ),
        # Parse optional strings to structs
        ExprTestCase(
            expr=(
                (
                    col("a")
                    .fillnull('{"x": 12, "y": 21, "z": "rando"}')
                    .str.parse(A)
                )
            ),
            df=pd.DataFrame(
                {
                    "a": [
                        '{"x": 1, "y": 2, "z": "a"}',
                        '{"x": 2, "y": 3, "z": "b"}',
                        None,
                        None,
                    ]
                }
            ),
            schema={"a": Optional[str]},
            display=None,
            refs={"a"},
            eval_result=[
                A(1, 2, "a"),
                A(2, 3, "b"),
                A(12, 21, "rando"),
                A(12, 21, "rando"),
            ],
            expected_dtype=A,
            proto_json={},
        ),
        ExprTestCase(
            expr=(
                (
                    col("a")
                    .fillnull("""{"x": 12, "y": 21, "z": "rando"}""")
                    .str.parse(A)
                )
            ),
            df=pd.DataFrame(
                {
                    "a": [
                        '{"x": 1, "y": 2, "z": "a"}',
                        '{"x": 2, "y": 3, "z": "b"}',
                        None,
                        None,
                    ]
                }
            ),
            schema={"a": Optional[str]},
            display=None,
            refs={"a"},
            eval_result=[
                A(1, 2, "a"),
                A(2, 3, "b"),
                A(12, 21, "rando"),
                A(12, 21, "rando"),
            ],
            expected_dtype=A,
            proto_json={},
        ),
        ExprTestCase(
            expr=((col("a").fillnull("""{"w": 12}""").str.parse(OptionalA))),
            df=pd.DataFrame(
                {
                    "a": [
                        '{"w": 1, "x": 2, "y": 3, "z": "a"}',
                        '{"w": 2, "x": 3, "y": 4, "z": "b"}',
                        None,
                        None,
                    ]
                }
            ),
            schema={"a": Optional[str]},
            display=None,
            refs={"a"},
            eval_result=[
                OptionalA(1, 2, 3, "a"),
                OptionalA(2, 3, 4, "b"),
                OptionalA(12, pd.NA, pd.NA, pd.NA),
                OptionalA(12, pd.NA, pd.NA, pd.NA),
            ],
            expected_dtype=OptionalA,
            proto_json={},
        ),
    ]

    for case in cases:
        check_test_case(case)


def test_list():
    test_cases = [
        ExprTestCase(
            expr=(col("a").list.get(0)),
            df=pd.DataFrame({"a": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]}),
            schema={"a": List[int]},
            display="col('a')[0]",
            refs={"a"},
            eval_result=[1, 4, 7],
            expected_dtype=Optional[int],
            proto_json=None,
        ),
        # Get index where index is an expression
        ExprTestCase(
            expr=(col("a").list.get(col("b") + col("c"))),
            df=pd.DataFrame(
                {
                    "a": [[1, 2, 3, 4], [4, 5, 6, 12], [7, 8, 9, 19]],
                    "b": [
                        0,
                        1,
                        2,
                    ],
                    "c": [1, 2, 0],
                }
            ),
            schema={"a": List[int], "b": int, "c": int},
            display="col('a')[(col('b') + col('c'))]",
            refs={"a", "b", "c"},
            eval_result=[2, 12, 9],
            expected_dtype=Optional[int],
            proto_json=None,
        ),
        # Out of bounds index
        ExprTestCase(
            expr=(col("a").list.get(col("b"))),
            df=pd.DataFrame(
                {
                    "a": [[1, 2, 3, 4], [4, 5, 6, 12], [7, 8, 9, 19]],
                    "b": [0, 21, 5],
                }
            ),
            schema={"a": List[int], "b": int},
            display="col('a')[col('b')]",
            refs={"a", "b"},
            eval_result=[1, pd.NA, pd.NA],
            expected_dtype=Optional[int],
            proto_json=None,
        ),
        # List contains
        ExprTestCase(
            expr=(col("a").list.contains(3)),
            df=pd.DataFrame({"a": [[1, 2, 3], [4, 5, 6], [7, 8, 9]]}),
            schema={"a": List[int]},
            display="CONTAINS(col('a'), 3)",
            refs={"a"},
            eval_result=[True, False, False],
            expected_dtype=bool,
            proto_json=None,
        ),
        # List contains with expression
        ExprTestCase(
            expr=(col("a").list.contains(col("b") * col("c"))),
            df=pd.DataFrame(
                {
                    "a": [[1, 2, 3], [4, 15, 6], [7, 8, 9]],
                    "b": [1, 5, 10],
                    "c": [2, 3, 4],
                }
            ),
            schema={"a": List[int], "b": int, "c": int},
            display="CONTAINS(col('a'), (col('b') * col('c')))",
            refs={"a", "b", "c"},
            eval_result=[True, True, False],
            expected_dtype=bool,
            proto_json=None,
        ),
        # List contains for list of strings
        ExprTestCase(
            expr=(col("a2").list.contains(col("b2"))),
            df=pd.DataFrame(
                {
                    "a2": [
                        ["a", "b", "c"],
                        ["d", "e", "f"],
                        ["g", "h", "i"],
                        ["a", "b", "c"],
                    ],
                    "b2": ["a", "e", "c", "d"],
                }
            ),
            schema={"a2": List[str], "b2": str},
            display="""CONTAINS(col('a2'), col('b2'))""",
            refs={"a2", "b2"},
            eval_result=[True, True, False, False],
            expected_dtype=bool,
            proto_json=None,
        ),
        # Support struct inside a list
        ExprTestCase(
            expr=(
                col("a").list.contains(
                    make_struct({"x": 1, "y": 2, "z": "a"}, A)
                )
            ),
            df=pd.DataFrame(
                {"a": [[A(1, 2, "a"), A(2, 3, "b"), A(4, 5, "c")]]}
            ),
            schema={"a": List[A]},
            display="""CONTAINS(col('a'), STRUCT(x=1, y=2, z="a"))""",
            refs={"a"},
            eval_result=[True],
            expected_dtype=bool,
            proto_json=None,
        ),
        ExprTestCase(
            expr=(col("a").list.len()),
            df=pd.DataFrame(
                {"a": [[A(1, 2, "a"), A(2, 3, "b"), A(4, 5, "c")]]}
            ),
            schema={"a": List[A]},
            display="LEN(col('a'))",
            refs={"a"},
            eval_result=[3],
            expected_dtype=int,
            proto_json=None,
        ),
        # List length
        ExprTestCase(
            expr=(col("a").list.len()),
            df=pd.DataFrame({"a": [[1, 2, 3], [4, 5, 6, 12], [7, 8, 9, 19]]}),
            schema={"a": List[int]},
            display="LEN(col('a'))",
            refs={"a"},
            eval_result=[3, 4, 4],
            expected_dtype=int,
            proto_json=None,
        ),
        # Empty list length
        ExprTestCase(
            expr=(col("a").list.len()),
            df=pd.DataFrame({"a": [[], [4, 5, 6, 12], [7, 8, 9, 19]]}),
            schema={"a": List[int]},
            display="LEN(col('a'))",
            refs={"a"},
            eval_result=[0, 4, 4],
            expected_dtype=int,
            proto_json=None,
        ),
    ]

    for test_case in test_cases:
        check_test_case(test_case)


def test_struct():
    cases = [
        # Get a field from a struct
        ExprTestCase(
            expr=(col("a").struct.get("x")),
            df=pd.DataFrame({"a": [A(1, 2, "a"), A(2, 3, "b"), A(4, 5, "c")]}),
            schema={"a": A},
            display="col('a').x",
            refs={"a"},
            eval_result=[1, 2, 4],
            expected_dtype=int,
            proto_json=None,
        ),
        ExprTestCase(
            expr=(col("a").struct.get("x") + col("a").struct.get("y")),
            df=pd.DataFrame({"a": [A(1, 2, "a"), A(2, 3, "b"), A(4, 5, "c")]}),
            schema={"a": A},
            display="(col('a').x + col('a').y)",
            refs={"a"},
            eval_result=[3, 5, 9],
            expected_dtype=int,
            proto_json=None,
        ),
    ]

    for case in cases:
        check_test_case(case)


def test_datetime():
    cases = [
        # Extract year from a datetime
        ExprTestCase(
            expr=(col("a").dt.year),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-01-02 00:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-01-03 00:00:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="DATEPART(col('a'), TimeUnit.YEAR)",
            refs={"a"},
            eval_result=[2021, 2021, 2021],
            expected_dtype=int,
            proto_json=None,
        ),
        # Extract month from a datetime
        ExprTestCase(
            expr=(col("a").dt.month),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="DATEPART(col('a'), TimeUnit.MONTH)",
            refs={"a"},
            eval_result=[1, 2, 3],
            expected_dtype=int,
            proto_json=None,
        ),
        # Extract week from a datetime
        ExprTestCase(
            expr=(col("a").dt.week),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="DATEPART(col('a'), TimeUnit.WEEK)",
            refs={"a"},
            eval_result=[53, 5, 9],
            expected_dtype=int,
            proto_json=None,
        ),
        # Since a datetime, unit is days
        ExprTestCase(
            expr=(
                col("a").dt.since(
                    lit("2021-01-01 00:01:00+0000").str.strptime(
                        "%Y-%m-%d %H:%M:%S%z"
                    ),
                    unit="day",
                )
            ),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="""SINCE(col('a'), STRPTIME("2021-01-01 00:01:00+0000", %Y-%m-%d %H:%M:%S%z, UTC), unit=TimeUnit.DAY)""",
            refs={"a"},
            eval_result=[0, 32, 61],
            expected_dtype=int,
            proto_json=None,
        ),
        # Since a datetime, unit is years
        ExprTestCase(
            expr=(
                col("a").dt.since(
                    lit("2021-01-01 00:01:00+0000").str.strptime(
                        "%Y-%m-%d %H:%M:%S%z"
                    ),
                    TimeUnit.YEAR,
                )
            ),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2026-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="""SINCE(col('a'), STRPTIME("2021-01-01 00:01:00+0000", %Y-%m-%d %H:%M:%S%z, UTC), unit=TimeUnit.YEAR)""",
            refs={"a"},
            eval_result=[0, 0, 5],
            expected_dtype=int,
            proto_json=None,
        ),
        # Since epoch days
        ExprTestCase(
            expr=(col("a").dt.since_epoch(unit="day")),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="SINCE_EPOCH(col('a'), unit=TimeUnit.DAY)",
            refs={"a"},
            eval_result=[18628, 18660, 18689],
            expected_dtype=int,
            proto_json=None,
        ),
        # Since epoch years
        ExprTestCase(
            expr=(col("a").dt.since_epoch(TimeUnit.YEAR)),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2026-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="SINCE_EPOCH(col('a'), unit=TimeUnit.YEAR)",
            refs={"a"},
            eval_result=[51, 51, 56],
            expected_dtype=int,
            proto_json=None,
        ),
        # Strftime
        ExprTestCase(
            expr=(col("a").dt.strftime("%Y-%m-%d")),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="STRFTIME(col('a'), %Y-%m-%d)",
            refs={"a"},
            eval_result=["2021-01-01", "2021-02-02", "2021-03-03"],
            expected_dtype=str,
            proto_json=None,
        ),
        # Complex strftime
        ExprTestCase(
            expr=(col("a").dt.strftime("%Y-%m-%d %H:%M:%S")),
            df=pd.DataFrame(
                {
                    "a": [
                        pd.Timestamp("2021-01-01 00:01:00+0000", tz="UTC"),
                        pd.Timestamp("2021-02-02 03:00:00+0000", tz="UTC"),
                        pd.Timestamp("2021-03-03 00:30:00+0000", tz="UTC"),
                    ]
                }
            ),
            schema={"a": datetime},
            display="STRFTIME(col('a'), %Y-%m-%d %H:%M:%S)",
            refs={"a"},
            eval_result=[
                "2021-01-01 00:01:00",
                "2021-02-02 03:00:00",
                "2021-03-03 00:30:00",
            ],
            expected_dtype=str,
            proto_json=None,
        ),
    ]

    for case in cases:
        check_test_case(case)


def random_datetime(start_year=1970, end_year=2024):
    year = random.randint(start_year, end_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # Keep it simple to avoid month/day issues
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    dt = datetime(year, month, day, hour, minute, second)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def test_parse_str_to_and_from_datetime():
    df = pd.DataFrame({"a": [random_datetime() for _ in range(100)]})
    schema = {"a": str}
    expr = (
        col("a")
        .str.strptime("%Y-%m-%d %H:%M:%S")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    ret = expr.eval(df, schema)
    assert ret.tolist() == df["a"].tolist()


def test_make_struct():
    cases = [
        # Make a struct
        ExprTestCase(
            expr=(
                make_struct(
                    {"x": col("a"), "y": col("a") + col("b"), "z": "constant"},
                    A,
                )
            ),
            df=pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}),
            schema={"a": int, "b": int},
            display="""STRUCT(x=col('a'), y=(col('a') + col('b')), z="constant")""",
            refs={"a", "b"},
            eval_result=[
                A(1, 5, "constant"),
                A(2, 7, "constant"),
                A(3, 9, "constant"),
            ],
            expected_dtype=A,
            proto_json=None,
        ),
        # Make a nested struct
        ExprTestCase(
            expr=(
                make_struct(
                    {
                        "a": make_struct(
                            {"x": col("a"), "y": col("b"), "z": col("c")}, A
                        ),
                        "b": make_struct({"p": col("d"), "q": col("e")}, B),
                        "c": col("f"),
                    },
                    Nested,
                )
            ),
            df=pd.DataFrame(
                {
                    "a": [1, 2, 3],
                    "b": [4, 5, 6],
                    "c": ["str_1", "str_2", "str_3"],
                    "d": [10, 11, 12],
                    "e": ["a", "b", "c"],
                    "f": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                }
            ),
            schema={
                "a": int,
                "b": int,
                "c": str,
                "d": int,
                "e": str,
                "f": List[int],
            },
            display="""STRUCT(a=STRUCT(x=col('a'), y=col('b'), z=col('c')), b=STRUCT(p=col('d'), q=col('e')), c=col('f'))""",
            refs={"a", "b", "c", "d", "e", "f"},
            eval_result=[
                Nested(A(1, 4, "str_1"), B(10, "a"), [1, 2, 3]),
                Nested(A(2, 5, "str_2"), B(11, "b"), [4, 5, 6]),
                Nested(A(3, 6, "str_3"), B(12, "c"), [7, 8, 9]),
            ],
            expected_dtype=Nested,
            proto_json=None,
        ),
    ]

    for case in cases:
        check_test_case(case)


def test_from_epoch():
    cases = [
        # From epoch
        ExprTestCase(
            expr=(from_epoch(col("a"), unit="second")),
            df=pd.DataFrame({"a": [1725321570, 1725321570]}),
            schema={"a": int},
            display="""FROM_EPOCH(col('a'), unit=TimeUnit.SECOND)""",
            refs={"a"},
            eval_result=[
                pd.Timestamp("2024-09-02 23:59:30+0000", tz="UTC"),
                pd.Timestamp("2024-09-02 23:59:30+0000", tz="UTC"),
            ],
            expected_dtype=datetime,
            proto_json=None,
        ),
        # From epoch years
        ExprTestCase(
            expr=(from_epoch(col("a") * col("b"), unit="millisecond")),
            df=pd.DataFrame({"a": [1725321570, 1725321570], "b": [1000, 1000]}),
            schema={"a": int, "b": int},
            display="""FROM_EPOCH((col('a') * col('b')), unit=TimeUnit.MILLISECOND)""",
            refs=set(["a", "b"]),
            eval_result=[
                pd.Timestamp("2024-09-02 23:59:30+0000", tz="UTC"),
                pd.Timestamp("2024-09-02 23:59:30+0000", tz="UTC"),
            ],
            expected_dtype=datetime,
            proto_json=None,
        ),
    ]

    for case in cases:
        check_test_case(case)


def test_fillnull():
    cases = [
        ExprTestCase(
            expr=(col("a").fillnull(0)),
            df=pd.DataFrame({"a": [1, 2, None, 4]}),
            schema={"a": Optional[int]},
            display="FILL_NULL(col('a'), 0)",
            refs={"a"},
            eval_result=[1, 2, 0, 4],
            expected_dtype=int,
            proto_json=None,
        ),
        ExprTestCase(
            expr=(col("a").fillnull("missing")),
            df=pd.DataFrame({"a": ["a", "b", None, "d"]}),
            schema={"a": Optional[str]},
            display="FILL_NULL(col('a'), \"missing\")",
            refs={"a"},
            eval_result=["a", "b", "missing", "d"],
            expected_dtype=str,
            proto_json=None,
        ),
        # Fillnull for datetime
        ExprTestCase(
            expr=(
                col("a")
                .str.strptime("%Y-%m-%d")
                .fillnull((lit("2021-01-01").str.strptime("%Y-%m-%d", "UTC")))
            ),
            df=pd.DataFrame({"a": ["2021-01-01", None, "2021-01-03"]}),
            schema={"a": Optional[str]},
            display="""FILL_NULL(STRPTIME(col('a'), %Y-%m-%d, UTC), STRPTIME("2021-01-01", %Y-%m-%d, UTC))""",
            refs={"a"},
            eval_result=[
                pd.Timestamp("2021-01-01 00:00:00+0000", tz="UTC"),
                pd.Timestamp("2021-01-01 00:00:00+0000", tz="UTC"),
                pd.Timestamp("2021-01-03 00:00:00+0000", tz="UTC"),
            ],
            expected_dtype=datetime,
            proto_json=None,
        ),
        # TODO(Nikhil): Add support for filling nulls for complex types
        # Fill null for struct
        # ExprTestCase(
        #     expr=(col("a").fillnull(lit(A(1, 2, "a"), A))),
        #     df=pd.DataFrame({"a": [A(1, 2, "a"), None, A(3, 4, "b")]}),
        #     schema={"a": Optional[A]},
        #     display="""FILL_NULL(col('a'), {"x": 1, "y": 2, "z": "a"})""",
        #     refs={"a"},
        #     eval_result=[A(1, 2, "a"), A(1, 2, "a"), A(3, 4, "b")],
        #     expected_dtype=A,
        #     proto_json=None,
        # ),
        # Fill null for Optional[List[int]]
        # ExprTestCase(
        #     expr=(col("a").fillnull(lit([1, 2, 3], List[int]))),
        #     df=pd.DataFrame({"a": [[1, 2, 3], None, [4, 5, 6]]}),
        #     schema={"a": Optional[List[int]]},
        #     display="FILL_NULL(col('a'), [1, 2, 3])",
        #     refs={"a"},
        #     eval_result=[[1, 2, 3], [1, 2, 3], [4, 5, 6]],
        #     expected_dtype=List[int],
        #     proto_json=None,
        # ),
    ]
    for case in cases:
        check_test_case(case)


def test_isnull():
    cases = [
        ExprTestCase(
            expr=(col("a").isnull()),
            df=pd.DataFrame({"a": [1, 2, None, 4]}),
            schema={"a": Optional[int]},
            display="IS_NULL(col('a'))",
            refs={"a"},
            eval_result=[False, False, True, False],
            expected_dtype=bool,
            proto_json=None,
        ),
        ExprTestCase(
            expr=(col("a").isnull()),
            df=pd.DataFrame({"a": ["a", "b", None, "d"]}),
            schema={"a": Optional[str]},
            display="IS_NULL(col('a'))",
            refs={"a"},
            eval_result=[False, False, True, False],
            expected_dtype=bool,
            proto_json=None,
        ),
        # Each type is a struct
        ExprTestCase(
            expr=(col("a").isnull()),
            df=pd.DataFrame({"a": [A(1, 2, "a"), A(2, 3, "b"), None]}),
            schema={"a": Optional[A]},
            display="IS_NULL(col('a'))",
            refs={"a"},
            eval_result=[False, False, True],
            expected_dtype=bool,
            proto_json=None,
        ),
        # Each type is a list
        ExprTestCase(
            expr=(col("a").isnull()),
            df=pd.DataFrame({"a": [[1, 2, 3], [4, 5, 6], None]}),
            schema={"a": Optional[List[int]]},
            display="IS_NULL(col('a'))",
            refs={"a"},
            eval_result=[False, False, True],
            expected_dtype=bool,
            proto_json=None,
        ),
    ]

    for case in cases:
        check_test_case(case)
