import pytest
import pandas as pd
from datetime import datetime
from typing import Dict
from fennel.datasets import dataset

from fennel.expr import col, when, lit
from fennel.expr.visitor import ExprPrinter, FetchReferences
from fennel.expr.serializer import ExprSerializer
from google.protobuf.json_format import ParseDict  # type: ignore
from fennel.gen.expr_pb2 import Expr
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
    ret = expr.eval(df, TestDataset.schema())
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
