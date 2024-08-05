import pytest
import pandas as pd
from datetime import datetime
from typing import Dict
from fennel.datasets import dataset

from fennel.expr import F, when
from fennel.expr.visitor import ExprPrinter
from fennel.expr.serializer import ExprSerializer
from google.protobuf.json_format import ParseDict  # type: ignore
from fennel.gen.expr_pb2 import Expr
from fennel.testing.test_utils import error_message


def test_basic_expr1():
    expr = (F("num") + F("d")).isnull()
    df = pd.DataFrame({"num": [1, 2, 3, 4], "d": [5, 6, 7, 8]})
    assert expr.typeof({"num": int, "d": int}) == bool
    ret = expr.eval(df, {"num": int, "d": int})
    assert ret.tolist() == [False, False, False, False]


def test_basic_expr2():

    expr = F("a") + F("b") + 3
    printer = ExprPrinter()
    expected = "((Ref('a') + Ref('b')) + 3)"
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


def test_math_expr():
    expr = (F("a").num.floor() + 3.2).num.ceil()
    printer = ExprPrinter()
    expected = "CEIL((FLOOR(Ref('a')) + 3.2))"
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

    expr = (
        when(F("a").num.floor() > 5)
        .then(F("b"))
        .when(F("a") > 3)
        .then(F("a"))
        .otherwise(1)
    )
    df = pd.DataFrame({"a": [1.4, 3.2, 6.1, 4.8], "b": [100, 200, 300, 400]})
    ret = expr.eval(df, {"a": float, "b": int})
    assert ret.tolist() == [1, 3.2, 300, 4.8]
    assert expr.typeof({"a": float, "b": int}) == float


def test_bool_expr():
    expr = (F("a") == 5) | ((F("b") == "random") & (F("c") == 3.2))
    printer = ExprPrinter()
    expected = """((Ref('a') == 5) or ((Ref('b') == "random") and (Ref('c') == 3.2)))"""
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


def test_str_expr():
    expr = (F("a").str.concat(F("b"))).str.lower().len().ceil()
    printer = ExprPrinter()
    expected = "CEIL(LEN(LOWER(Ref('a') + Ref('b'))))"
    assert expected == printer.print(expr.root)

    expr = (
        when(((F("a").str.concat(F("b"))).str.upper()).str.contains(F("c")))
        .then(F("b"))
        .otherwise("No Match")
    )
    expected = """WHEN CONTAINS(UPPER(Ref('a') + Ref('b')), Ref('c')) THEN Ref('b') ELSE "No Match\""""
    assert expected == printer.print(expr.root)
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
        when(F("a").str.contains("p"))
        .then(F("b"))
        .when(F("b").str.contains("b"))
        .then(F("a"))
        .when(F("c").str.contains("C"))
        .then(F("c"))
        .otherwise("No Match")
    )
    expected = """WHEN CONTAINS(Ref('a'), "p") THEN Ref('b') WHEN CONTAINS(Ref('b'), "b") THEN Ref('a') WHEN CONTAINS(Ref('c'), "C") THEN Ref('c') ELSE "No Match\""""
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
    expr = (F("a").dict.get("x") + F("a").dict.get("y")).num.ceil() + F(
        "a"
    ).dict.len()
    printer = ExprPrinter()
    expected = (
        """(CEIL((Ref('a').get("x") + Ref('a').get("y"))) + LEN(Ref('a')))"""
    )
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
