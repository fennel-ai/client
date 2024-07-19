import pytest
import pandas as pd
from datetime import datetime
from typing import Dict
from fennel.datasets import dataset

from fennel.expr import F, when
from fennel.expr.visitor import Printer
from fennel.expr.serializer import ExprSerializer
from google.protobuf.json_format import ParseDict  # type: ignore
from fennel.gen.expr_pb2 import Expr
from fennel.testing.test_utils import error_message
from fennel.expr.utils import compute_expr


def test_basic_expr1():
    expr = (F("num") + F("d")).isnull()
    df = pd.DataFrame({"num": [1, 2, 3, 4], "d": [5, 6, 7, 8]})
    serializer = ExprSerializer()
    proto_expr = serializer.serialize(expr.root)
    proto_bytes = proto_expr.SerializeToString()
    ret = compute_expr(expr, df, {"num": int, "d": int})
    assert ret.tolist() == [False, False, False, False]


def test_basic_expr2():

    expr = F("a") + F("b") + 3
    printer = Printer()
    expected = "((Field[`a`] + Field[`b`]) + 3)"
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
                "jsonLiteral": {"literal": '"3"', "dtype": {"intType": {}}}
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

    print(TestDataset.schema())
    ret = compute_expr(expr, df, {"a": int, "b": int})
    assert ret.tolist() == [9, 11, 13, 15]
    ret = compute_expr(expr, df, TestDataset.schema())
    assert ret.tolist() == [9, 11, 13, 15]
    print(ret)


def test_math_expr():
    expr = (F("a").num.floor() + 3.2).num.ceil()
    printer = Printer()
    expected = "CEIL((FLOOR(Field[`a`]) + 3.2))"
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
                            "literal": '"3.2"',
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
    proto_bytes = proto_expr.SerializeToString()
    df = pd.DataFrame({"a": [1.4, 2.9, 3.1, 4.8], "b": ["a", "b", "c", "d"]})
    ret = compute_expr(expr, df, {"a": float})
    assert ret.tolist() == [5, 6, 7, 8]

    expr = when(F("a").num.floor() > 5).then(F("b")).when(F("a") > 3).then(F("a")).otherwise(1)
    df = pd.DataFrame({"a": [1.4, 3.2, 6.1, 4.8], "b": [100, 200, 300,400]})
    ret = compute_expr(expr, df, {"a": float, "b": int})
    assert ret.tolist() == [1, 3.2, 300, 4.8]     

def test_str_expr():
    expr = (F("a") + F("b")).str.lower().len().ceil()
    printer = Printer()
    expected = "CEIL(LEN(LOWER((Field[`a`] + Field[`b`]))))"
    assert expected == printer.print(expr.root)

    expr = (
        when((F("a").str.upper()).str.contains(F("c")))
        .then(F("b"))
        .otherwise("No Match")
    )
    expected = 'WHEN CONTAINS(UPPER(Field[`a`]), Field[`c`]) THEN Field[`b`] ELSE "No Match"'
    assert expected == printer.print(expr.root)
    df = pd.DataFrame(
        {
            "a": ["p", "BRandomS", "CRandomStrin", "tqz"],
            "b": ["aa", "tring", "g", "d"],
            "c": ["A", "B", "C", "D"],
        }
    )
    ret = compute_expr(expr, df, {"a": str, "b": str})
    assert ret.tolist() == [
        "No Match",
        "BRandomSB",
        "CRandomStringC",
        "No Match",
    ]

    expr = (
        when(F("a").str.contains("p"))
        .then(F("b"))
        .when(F("b").str.contains("b"))
        .then(F("a"))
        .when(F("c").str.contains("C"))
        .then(F("c"))
        .otherwise("No Match")
    )
    expected = 'WHEN CONTAINS(Field[`a`], "p") THEN Field[`b`] WHEN CONTAINS(Field[`b`], "b") THEN Field[`a`] WHEN CONTAINS(Field[`c`], "C") THEN Field[`c`] ELSE "No Match"'
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
                                            "literal": '"\\"p\\""',
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
                                            "literal": '"\\"b\\""',
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
                                            "literal": '"\\"C\\""',
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
                    "literal": '"\\"No Match\\""',
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
    ret = compute_expr(expr, df, {})
    print(ret)


def test_dict_op():
    expr = (F("a").dict.get("x") + F("a").dict.get("y")).num.ceil() + F(
        "a"
    ).dict.len()
    printer = Printer()
    expected = """(CEIL((Field[`a`].get("x") + Field[`a`].get("y"))) + LEN(Field[`a`]))"""
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
                                                    "literal": '"\\"x\\""',
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
                                                    "literal": '"\\"y\\""',
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
