from fennel.dtypes import struct
from fennel.utils import fhash, to_columnar_json
from datetime import datetime, timezone
from math import nan

import json
import pandas as pd


def test_fhash_Callable():
    def f(x: int, y: int) -> int:
        x = x + 1
        return x + y

    hash_code = "baae07d4aa0291b3ba2758f66817133c"
    assert fhash(f) == hash_code

    def f(x: int, y: int) -> int:
        x = x + 1
        # this is a comment
        return x + y

    assert fhash(f) == hash_code

    def f(x: int, y: int) -> int:
        x = x + 1

        # this is a comment
        return x + y

    assert fhash(f) == hash_code

    def f(x: int, y: int) -> int:
        x = x + 1

        # this is a comment

        return x + y

    assert fhash(f) == hash_code


def test_to_columnar_json():
    # single col
    col1 = pd.Series([True, False, True, False, True], name="isEven")
    df1 = pd.DataFrame(col1)
    dict1 = to_columnar_json(df1)
    assert dict1["isEven"] == [True, False, True, False, True]
    assert all(pd.DataFrame(dict1) == df1)

    # multiple cols
    col2 = pd.Series([0, 10, 20, 30, 40])
    col3 = pd.Series(["foo", "bar", "baz", "panda", "fennel"], name="names")
    col4 = pd.Series([3.14, 2.71828, 9.81, 1.618, 0])
    df2 = pd.DataFrame([col1, col2, col3, col4]).T
    dict2 = to_columnar_json(df2)
    expected = {
        "isEven": [True, False, True, False, True],
        "Unnamed 0": [0, 10, 20, 30, 40],
        "names": ["foo", "bar", "baz", "panda", "fennel"],
        "Unnamed 1": [3.14, 2.71828, 9.81, 1.618, 0],
    }
    expected_str = '{"isEven": [true, false, true, false, true], "Unnamed 0": [0, 10, 20, 30, 40], "names": ["foo", "bar", "baz", "panda", "fennel"], "Unnamed 1": [3.14, 2.71828, 9.81, 1.618, 0.0]}'
    assert dict2 == expected
    assert all(pd.DataFrame(dict2) == df2)

    dict_as_str = to_columnar_json(df2, as_str=True)
    assert dict_as_str == expected_str

    # contains nulls and timestamps, handle in json way
    now = datetime.now(timezone.utc)
    df3 = pd.DataFrame(
        [
            [1, "abc", None, 3.14, now],
            [nan, "def", False, 2.71828, now],
        ]
    ).rename(columns={0: "num", 1: "word", 2: "bool", 3: "math", 4: "time"})
    now_ms = int(now.timestamp() * 1000)
    expected = {
        "num": [1.0, None],
        "word": ["abc", "def"],
        "bool": [None, False],
        "math": [3.14, 2.71828],
        "time": [now_ms, now_ms],
    }
    # str encodes Nones and nans as 'null'
    expected_str = (
        "{"
        + '"num": [1.0, null], "word": ["abc", "def"], "bool": [null, false], '
        + f'"math": [3.14, 2.71828], "time": [{now_ms}, {now_ms}]'
        + "}"
    )
    assert to_columnar_json(df3) == expected
    assert to_columnar_json(df3, as_str=True) == expected_str
    assert json.loads(to_columnar_json(df3, as_str=True)) == expected

    # Contains complex types : list, dict, struct
    @struct
    class Building:
        address: str
        num_floors: int
        surface_area: float

    buildings = [
        Building("1 Infinite Loop", 2, 1000.14159),
        Building("50 Mission St", 65, 300.86),
    ]

    df4 = pd.DataFrame(
        [
            [
                "abc",
                [1, 2, 3, 4],
                {"foo": 10, "bar": [False, True]},
                buildings[0],
            ],
            [
                "def",
                [5, 6, 7, 8],
                {"foo": 20, "bar": [True, False], "baz": "panda"},
                buildings[1],
            ],
        ]
    ).rename(columns={0: "word", 1: "nums_list", 2: "dict", 3: "building"})
    expected = {
        "word": ["abc", "def"],
        "nums_list": [[1, 2, 3, 4], [5, 6, 7, 8]],
        "dict": [
            {"foo": 10, "bar": [False, True]},
            {"foo": 20, "bar": [True, False], "baz": "panda"},
        ],
        "building": [
            {
                "address": "1 Infinite Loop",
                "num_floors": 2,
                "surface_area": 1000.14159,
            },
            {
                "address": "50 Mission St",
                "num_floors": 65,
                "surface_area": 300.86,
            },
        ],
    }

    dict4 = to_columnar_json(df4)
    assert dict4 == expected
    assert all(pd.DataFrame(dict4) == df4)
    assert json.loads(to_columnar_json(df4, as_str=True)) == expected
