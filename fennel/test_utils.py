from fennel.utils import fhash, to_columnar_json

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
    col1 = pd.Series([True, False, True, False, True], name="isEven")
    df1 = pd.DataFrame(col1)
    dict1 = to_columnar_json(df1)
    assert dict1["isEven"] == [True, False, True, False, True]
    assert all(pd.DataFrame(dict1) == df1)

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
