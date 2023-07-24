from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from typing import Dict, List, Optional

import fennel.gen.schema_pb2 as proto
from fennel.lib.schema.schema import (
    get_datatype,
    data_schema_check,
    between,
    oneof,
    regex,
    is_hashable,
    parse_json,
    struct,
    FENNEL_STRUCT_SRC_CODE,
    FENNEL_STRUCT_DEPENDENCIES_SRC_CODE,
)


def test_get_data_type():
    assert get_datatype(int) == proto.DataType(int_type=proto.IntType())
    assert get_datatype(Optional[int]) == proto.DataType(
        optional_type=proto.OptionalType(
            of=proto.DataType(int_type=proto.IntType())
        )
    )
    x: float = 1.0
    assert get_datatype(type(x)) == proto.DataType(
        double_type=proto.DoubleType()
    )
    x: bool = True
    assert get_datatype(type(x)) == proto.DataType(bool_type=proto.BoolType())
    x: str = "hello"
    assert get_datatype(type(x)) == proto.DataType(
        string_type=proto.StringType()
    )
    x: datetime = datetime.now()

    assert get_datatype(type(x)) == proto.DataType(
        timestamp_type=proto.TimestampType()
    )
    assert get_datatype(List[int]) == proto.DataType(
        array_type=proto.ArrayType(of=proto.DataType(int_type=proto.IntType()))
    )
    assert get_datatype(Dict[str, float]) == proto.DataType(
        map_type=proto.MapType(
            key=proto.DataType(string_type=proto.StringType()),
            value=proto.DataType(double_type=proto.DoubleType()),
        )
    )
    assert get_datatype(Dict[str, Dict[str, List[float]]]) == proto.DataType(
        map_type=proto.MapType(
            key=proto.DataType(string_type=proto.StringType()),
            value=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(string_type=proto.StringType()),
                    value=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                )
            ),
        )
    )
    assert get_datatype(List[Dict[str, float]]) == proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(string_type=proto.StringType()),
                    value=proto.DataType(double_type=proto.DoubleType()),
                )
            )
        )
    )
    assert get_datatype(List[Dict[str, List[float]]]) == proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(
                map_type=proto.MapType(
                    key=proto.DataType(string_type=proto.StringType()),
                    value=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                )
            )
        )
    )


def test_additional_dtypes():
    assert get_datatype(int) == proto.DataType(int_type=proto.IntType())
    assert get_datatype(between(int, 1, 5)) == proto.DataType(
        between_type=proto.Between(
            dtype=proto.DataType(int_type=proto.IntType()),
            min=proto.Value(int=1),
            max=proto.Value(int=5),
        )
    )
    assert get_datatype(between(int, 1, 5, False, True)) == proto.DataType(
        between_type=proto.Between(
            dtype=proto.DataType(int_type=proto.IntType()),
            min=proto.Value(int=1),
            max=proto.Value(int=5),
            strict_min=False,
            strict_max=True,
        )
    )
    assert get_datatype(between(float, 1, 5, True, False)) == proto.DataType(
        between_type=proto.Between(
            dtype=proto.DataType(double_type=proto.DoubleType()),
            min=proto.Value(float=1),
            max=proto.Value(float=5),
            strict_min=True,
            strict_max=False,
        )
    )
    assert get_datatype(oneof(str, ["male", "female"])) == proto.DataType(
        one_of_type=proto.OneOf(
            of=proto.DataType(string_type=proto.StringType()),
            options=[
                proto.Value(string="male"),
                proto.Value(string="female"),
            ],
        )
    )
    assert get_datatype(oneof(int, [1, 2, 3])) == proto.DataType(
        one_of_type=proto.OneOf(
            of=proto.DataType(int_type=proto.IntType()),
            options=[
                proto.Value(int=1),
                proto.Value(int=2),
                proto.Value(int=3),
            ],
        )
    )
    assert get_datatype(regex("[a-z]+")) == proto.DataType(
        regex_type=proto.RegexType(pattern="[a-z]+")
    )


def test_additional_dtypes_invalid():
    with pytest.raises(TypeError) as e:
        get_datatype(between(int, 1, 5.0))
    assert str(e.value) == "Dtype of between is int and max param is float"
    with pytest.raises(TypeError) as e:
        get_datatype(between(str, 1, 5))
    assert str(e.value) == "'between' type only accepts int or float types"
    with pytest.raises(TypeError) as e:
        get_datatype(oneof(float, [1, 2, 3]))
    assert str(e.value) == "'oneof' type only accepts int or str types"
    with pytest.raises(TypeError) as e:
        get_datatype(oneof(int, [1.2, 2.3, 3]))
    assert (
        str(e.value) == "'oneof' options should match the type of dtype, "
        "found 'float' expected 'int'."
    )
    with pytest.raises(TypeError) as e:
        get_datatype(oneof(str, [1, 2, 3]))
    assert (
        str(e.value) == "'oneof' options should match the type of dtype, "
        "found 'int' expected 'str'."
    )

    with pytest.raises(TypeError) as e:
        get_datatype(regex(1))
    assert str(e.value) == "'regex' type only accepts str types"


def test_valid_schema():
    now = datetime.now()
    yesterday = now - timedelta(days=1)

    data = [
        [18232, "Ross", 32, "USA", True, now],
        [18234, "Monica", 64, "Chile", False, yesterday],
    ]
    columns = ["user_id", "name", "age", "country", "Truth", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="user_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="name",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
                proto.Field(
                    name="age",
                    dtype=proto.DataType(
                        optional_type=proto.OptionalType(
                            of=proto.DataType(int_type=proto.IntType())
                        )
                    ),
                ),
                proto.Field(
                    name="country",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
                proto.Field(
                    name="Truth",
                    dtype=proto.DataType(bool_type=proto.BoolType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data.append([18234, "Monica", None, "Chile", True, yesterday])
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data = [
        [18232, np.array([1, 2, 3, 4]), np.array([1, 2, 3]), 10, now],
        [
            18234,
            np.array([1, 2.2, 0.213, 0.343]),
            np.array([0.87, 2, 3]),
            9,
            now,
        ],
        [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
    ]
    columns = [
        "doc_id",
        "bert_embedding",
        "fast_text_embedding",
        "num_words",
        "timestamp",
    ]
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="doc_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="bert_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=4)
                    ),
                ),
                proto.Field(
                    name="fast_text_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=3)
                    ),
                ),
                proto.Field(
                    name="num_words",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data = [
        [
            {"a": 1, "b": 2, "c": 3},
        ]
    ]
    dsschema = proto.DSSchema(
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="X",
                    dtype=proto.DataType(
                        map_type=proto.MapType(
                            key=proto.DataType(string_type=proto.StringType()),
                            value=proto.DataType(int_type=proto.IntType()),
                        )
                    ),
                )
            ]
        )
    )
    df = pd.DataFrame(data, columns=["X"])
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0


def test_invalid_schema():
    now = datetime.now()
    yesterday = now - timedelta(days=1)

    data = [
        [18232, "Ross", "32", "USA", now],
        [18234, "Monica", 64, "Chile", yesterday],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="user_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="name",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
                proto.Field(
                    name="age",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
                proto.Field(
                    name="country",
                    dtype=proto.DataType(string_type=proto.StringType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 1

    data = [
        [18232, np.array([1, 2, 3]), np.array([1, 2, 3]), 10, now],
        [
            18234,
            np.array([1, 2.2, 0.213, 0.343]),
            np.array([0.87, 2, 3]),
            9,
            now,
        ],
        [18934, [1, 2.2, 0.213, 0.343], [0.87, 2, 3], 12, now],
    ]
    columns = [
        "doc_id",
        "bert_embedding",
        "fast_text_embedding",
        "num_words",
        "timestamp",
    ]
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="doc_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="bert_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=4)
                    ),
                ),
                proto.Field(
                    name="fast_text_embedding",
                    dtype=proto.DataType(
                        embedding_type=proto.EmbeddingType(embedding_size=3)
                    ),
                ),
                proto.Field(
                    name="num_words",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    df = pd.DataFrame(data, columns=columns)
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 1

    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="doc_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="bert_embedding",
                    dtype=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                ),
                proto.Field(
                    name="fast_text_embedding",
                    dtype=proto.DataType(
                        array_type=proto.ArrayType(
                            of=proto.DataType(double_type=proto.DoubleType())
                        )
                    ),
                ),
                proto.Field(
                    name="num_words",
                    dtype=proto.DataType(int_type=proto.IntType()),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data = [[[123]]]
    dsschema = proto.DSSchema(
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="X",
                    dtype=proto.DataType(
                        map_type=proto.MapType(
                            key=proto.DataType(string_type=proto.StringType()),
                            value=proto.DataType(int_type=proto.IntType()),
                        )
                    ),
                )
            ]
        )
    )
    df = pd.DataFrame(data, columns=["X"])
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 1


def test_invalid_schema_additional_types():
    now = datetime.now()
    data = [
        [18232, "Ross9", 212, "transgender", 5, now],
    ]
    columns = ["user_id", "name", "age", "gender", "grade", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    dsschema = proto.DSSchema(
        keys=proto.Schema(
            fields=[
                proto.Field(
                    name="user_id",
                    dtype=proto.DataType(int_type=proto.IntType()),
                )
            ]
        ),
        values=proto.Schema(
            fields=[
                proto.Field(
                    name="name",
                    dtype=proto.DataType(
                        regex_type=proto.RegexType(
                            pattern="[A-Za-z]+[0-9]",
                        )
                    ),
                ),
                proto.Field(
                    name="age",
                    dtype=proto.DataType(
                        between_type=proto.Between(
                            dtype=proto.DataType(int_type=proto.IntType()),
                            min=proto.Value(int=1),
                            max=proto.Value(int=100),
                        )
                    ),
                ),
                proto.Field(
                    name="gender",
                    dtype=proto.DataType(
                        one_of_type=proto.OneOf(
                            of=proto.DataType(string_type=proto.StringType()),
                            options=[
                                proto.Value(string="male"),
                                proto.Value(string="female"),
                            ],
                        )
                    ),
                ),
                proto.Field(
                    name="gender",
                    dtype=proto.DataType(
                        one_of_type=proto.OneOf(
                            of=proto.DataType(int_type=proto.IntType()),
                            options=[
                                proto.Value(int=1),
                                proto.Value(int=2),
                                proto.Value(int=3),
                            ],
                        )
                    ),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 3


def test_is_hashable():
    assert is_hashable(int) is True
    assert is_hashable(Optional[int]) is True
    assert is_hashable(float) is False
    assert is_hashable(str) is True
    assert is_hashable(List[float]) is False
    assert is_hashable(List[int]) is True
    assert is_hashable(List[str]) is True
    assert is_hashable(Dict[str, int]) is True
    assert is_hashable(Dict[str, float]) is False
    assert is_hashable(Dict[str, List[int]]) is True
    assert is_hashable(Dict[str, List[float]]) is False
    assert is_hashable(Optional[Dict[str, List[int]]]) is True


@struct
class MyString:
    value: str


@struct
class Manufacturer2:
    name: MyString
    country: str


@struct
class Car2:
    make: Manufacturer2
    model: str
    year: int


def test_parse_json_with_car():
    car_json = {
        "make": {
            "name": {"value": "Test Manufacturer"},
            "country": "Test " "Country",
        },
        "model": "Test Model",
        "year": "2023",
    }
    car = parse_json(Car2, car_json)
    assert isinstance(car, Car2)
    assert isinstance(car.make, Manufacturer2)
    assert car.make.name.value == "Test Manufacturer"
    assert car.make.country == "Test Country"
    assert car.model == "Test Model"
    assert car.year == 2023

    assert hasattr(Car2, FENNEL_STRUCT_SRC_CODE)
    assert hasattr(Manufacturer2, FENNEL_STRUCT_SRC_CODE)
    car_code = getattr(Car2, FENNEL_STRUCT_SRC_CODE)
    expected_car_code = """
@struct
class Car2:
    make: Manufacturer2
    model: str
    year: int
"""
    assert car_code.strip() == expected_car_code.strip()

    expected_dependency_code = """
@struct
class MyString:
    value: str


@struct
class Manufacturer2:
    name: MyString
    country: str
    """
    assert hasattr(Car2, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE)
    dependency_code = getattr(Car2, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE)
    assert dependency_code.strip() == expected_dependency_code.strip()


@struct
class Manufacturer:
    name: str
    country: str


@struct
class Car:
    make: Manufacturer
    model: str
    year: int


def test_parse_json_with_list_of_cars():
    cars_json = [
        {
            "make": {"name": "Test Manufacturer", "country": "Test Country"},
            "model": "Test Model",
            "year": "2023",
        },
        {
            "make": {
                "name": "Second Manufacturer",
                "country": "Second Country",
            },
            "model": "Second Model",
            "year": "2024",
        },
    ]
    cars = parse_json(List[Car], cars_json)
    assert isinstance(cars, list)
    assert all(isinstance(car, Car) for car in cars)
    assert cars[0].make.name == "Test Manufacturer"
    assert cars[0].make.country == "Test Country"
    assert cars[0].model == "Test Model"
    assert cars[0].year == 2023
    assert cars[1].make.name == "Second Manufacturer"
    assert cars[1].make.country == "Second Country"
    assert cars[1].model == "Second Model"
    assert cars[1].year == 2024


@struct
class UserCarMap:
    map: Dict[str, List[Car]]


def test_parse_json_complex():
    user_car_map_json = {
        "map": {
            "user1": [
                {
                    "make": {
                        "name": "Test Manufacturer",
                        "country": "Test Country",
                    },
                    "model": "Test Model",
                    "year": "2023",
                },
                {
                    "make": {
                        "name": "Second Manufacturer",
                        "country": "Second Country",
                    },
                    "model": "Second Model",
                    "year": "2024",
                },
            ]
        }
    }
    user_car_map = parse_json(UserCarMap, user_car_map_json)
    assert isinstance(user_car_map, UserCarMap)
    assert isinstance(user_car_map.map, dict)
    assert all(isinstance(k, str) for k in user_car_map.map.keys())
    assert all(
        isinstance(v, list) and all(isinstance(car, Car) for car in v)
        for v in user_car_map.map.values()
    )
    car1 = user_car_map.map["user1"][0]
    assert isinstance(car1.make, Manufacturer)
    assert car1.make.name == "Test Manufacturer"
    assert car1.make.country == "Test Country"
    assert car1.model == "Test Model"
    assert car1.year == 2023
    car2 = user_car_map.map["user1"][1]
    assert isinstance(car2.make, Manufacturer)
    assert car2.make.name == "Second Manufacturer"
    assert car2.make.country == "Second Country"
    assert car2.model == "Second Model"
    assert car2.year == 2024


def test_as_json():
    # Create an instance of Manufacturer
    manufacturer1 = Manufacturer("Test Manufacturer", "Test Country")
    # Convert it to a dictionary
    manufacturer1_dict = manufacturer1.as_json()
    # Check the result
    assert manufacturer1_dict == {
        "name": "Test Manufacturer",
        "country": "Test Country",
    }

    # Create an instance of Car
    car1 = Car(make=manufacturer1, model="Test Model", year=2023)
    # Convert it to a dictionary
    car1_dict = car1.as_json()
    # Check the result
    assert car1_dict == {
        "make": {"name": "Test Manufacturer", "country": "Test Country"},
        "model": "Test Model",
        "year": 2023,
    }

    # Create another instance of Manufacturer
    manufacturer2 = Manufacturer("Second Manufacturer", "Second Country")
    # Convert it to a dictionary
    manufacturer2_dict = manufacturer2.as_json()
    # Check the result
    assert manufacturer2_dict == {
        "name": "Second Manufacturer",
        "country": "Second Country",
    }

    # Create another instance of Car
    car2 = Car(make=manufacturer2, model="Second Model", year=2024)
    # Convert it to a dictionary
    car2_dict = car2.as_json()
    # Check the result
    assert car2_dict == {
        "make": {"name": "Second Manufacturer", "country": "Second Country"},
        "model": "Second Model",
        "year": 2024,
    }

    # Create an instance of UserCarMap
    user_car_map = UserCarMap({"user1": [car1, car2]})
    # Convert it to a dictionary
    user_car_map_dict = user_car_map.as_json()
    # Check the result
    assert user_car_map_dict == {
        "map": {
            "user1": [
                {
                    "make": {
                        "name": "Test Manufacturer",
                        "country": "Test Country",
                    },
                    "model": "Test Model",
                    "year": 2023,
                },
                {
                    "make": {
                        "name": "Second Manufacturer",
                        "country": "Second Country",
                    },
                    "model": "Second Model",
                    "year": 2024,
                },
            ]
        }
    }
