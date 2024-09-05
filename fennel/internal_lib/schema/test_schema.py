import numpy as np
import pandas as pd
import pytest
import unittest

from datetime import datetime, date, timedelta, timezone
from decimal import Decimal as PythonDecimal
from typing import Dict, List, Optional, Union, get_type_hints

import fennel.gen.schema_pb2 as proto
from fennel.dtypes.dtypes import (
    Embedding,
    struct,
    FENNEL_STRUCT_SRC_CODE,
    FENNEL_STRUCT_DEPENDENCIES_SRC_CODE,
    Decimal,
)
from fennel.internal_lib.schema.schema import (
    fennel_is_optional,
    from_proto,
    get_datatype,
    data_schema_check,
    between,
    oneof,
    regex,
    is_hashable,
    parse_json,
    convert_dtype_to_arrow_type,
    validate_val_with_dtype,
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
    x: datetime = datetime.now(timezone.utc)

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
    assert get_datatype(Decimal[28]) == proto.DataType(
        decimal_type=proto.DecimalType(scale=28)
    )
    assert get_datatype(Decimal[2]) == proto.DataType(
        decimal_type=proto.DecimalType(scale=2)
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

    with pytest.raises(TypeError) as e:
        get_datatype(Decimal[29])
    assert (
        str(e.value)
        == "Scale defined in the decimal type cannot be greater than 28."
    )

    with pytest.raises(TypeError) as e:
        get_datatype(Decimal[0])
    assert (
        str(e.value)
        == "Scale defined in the decimal type cannot be 0. If you want to use decimal with zero scale then use int type instead."
    )


def test_validate_val_with_dtype():
    value = PythonDecimal("29.00")
    dtype = Decimal[2]
    validate_val_with_dtype(dtype, value)

    value = "29.00"
    dtype = Decimal[3]
    with pytest.raises(ValueError) as e:
        validate_val_with_dtype(dtype, value)
    assert (
        str(e.value)
        == "Expected type python Decimal or float or int, got `str` for value `29.00`"
    )


def test_valid_schema():
    # Replacing tzinfo because the data schema checks expects dtype to be datetime[ns] and not datetime[ns, UTC]
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)

    data = [
        [18232, "Ross", 32, "USA", True, PythonDecimal("1000.20"), now],
        [
            18234,
            "Monica",
            64,
            "Chile",
            False,
            PythonDecimal("1000.20"),
            yesterday,
        ],
    ]
    columns = [
        "user_id",
        "name",
        "age",
        "country",
        "Truth",
        "net_worth",
        "timestamp",
    ]
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
                proto.Field(
                    name="net_worth",
                    dtype=proto.DataType(
                        decimal_type=proto.DecimalType(scale=2)
                    ),
                ),
            ]
        ),
        timestamp="timestamp",
    )
    exceptions = data_schema_check(dsschema, df)
    assert len(exceptions) == 0

    data.append(
        [
            18234,
            "Monica",
            None,
            "Chile",
            True,
            PythonDecimal("1000.30"),
            yesterday,
        ]
    )
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
    # Replacing tzinfo because the data schema checks expects dtype to be datetime[ns] and not datetime[ns, UTC]
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)

    data = [
        [18232, "Ross", "32", "USA", PythonDecimal(100.122), now],
        [18234, "Monica", 64, "Chile", PythonDecimal(100.122), yesterday],
    ]
    columns = ["user_id", "name", "age", "country", "net_worth", "timestamp"]
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
                proto.Field(
                    name="net_worth",
                    dtype=proto.DataType(
                        decimal_type=proto.DecimalType(scale=3)
                    ),
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


def test_invalid_schema_additional_types():
    # Replacing tzinfo because the data schema checks expects dtype to be datetime[ns] and not datetime[ns, UTC]
    now = datetime.now(timezone.utc)
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
    founded: datetime


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
            "founded": "2021-01-01T00:00:00",
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
    founded: datetime
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


@struct
class UserCarMap:
    map: Dict[str, List[Car]]


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


def test_convert_dtype_to_arrow_type():
    # Testing complex data type of list[Struct]
    data_type = proto.DataType(
        array_type=proto.ArrayType(
            of=proto.DataType(
                struct_type=proto.StructType(
                    fields=[
                        proto.Field(
                            name="a",
                            dtype=proto.DataType(int_type=proto.IntType()),
                        ),
                        proto.Field(
                            name="b",
                            dtype=proto.DataType(
                                map_type=proto.MapType(
                                    key=proto.DataType(
                                        string_type=proto.StringType()
                                    ),
                                    value=proto.DataType(
                                        int_type=proto.IntType()
                                    ),
                                )
                            ),
                        ),
                        proto.Field(
                            name="c",
                            dtype=proto.DataType(
                                array_type=proto.ArrayType(
                                    of=proto.DataType(int_type=proto.IntType())
                                )
                            ),
                        ),
                    ]
                )
            )
        )
    )
    arrow_dtype = convert_dtype_to_arrow_type(data_type)
    assert (
        str(arrow_dtype)
        == "list<item: struct<a: int64 not null, b: map<string, int64> not null, c: list<item: int64> not null>>"
    )


class TestDataTypeConversions(unittest.TestCase):

    def test_int_type(self):
        original = int
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(original, result)

    def test_float_type(self):
        original = float
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(original, result)

    def test_string_type(self):
        original = str
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(original, result)

    def test_datetime_type(self):
        original = datetime
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(original, result)

    def test_date_type(self):
        original = date
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(original, result)

    def test_numpy_int64(self):
        original = np.int64
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(int, result)  # np.int64 should map to int

    def test_numpy_float64(self):
        original = np.float64
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(float, result)  # np.float64 should map to float

    def test_pandas_int64_dtype(self):
        original = pd.Int64Dtype
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(int, result)  # pd.Int64Dtype should map to int

    def test_pandas_string_dtype(self):
        original = pd.StringDtype
        proto = get_datatype(original)
        result = from_proto(proto)
        self.assertEqual(str, result)  # pd.StringDtype should map to str

    def test_custom_type_between(self):
        original = between(
            dtype=int, min=1, max=5, strict_min=True, strict_max=False
        )
        proto = original.to_proto()
        result = from_proto(proto)
        self.assertEqual(original.dtype, result.dtype)
        self.assertEqual(original.min, result.min)
        self.assertEqual(original.max, result.max)
        self.assertEqual(original.strict_min, result.strict_min)
        self.assertEqual(original.strict_max, result.strict_max)

    def test_custom_type_oneof(self):
        original = oneof(dtype=int, options=[1, 2, 3])
        proto = original.to_proto()
        result = from_proto(proto)
        self.assertEqual(original.dtype, result.dtype)
        self.assertEqual(original.options, result.options)

    def test_list_type(self):
        original_type = List[int]
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        self.assertEqual(original_type, converted_type)

    def test_dict_type(self):
        original_type = Dict[str, float]
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        self.assertEqual(original_type, converted_type)

    def test_optional_type(self):
        original_type = Optional[int]
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        self.assertEqual(original_type, converted_type)

    def test_complex_case(self):
        original_type = List[Dict[str, List[float]]]
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        self.assertEqual(original_type, converted_type)

    def test_embedding_case(self):
        original_type = Embedding[4]
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        self.assertEqual(original_type, converted_type)

    def test_struct_case(self):
        def assert_struct_fields_match(self, original_cls, reconstructed_cls):
            # Names of class should match
            self.assertEqual(
                original_cls.__name__,
                reconstructed_cls.__name__,
                "Class names do not match.",
            )
            original_fields = get_type_hints(original_cls)
            reconstructed_fields = get_type_hints(reconstructed_cls)
            self.assertEqual(
                set(original_fields.keys()),
                set(reconstructed_fields.keys()),
                "Field names do not match.",
            )

            for field_name, original_type in original_fields.items():
                reconstructed_type = reconstructed_fields[field_name]
                self.assertEqual(
                    original_type,
                    reconstructed_type,
                    f"Types for field {field_name} do not match.",
                )

        @struct
        class A:
            a: int
            b: str

        original_type = A
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        assert_struct_fields_match(self, original_type, converted_type)

        @struct
        class ComplexStruct:
            a: List[int]
            b: Dict[str, float]
            c: Optional[str]
            d: Embedding[4]
            e: List[Dict[str, List[float]]]
            f: Dict[str, Optional[int]]
            g: Optional[Dict[str, List[float]]]

        original_type = ComplexStruct
        proto = get_datatype(original_type)
        converted_type = from_proto(proto)
        assert_struct_fields_match(self, original_type, converted_type)
