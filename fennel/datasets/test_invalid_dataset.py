from datetime import datetime

import pytest
from typing import Optional, List, Union

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.lib.aggregate import Count
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
)
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, struct
from fennel.lib.window import Window
from fennel.test_lib import *


def test_multiple_date_time():
    with pytest.raises(ValueError) as e:

        @dataset
        class UserInfoDataset:
            user_id: int = field(key=True)
            name: str
            gender: str
            # Users date of birth
            dob: str
            age: int
            account_creation_date: datetime
            country: Optional[str]
            timestamp: datetime

    _ = InternalTestClient()
    assert (
        str(e.value) == "Multiple timestamp fields are not supported in "
        "dataset `UserInfoDataset`."
    )


def test_invalid_retention_window():
    with pytest.raises(TypeError) as e:

        @dataset(history=324)
        class Activity:
            user_id: int
            action_type: float
            amount: Optional[float]
            timestamp: datetime

    assert (
        str(e.value) == "duration 324 must be a specified as a string for eg. "
        "1d/2m/3y."
    )


@meta(owner="test@test.com")
@dataset
class RatingActivity:
    userid: int
    rating: float
    movie: str
    t: datetime


def test_incorrect_aggregate():
    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class PositiveRatingActivity:
            cnt_rating: int
            unique_ratings: int
            movie: str = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(RatingActivity)
            def filter_positive_ratings(cls, rating: Dataset):
                filtered_ds = rating.filter(lambda df: df["rating"] >= 3.5)
                filter2 = filtered_ds.filter(
                    lambda df: df["movie"].isin(["Jumanji", "Titanic", "RaOne"])
                )
                return filter2.groupby("movie").aggregate(
                    [
                        Count(
                            window=Window("forever"),
                            into_field=str(cls.cnt_rating),
                        ),
                        Count(
                            window=Window("forever"),
                            into_field=str(cls.unique_ratings),
                            of="rating",
                            unique=True,
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Invalid aggregate `window=Window(start='forever', end='0s') into_field='unique_ratings' of='rating' unique=True approx=False`: Exact unique counts are not yet supported, please set approx=True"
    )


def test_invalid_struct_type():
    with pytest.raises(TypeError) as e:

        @struct
        class Car:
            model: str
            year: int

            def set_year(self, year: int):
                self.year = year

    assert (
        str(e.value)
        == "Struct `Car` contains method `set_year`, which is not allowed."
    )

    with pytest.raises(TypeError) as e:

        @struct
        class Car2:
            model: str
            year: int

            @expectations
            def get_expectations(cls):
                return [
                    expect_column_values_to_be_between(
                        column="year", min_value=1, max_value=100, mostly=0.95
                    )
                ]

    assert (
        str(e.value)
        == "Struct `Car2` contains method `get_expectations`, which is not "
        "allowed."
    )

    with pytest.raises(ValueError) as e:

        @struct
        class Car3:
            model: str
            year: int = 1990

    assert (
        str(e.value)
        == "Struct `Car3` contains attribute `year` with a default value, "
        "`1990` which is not allowed."
    )

    with pytest.raises(ValueError) as e:

        @struct
        @meta(owner="test@test.com")
        class Car4:
            model: str
            year: int

    assert (
        str(e.value)
        == "Struct `Car4` contains decorator @meta which is not allowed."
    )

    with pytest.raises(Exception) as e:

        @struct
        class Car5:
            model: str
            sibling_car: Optional["Car"]
            year: int

    assert (
        str(e.value)
        == "Struct `Car5` contains forward reference `sibling_car` which is "
        "not allowed."
    )

    with pytest.raises(TypeError) as e:

        class Manufacturer:
            name: str
            country: str
            timestamp: datetime

        @struct
        class Car:
            model: str
            manufacturer: Optional[Manufacturer]
            year: int

    assert (
        str(e.value) == "Struct `Car` contains attribute `manufacturer` of a "
        "non-struct type, which is not allowed."
    )

    with pytest.raises(Exception) as e:

        class Manufacturer:
            name: str
            country: str
            timestamp: datetime

        @struct
        class Car:
            model: str
            manufacturer: List[Manufacturer]
            year: int

    assert (
        str(e.value) == "Struct `Car` contains attribute `manufacturer` of a "
        "non-struct type, which is not allowed."
    )

    with pytest.raises(Exception) as e:

        @struct
        class Manufacturer:
            name: str
            country: str
            timestamp: datetime

    assert (
        str(e.value)
        == "Struct `Manufacturer` contains attribute `timestamp` of a "
        "non-struct type, which is not allowed."
    )

    with pytest.raises(TypeError) as e:

        @struct
        class Car:
            model: str
            year: int

        @struct
        class Bike:
            model: str
            year: int

        @dataset
        class Vehicle:
            vehicle: Union[Car, Bike]
            timestamp: datetime

    assert (
        str(e.value)
        == "Invalid type for field `vehicle` in dataset Vehicle: Multiple fennel structs found `Car, Bike`"
    )


def test_dataset_with_pipes():
    @dataset
    class XYZ:
        user_id: int
        name: str
        timestamp: datetime

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline
            def create_pipeline(cls, a: Dataset):
                return a

    assert (
        str(e.value)
        == "pipeline decorator on `create_pipeline` must have a parenthesis"
    )

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset1:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(XYZ)
            def create_pipeline(cls, a: Dataset):
                return a

    assert str(e.value) == "pipeline decorator on `XYZ` must have a parenthesis"

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset2:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(version=1)
            def create_pipeline(cls, a: Dataset):
                return a

    assert (
        str(e.value)
        == "pipeline `create_pipeline` must have Datasets as @input parameters."
    )

    with pytest.raises(TypeError) as e:

        @dataset
        class ABCDataset3:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(version=1)  # type: ignore
            @inputs(XYZ)
            def create_pipeline(a: Dataset):  # type: ignore
                return a

    assert (
        str(e.value)
        == "pipeline functions are classmethods and must have cls as the "
        "first parameter, found `a` for pipeline `create_pipeline`."
    )


def test_dataset_incorrect_join():
    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @dataset
        class ABCDataset:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(version=1)
            @inputs(XYZ)
            def create_pipeline(cls, a: Dataset):
                b = a.transform(lambda x: x)
                return a.join(b, how="left", on=["user_id"])  # type: ignore

    assert str(e.value) == "Cannot join with an intermediate dataset"


def test_dataset_incorrect_join_bounds():
    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            t: datetime

        @dataset
        class B:
            b1: int = field(key=True)
            t: datetime

        @dataset
        class ABCDataset1:
            a1: int = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline1(cls, a: Dataset, b: Dataset):
                return a.join(
                    b,
                    how="left",
                    left_on=["a1"],
                    right_on=["b1"],
                    within=("0s",),  # type: ignore
                )

    assert "Should be a tuple of 2 values" in str(e.value)

    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            t: datetime

        @dataset
        class B:
            b1: int = field(key=True)
            t: datetime

        @dataset
        class ABCDataset3:
            a1: int = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline1(cls, a: Dataset, b: Dataset):
                return a.join(
                    b,
                    how="left",
                    left_on=["a1"],
                    right_on=["b1"],
                    within=(None, "0s"),  # type: ignore
                )

    assert "Neither bounds can be None" in str(e.value)

    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            t: datetime

        @dataset
        class B:
            b1: int = field(key=True)
            t: datetime

        @dataset
        class ABCDataset4:
            a1: int = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline1(cls, a: Dataset, b: Dataset):
                return a.join(
                    b,
                    how="left",
                    left_on=["a1"],
                    right_on=["b1"],
                    within=("forever", None),  # type: ignore
                )

    assert "Neither bounds can be None" in str(e.value)

    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            t: datetime

        @dataset
        class B:
            b1: int = field(key=True)
            t: datetime

        @dataset
        class ABCDataset5:
            a1: int = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline1(cls, a: Dataset, b: Dataset):
                return a.join(
                    b,
                    how="left",
                    left_on=["a1"],
                    right_on=["b1"],
                    within=(None, None),  # type: ignore
                )

    assert "Neither bounds can be None" in str(e.value)

    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            t: datetime

        @dataset
        class B:
            b1: int = field(key=True)
            t: datetime

        @dataset
        class ABCDataset6:
            a1: int = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline1(cls, a: Dataset, b: Dataset):
                return a.join(
                    b,
                    how="left",
                    left_on=["a1"],
                    right_on=["b1"],
                    within=("forever", "forever"),
                )  # type: ignore

    assert "Upper bound cannot be `forever`" in str(e.value)


def test_dataset_optional_key():
    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: Optional[str] = field(key=True)
            timestamp: datetime

    assert str(e.value) == "Key name in dataset XYZ cannot be Optional."


def test_protected_fields():
    with pytest.raises(Exception) as e:

        @dataset(history="324d")
        class Activity:
            fields: List[int]
            key_fields: float
            on_demand: Optional[float]
            timestamp_field: datetime

    assert (
        str(e.value)
        == "[Exception('Field name `fields` is reserved. Please use a "
        "different name in dataset `Activity`.'), Exception('Field "
        "name `key_fields` is reserved. Please use a different name in dataset `Activity`"
        ".'), Exception('Field name `on_demand` is reserved. Please "
        "use a different name in dataset `Activity`.'), Exception('Field "
        "name `timestamp_field` is reserved. Please use a different "
        "name in dataset `Activity`.')]"
    )


def test_join():
    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            v: int
            t: datetime

        @dataset
        class B:
            b1: int = field(key=True)
            v: int
            t: datetime

        @dataset
        class ABCDataset:
            a1: int = field(key=True)
            v: Optional[int]
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline1(cls, a: Dataset, b: Dataset):
                x = a.join(
                    b,
                    how="left",
                    left_on=["a1"],
                    right_on=["b1"],
                )  # type: ignore
                return x

    assert (
        "Column name collision. `v` already exists in schema of left input"
        in str(e.value)
    )
