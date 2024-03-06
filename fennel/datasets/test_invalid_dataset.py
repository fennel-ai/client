from datetime import datetime
from typing import Optional, List, Union

import pandas as pd
import pytest

from fennel.datasets import (
    dataset,
    pipeline,
    field,
    Dataset,
    Count,
    Average,
    Stddev,
    Distinct,
)
from fennel.lib import (
    meta,
    inputs,
    expectations,
    expect_column_values_to_be_between,
)
from fennel.dtypes import struct
from fennel.sources import Webhook, source
from fennel.testing import *

__owner__ = "eng@fennel.ai"


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
        "dataset `UserInfoDataset`. Please set one of the datetime fields to be the timestamp field."
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


def test_invalid_select():
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            a3: str
            a4: float
            t: datetime

        @meta(owner="thaqib@fennel.ai")
        @dataset
        class B:
            a1: int
            a2: str
            t: datetime

            @pipeline
            @inputs(A)
            def from_a(cls, a: Dataset):
                return a.select("a2", "a3")

    assert (
        str(e.value)
        == """Field `a1` is a key or timestamp field in schema of select node input '[Dataset:A]'. Value fields are: ['a2', 'a3', 'a4']"""
    )


def test_invalid_assign():
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            t: datetime

        @meta(owner="thaqib@fennel.ai")
        @dataset
        class B:
            a1: int
            a2: str
            t: datetime

            @pipeline
            @inputs(A)
            def from_a(cls, a: Dataset):
                return a.assign("a1", float, lambda df: df["a1"] * 1.0)

    assert (
        str(e.value) == "Field `a1` is a key or timestamp field in "
        "schema of assign node input '[Dataset:A]'. Value fields are: ['a2']"
    )


def test_select_drop_invalid_param():
    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            a3: str
            a4: float
            t: datetime

        @meta(owner="thaqib@fennel.ai")
        @dataset
        class B:
            a1: int = field(key=True)
            a2: int
            t: datetime

            @pipeline
            @inputs(A)
            def from_a(cls, a: Dataset):
                return a.select("a1", "a2", columns=["a1", "a2"])

    assert (
        str(e.value)
        == "can only specify either 'columns' or positional arguments to select, not both."
    )

    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            a3: str
            a4: float
            t: datetime

        @meta(owner="thaqib@fennel.ai")
        @dataset
        class C:
            a1: int = field(key=True)
            a2: int
            t: datetime

            @pipeline
            @inputs(A)
            def from_a_drop(cls, a: Dataset):
                return a.drop("a3", "a4", columns=["a3", "a4"])

    assert (
        str(e.value)
        == "can only specify either 'columns' or positional arguments to drop, not both."
    )

    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            a3: str
            a4: float
            t: datetime

        @meta(owner="thaqib@fennel.ai")
        @dataset
        class D:
            a1: int = field(key=True)
            a2: int
            t: datetime

            @pipeline
            @inputs(A)
            def from_a_drop(cls, a: Dataset):
                return a.drop()

    assert (
        str(e.value)
        == "must specify either 'columns' or positional arguments to drop."
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

            @pipeline
            @inputs(RatingActivity)
            def filter_positive_ratings(cls, rating: Dataset):
                filtered_ds = rating.filter(lambda df: df["rating"] >= 3.5)
                filter2 = filtered_ds.filter(
                    lambda df: df["movie"].isin(["Jumanji", "Titanic", "RaOne"])
                )
                return filter2.groupby("movie").aggregate(
                    [
                        Count(
                            window="forever",
                            into_field=str(cls.cnt_rating),
                        ),
                        Count(
                            window="forever",
                            into_field=str(cls.unique_ratings),
                            of="rating",
                            unique=True,
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Invalid aggregate `window=Last(start='forever', end='0s') into_field='unique_ratings' of='rating' unique=True approx=False`: Exact unique counts are not yet supported, please set approx=True"
    )

    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset
        class PositiveRatingActivity2:
            cnt_rating: int
            unique_ratings: int
            movie: str = field(key=True)
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def count_distinct_pipeline(cls, rating: Dataset):
                filtered_ds = rating.filter(lambda df: df["rating"] >= 3.5)
                filter2 = filtered_ds.filter(
                    lambda df: df["movie"].isin(["Jumanji", "Titanic", "RaOne"])
                )
                return filter2.groupby("movie").aggregate(
                    [
                        Count(
                            window="forever",
                            into_field=str(cls.cnt_rating),
                        ),
                        Distinct(
                            window="forever",
                            into_field=str(cls.unique_ratings),
                            of="rating",
                            unordered=True,
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Cannot use distinct for field `rating` of type `float`, as it is not hashable"
    )

    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class PositiveRatingActivity3:
            cnt_rating: int
            unique_users: int
            movie: str = field(key=True)
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def count_distinct_pipeline(cls, rating: Dataset):
                filtered_ds = rating.filter(lambda df: df["rating"] >= 3.5)
                filter2 = filtered_ds.filter(
                    lambda df: df["movie"].isin(["Jumanji", "Titanic", "RaOne"])
                )
                return filter2.groupby("movie").aggregate(
                    [
                        Count(
                            window="forever",
                            into_field=str(cls.cnt_rating),
                        ),
                        Distinct(
                            window="forever",
                            into_field=str(cls.unique_users),
                            of="userid",
                            unordered=False,
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Invalid aggregate `window=Last(start='forever', end='0s') into_field='unique_users' of='userid' unordered=False`: Distinct requires unordered=True"
    )

    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset
        class Ratings:
            cnt_rating: int
            avg_rating: float
            stddev: float
            movie: str = field(key=True)
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def get_stddev_ratings(cls, rating: Dataset):
                return rating.groupby("movie").aggregate(
                    [
                        Count(
                            window="forever",
                            into_field=str(cls.cnt_rating),
                        ),
                        Average(
                            window="forever",
                            into_field=str(cls.avg_rating),
                            of="rating",
                            default=0,
                        ),
                        Stddev(
                            window="forever",
                            into_field=str(cls.stddev),
                            of="movie",  # invalid type for ratings
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Cannot get standard deviation of field movie of type str"
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
        class Car6:
            model: str
            manufacturer: Optional[Manufacturer]
            year: int

    assert (
        str(e.value) == "Struct `Car6` contains attribute `manufacturer` "
        "of a non-struct type, which is not allowed."
    )

    with pytest.raises(Exception) as e:

        class Manufacturer:
            name: str
            country: str
            timestamp: datetime

        @struct
        class Car7:
            model: str
            manufacturer: List[Manufacturer]
            year: int

    assert (
        str(e.value) == "Struct `Car7` contains attribute `manufacturer` "
        "of a non-struct type, which is not allowed."
    )

    with pytest.raises(Exception) as e:

        class NotStruct:
            num: int

        @struct
        class Manufacturer:
            name: str
            country: str
            s: NotStruct

    assert (
        str(e.value) == "Struct `Manufacturer` contains attribute `s` of a "
        "non-struct type, which is not allowed."
    )

    with pytest.raises(TypeError) as e:

        @struct
        class Car8:
            model: str
            year: int

        @struct
        class Bike:
            model: str
            year: int

        @dataset
        class Vehicle:
            vehicle: Union[Car8, Bike]
            timestamp: datetime

    assert (
        str(e.value)
        == "Invalid type for field `vehicle` in dataset Vehicle: Multiple "
        "fennel structs found `Car8, Bike`"
    )


def test_dataset_with_pipes():
    @dataset
    class XYZ:
        user_id: int
        name: str
        timestamp: datetime

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset2:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline
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

            @pipeline  # type: ignore
            @inputs(XYZ)
            def create_pipeline(a: Dataset):  # type: ignore
                return a

    assert (
        str(e.value)
        == "pipeline functions are classmethods and must have cls as the "
        "first parameter, found `a` for pipeline `create_pipeline`."
    )


@mock
def test_pipeline_input_validation_during_sync(client):
    with pytest.raises(ValueError) as e:

        @meta(owner="eng@fennel.ai")
        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @meta(owner="eng@fennel.ai")
        @dataset
        class ABCDataset:
            user_id: int
            name: str
            timestamp: datetime

            @pipeline
            @inputs(XYZ)
            def create_pipeline(cls, a: Dataset):
                return a

        client.commit(message="msg", datasets=[ABCDataset])
    assert (
        str(e.value)
        == "Dataset `XYZ` is an input to the pipelines: `['ABCDataset.create_pipeline']` but is not synced. Please add it to the sync call."
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

            @pipeline
            @inputs(XYZ)
            def create_pipeline(cls, a: Dataset):
                b = a.transform(lambda x: x)
                return a.join(b, how="left", on=["user_id"])  # type: ignore

    assert (
        str(e.value)
        == "Cannot join with an intermediate dataset, i.e something defined inside a pipeline. Only joining against keyed datasets is permitted."
    )

    with pytest.raises(TypeError) as e:

        @dataset
        class XYZ:
            user_id: Optional[int]
            agent_id: int
            name: str
            timestamp: datetime

        @dataset
        class ABC:
            user_id: int = field(key=True)
            agent_id: int = field(key=True)
            age: int
            timestamp: datetime

        @dataset
        class XYZJoinedABC:
            user_id: int
            name: str
            age: int
            timestamp: datetime

            @pipeline
            @inputs(XYZ, ABC)
            def create_pipeline(cls, a: Dataset, b: Dataset):
                c = a.join(b, how="inner", on=["user_id", "agent_id"])  # type: ignore
                return c

    assert (
        str(e.value)
        == "Fields used in a join operator must not be optional in left schema, found `user_id` of "
        "type `Optional[int]` in `'[Pipeline:create_pipeline]->join node'`"
    )


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

            @pipeline
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

            @pipeline
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

            @pipeline
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

            @pipeline
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

            @pipeline
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

            @pipeline
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


webhook = Webhook(name="fennel_webhook")

__owner__ = "eng@fennel.ai"


@mock
def test_invalid_union(client):
    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            t: datetime

        @meta(owner="thaqib@fennel.ai")
        @dataset
        class B:
            a1: int = field(key=True)
            a2: str
            t: datetime

            @pipeline
            @inputs(A)
            def from_a(cls, a: Dataset):
                return a + a

    assert (
        str(e.value)
        == """Union over keyed datasets is currently not supported. Found dataset with keys `{'a1': <class 'pandas.core.arrays.integer.Int64Dtype'>}` in pipeline `from_a`"""
    )


@mock
def test_invalid_assign_schema(client):
    @source(
        webhook.endpoint("mysql_relayrides.location"),
        disorder="14d",
        cdc="append",
        tier="local",
    )
    @dataset
    class LocationDS:
        id: int = field(key=True)
        latitude: float
        longitude: float
        created: datetime

    @dataset
    class LocationDS2:
        latitude_int: int = field(key=True)
        longitude_int: int = field(key=True)
        id: int
        created: datetime

        @pipeline
        @inputs(LocationDS)
        def location_ds(cls, location: Dataset):
            ds = location.assign(
                "latitude_int", int, lambda df: df["latitude"] * 1000
            )
            ds = ds.assign(
                "longitude_int", int, lambda df: df["longitude"] * 1000
            )
            ds = ds.drop(["latitude", "longitude"])
            return ds.groupby(["latitude_int", "longitude_int"]).first()

    client.commit(
        message="msg",
        datasets=[LocationDS, LocationDS2],
    )
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "latitude": [1.12312, 2.3423423, 2.24343],
            "longitude": [1.12312, 2.3423423, 2.24343],
            "created": [
                datetime.utcfromtimestamp(1672858163),
                datetime.utcfromtimestamp(1672858163),
                datetime.utcfromtimestamp(1672858163),
            ],
        }
    )

    with pytest.raises(Exception) as e:
        client.log("fennel_webhook", "mysql_relayrides.location", df)
    assert (
        str(e.value)
        == "Error while executing pipeline `location_ds` in dataset `LocationDS`: Error in assign node for column `latitude_int` for pipeline `LocationDS2.location_ds`, Field `latitude_int` is of type int, but the column in the dataframe is of type `Float64`. Error found during checking schema for `LocationDS2.location_ds`."
    )
