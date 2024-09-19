from datetime import datetime, timezone, date
from typing import Optional, List, Union

import pandas as pd
import pytest

from fennel.connectors import Webhook, source
from fennel.datasets import (
    dataset,
    pipeline,
    field,
    Dataset,
    Count,
    Average,
    Stddev,
    Distinct,
    ExpDecaySum,
    index,
)
from fennel.dtypes import struct, Window, Continuous, Session, Hopping
from fennel.expr import col
from fennel.lib import (
    meta,
    inputs,
    expectations,
    expect_column_values_to_be_between,
)
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
        == """invalid select - '[Pipeline:from_a]->select node' key field : `a1` must be in columns"""
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
        class B1:
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


def strip_whitespace(s):
    return "".join(s.split())


def test_incorrect_assign_expr_type():
    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset
        class RatingActivityTransformed:
            userid: int
            rating_sq: float
            movie_suffixed: str
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def transform(cls, rating: Dataset):
                return rating.assign(
                    rating_sq=(col("rating") * col("rating")).astype(str),
                    movie_suffixed=col("movie")
                    .str.concat("_suffix")
                    .astype(int),
                ).drop("rating", "movie")

    expected_err = "'movie_suffixed' is expected to be of type `int`, but evaluates to `str`. Full expression: `col('movie') + \"_suffix\"`"
    assert expected_err in str(e.value)

    with pytest.raises(TypeError) as e2:

        @meta(owner="test@test.com")
        @dataset
        class RatingActivityTransformed2:
            userid: int
            rating_sq: int
            movie_suffixed: str
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def transform(cls, rating: Dataset):
                return rating.assign(
                    rating_sq=(col("rating") * col("rating")).astype(float),
                    movie_suffixed=col("movie")
                    .str.concat("_suffix")
                    .astype(str),
                ).drop("rating", "movie")

    assert (
        str(e2.value)
        == """[TypeError('Field `rating_sq` has type `float` in `pipeline transform output value` schema but type `int` in `RatingActivityTransformed2 value` schema.')]"""
    )

    with pytest.raises(ValueError) as e2:

        @meta(owner="test@test.com")
        @dataset
        class RatingActivityTransformed3:
            userid: int
            rating_sq: int
            movie_suffixed: str
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def transform(cls, rating: Dataset):
                return rating.assign(
                    rating_sq=(col("rating") % col("rating")).astype(float),
                    movie_suffixed=(col("movie") + "_suffix").astype(str),
                ).drop("rating", "movie")

    assert (
        str(e2.value)
        == """invalid assign - '[Pipeline:transform]->assign node' error in expression for column `movie_suffixed`: Failed to compile expression: invalid expression: both sides of '+' must be numeric types but found String & String, left: col(movie), right: lit(String("_suffix"))"""
    )


def test_incorrect_filter_expr_type():
    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset
        class RatingActivityFiltered:
            userid: int
            rating: float
            t: datetime

            @pipeline
            @inputs(RatingActivity)
            def transform(cls, rating: Dataset):
                return rating.filter(col("rating") + 3.5).drop("movie")

    assert (
        str(e.value)
        == """Filter expression must return type bool, found float."""
    )


def test_expectations_on_aggregated_datasets():
    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class PositiveRatingActivity:
            cnt_rating: int
            movie: str = field(key=True)
            t: datetime

            @expectations
            def dataset_expectations(cls):
                return [
                    expect_column_values_to_be_between(
                        column=str(cls.cnt_rating), min_value=0, max_value=100
                    ),
                ]

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
                            window=Continuous("forever"),
                            into_field=str(cls.cnt_rating),
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Dataset PositiveRatingActivity has a terminal aggregate node with Continuous windows, we currently dont support expectations on continuous windows.This is because values are materialized into buckets which are combined at read time."
    )

    # Discrete window is fine
    @meta(owner="test@test.com")
    @dataset
    class PositiveRatingActivity2:
        cnt_rating: int
        movie: str = field(key=True)
        t: datetime

        @expectations
        def dataset_expectations(cls):
            return [
                expect_column_values_to_be_between(
                    column=str(cls.cnt_rating), min_value=0, max_value=100
                ),
            ]

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
                        window=Hopping("7d", "1d"),
                        into_field=str(cls.cnt_rating),
                    ),
                ],
            )


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
                            window=Continuous("forever"),
                            into_field=str(cls.cnt_rating),
                        ),
                        Count(
                            window=Continuous("forever"),
                            into_field=str(cls.unique_ratings),
                            of="rating",
                            unique=True,
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Invalid aggregate `window=Continuous(duration='forever') into_field='unique_ratings' of='rating' unique=True approx=False dropnull=False`: Exact unique counts are not yet supported, please set approx=True"
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
                            window=Continuous("forever"),
                            into_field=str(cls.cnt_rating),
                        ),
                        Distinct(
                            window=Continuous("forever"),
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
                            window=Continuous("forever"),
                            into_field=str(cls.cnt_rating),
                        ),
                        Distinct(
                            window=Continuous("forever"),
                            into_field=str(cls.unique_users),
                            of="userid",
                            unordered=False,
                        ),
                    ],
                )

    assert (
        str(e.value)
        == "Invalid aggregate `window=Continuous(duration='forever') into_field='unique_users' of='userid' unordered=False dropnull=False`: Distinct requires unordered=True"
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
                            window=Continuous("forever"),
                            into_field=str(cls.cnt_rating),
                        ),
                        Average(
                            window=Continuous("forever"),
                            into_field=str(cls.avg_rating),
                            of="rating",
                            default=0,
                        ),
                        Stddev(
                            window=Continuous("forever"),
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

        @dataset(index=True)
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


def test_dataset_incorrect_join_fields():
    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @dataset(index=True)
        class ABC:
            user_id: int = field(key=True)
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
                c = a.join(b, how="inner", on=["user_id"], fields=["rank"])  # type: ignore
                return c

    assert (
        str(e.value)
        == "Field `rank` specified in fields ['rank'] doesn't exist in "
        "allowed fields ['age', 'timestamp'] of right schema of "
        "'[Pipeline:create_pipeline]->join node'."
    )

    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @dataset(index=True)
        class ABC:
            user_id: int = field(key=True)
            age: int
            timestamp: datetime

        @dataset
        class XYZJoinedABC1:
            user_id: int
            name: str
            age: int
            timestamp: datetime

            @pipeline
            @inputs(XYZ, ABC)
            def create_pipeline(cls, a: Dataset, b: Dataset):
                c = a.join(b, how="inner", on=["user_id"], fields=["user_id"])  # type: ignore
                return c

    assert (
        str(e.value)
        == "Field `user_id` specified in fields ['user_id'] doesn't exist in "
        "allowed fields ['age', 'timestamp'] of right schema of "
        "'[Pipeline:create_pipeline]->join node'."
    )

    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @dataset(index=True)
        class ABC:
            user_id: int = field(key=True)
            age: int
            timestamp: datetime

        @dataset
        class XYZJoinedABC2:
            user_id: int
            name: str
            age: int
            timestamp: datetime

            @pipeline
            @inputs(XYZ, ABC)
            def create_pipeline(cls, a: Dataset, b: Dataset):
                c = a.join(b, how="inner", on=["user_id"], fields=["timestamp"])  # type: ignore
                return c

    assert (
        str(e.value)
        == "Field `timestamp` specified in fields ['timestamp'] already "
        "exists in left schema of '[Pipeline:create_pipeline]->join node'."
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

        @dataset(index=True)
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
        env="local",
    )
    @dataset
    class LocationDS:
        id: int
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
                datetime.fromtimestamp(1672858163, tz=timezone.utc),
                datetime.fromtimestamp(1672858163, tz=timezone.utc),
                datetime.fromtimestamp(1672858163, tz=timezone.utc),
            ],
        }
    )

    with pytest.raises(Exception) as e:
        client.log("fennel_webhook", "mysql_relayrides.location", df)
    assert (
        str(e.value)
        == "Error while executing pipeline `location_ds` in dataset `LocationDS2`: Error in assign node for column `latitude_int` for pipeline `LocationDS2.location_ds`, Field `latitude_int` is of type int, but the column in the dataframe is of type `Float64`. Error found during checking schema for `LocationDS2.location_ds`."
    )


def test_non_keyed_index_dataset_raises_exception():
    with pytest.raises(Exception) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset(online=True)
        class Users:
            user_id: str
            age: int
            t: datetime

    assert (
        str(e.value)
        == "Index is only applicable for datasets with keyed fields. Found zero key fields for dataset : `Users`."
    )


def test_two_indexes_dataset_raises_exception():
    with pytest.raises(Exception) as e:

        @meta(owner="nitin@fennel.ai")
        @index(type="primary", online=True, offline=None)
        @index(type="primary", online=True, offline=None)
        @dataset
        class Users1:
            user_id: str = field(key=True)
            age: int
            t: datetime

    assert (
        str(e.value)
        == "`index` can only be called once on a Dataset. Found either more than one index decorators on Dataset "
        "`Users1` or found 'index', 'offline' or 'online' param on @dataset with @index decorator."
    )

    with pytest.raises(Exception) as e:

        @meta(owner="nitin@fennel.ai")
        @index(type="primary", online=True, offline=None)
        @dataset(index=True)
        class Users2:
            user_id: str = field(key=True)
            age: int
            t: datetime

    assert (
        str(e.value)
        == "`index` can only be called once on a Dataset. Found either more than one index decorators on Dataset "
        "`Users2` or found 'index', 'offline' or 'online' param on @dataset with @index decorator."
    )


@mock
def test_source_and_pipelines_together(client):
    @meta(owner="test@test.com")
    @source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
    @index
    @dataset
    class UserInfoDataset:
        user_id: int = field(key=True).meta(description="User ID")  # type: ignore
        name: str = field().meta(description="User name")  # type: ignore
        age: Optional[int]
        country: Optional[str]
        timestamp: datetime = field(timestamp=True)

    @meta(owner="test@test.com")
    @source(
        webhook.endpoint("UserInfoDatasetDerived"), disorder="14d", cdc="upsert"
    )
    @index
    @dataset
    class UserInfoDatasetDerived:
        user_id: int = field(key=True, erase_key=True).meta(description="User ID")  # type: ignore
        name: str = field().meta(description="User name")  # type: ignore
        country_name: Optional[str]
        ts: datetime = field(timestamp=True)

        @pipeline
        @inputs(UserInfoDataset)
        def get_info(cls, info: Dataset):
            x = info.rename({"country": "country_name", "timestamp": "ts"})
            return x.drop(columns=["age"])

    with pytest.raises(Exception) as e:
        client.commit(
            message="msg",
            datasets=[UserInfoDataset, UserInfoDatasetDerived],
        )
    assert (
        str(e.value)
        == "Dataset `UserInfoDatasetDerived` has a source and pipelines defined. Please define either a source or pipelines, not both."
    )


__owner__ = "aditya@fennel.ai"


def test_invalid_operators_over_keyed_datasets():
    # First operator over keyed datasets is not defined
    with pytest.raises(Exception) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            t: datetime

        @dataset
        class ABCDataset:
            a2: int = field(key=True)
            a1: int
            t: datetime

            @pipeline
            @inputs(A)
            def pipeline1(cls, a: Dataset):
                return a.groupby("a2").first()

    assert (
        str(e.value)
        == "First over keyed datasets is not defined. Found dataset with keys `['a1']` in pipeline `pipeline1`"
    )

    # Latest operator over keyed datasets is not defined
    with pytest.raises(Exception) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            t: datetime

        @dataset
        class ABCDataset2:
            a1: int = field(key=True)
            a2: int
            t: datetime

            @pipeline
            @inputs(A)
            def pipeline1(cls, a: Dataset):
                return a.groupby("a2").latest()

    assert (
        str(e.value)
        == "Latest over keyed datasets is not defined. Found dataset with keys `['a1']` in pipeline `pipeline1`"
    )

    # Windows over keyed datasets is not defined
    with pytest.raises(Exception) as e:

        @dataset
        class Events:
            event_id: int = field(key=True)
            user_id: int
            page_id: int
            t: datetime

        @dataset
        class Sessions:
            user_id: int = field(key=True)
            window: Window = field(key=True)
            t: datetime

            @pipeline
            @inputs(Events)
            def pipeline1(cls, a: Dataset):
                return a.groupby("user_id", window=Session("60m")).aggregate()

    assert (
        str(e.value)
        == "Using 'window' param in groupby on keyed dataset is not allowed. Found dataset with keys `['event_id']` in pipeline `pipeline1`."
    )


@mock
def test_invalid_timestamp_field(client):
    with pytest.raises(ValueError) as e:

        @dataset
        class A:
            a1: int = field(key=True)
            a2: int
            t: date = field(timestamp=True)

        client.commit(datasets=[A], message="first_commit")

    assert (
        str(e.value)
        == "'date' dtype fields cannot be marked as timestamp field. Found field : `t` "
        "of dtype :  `<class 'datetime.date'>` in dataset `A`"
    )


def test_invalid_window_aggregation():
    @dataset
    class Events:
        event_id: int
        user_id: int
        page_id: int
        t: datetime

    with pytest.raises(ValueError) as e:

        @dataset
        class Sessions2:
            user_id: int = field(key=True)
            window: Window = field(key=True)
            t: datetime
            count: int

            @pipeline
            @inputs(Events)
            def pipeline1(cls, a: Dataset):
                return a.groupby("user_id", window=Session("60m")).first()

    assert (
        str(e.value)
        == "Only 'aggregate' method is allowed after 'groupby' when you have defined a window."
    )

    with pytest.raises(AttributeError) as e:

        @dataset
        class Sessions3:
            user_id: int = field(key=True)
            window: Window = field(key=True)
            t: datetime
            count: int

            @pipeline
            @inputs(Events)
            def pipeline1(cls, a: Dataset):
                return a.groupby("user_id").window(window="fdfd")  # type: ignore

    assert str(e.value) == "'GroupBy' object has no attribute 'window'"

    with pytest.raises(AttributeError) as e:

        @dataset
        class Sessions4:
            user_id: int = field(key=True)
            window: Window = field(key=True)
            t: datetime
            count: int

            @pipeline
            @inputs(Events)
            def pipeline1(cls, a: Dataset):
                return a.groupby("user_id", window=Session("19m")).summarize()  # type: ignore

    assert str(e.value) == "'GroupBy' object has no attribute 'summarize'"


def test_invalid_exponential_aggregation():
    @source(webhook.endpoint("A1"), cdc="append", disorder="14d")
    @dataset
    class A1:
        user_id: str
        page_id: str
        timespent: int
        event_id: str
        t: datetime

    with pytest.raises(TypeError) as e:

        @dataset(index=True)
        class A10:
            user_id: str = field(key=True)
            timespent_sum: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").aggregate(
                    timespent_sum=ExpDecaySum(
                        of="timespent",
                        window=Continuous("forever"),
                        half_life="1d",
                    )
                )

    assert (
        str(e.value)
        == "[TypeError('Field `timespent_sum` has type `float` in `pipeline pipeline_window output value` schema but type `int` in `A10 value` schema.')]"
    )

    with pytest.raises(ValueError) as e:

        @dataset(index=True)
        class A11:
            user_id: str = field(key=True)
            timespent_sum: float
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").aggregate(
                    timespent_sum=ExpDecaySum(
                        of="timespent",
                        window=Continuous("forever"),
                        half_life=3,
                    )
                )

    assert (
        str(e.value)
        == "Invalid half life duration for exp decay aggregation: duration 3 must be a specified as a string for eg. 1d/2m/3y."
    )

    with pytest.raises(Exception) as e:

        @dataset(index=True)
        class A12:
            user_id: str = field(key=True)
            count: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return (
                    event.groupby("user_id", "page_id")
                    .aggregate(
                        timespent_sum=ExpDecaySum(
                            of="timespent",
                            window=Continuous("forever"),
                            half_life="1d",
                        ),
                        emit="final",
                    )
                    .groupby("user_id")
                    .aggregate(count=Count(window=Continuous("1h")))
                )

    assert (
        str(e.value) == "ExpDecaySum aggregation does not support emit='final'."
    )

    with pytest.raises(ValueError) as e:

        @dataset(index=True)
        class A13:
            user_id: str = field(key=True)
            count: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return (
                    event.groupby("user_id", "page_id")
                    .aggregate(
                        timespent_sum=ExpDecaySum(
                            of="timespent",
                            window=Continuous("forever"),
                            half_life="1d",
                        )
                    )
                    .groupby("user_id")
                    .aggregate(count=Count(window=Continuous("1h")))
                )

    assert (
        str(e.value)
        == "Cannot add node 'Aggregate' after a terminal node in pipeline : `pipeline_window`."
    )

    with pytest.raises(ValueError) as e:

        @dataset(index=True)
        class A14:
            user_id: str = field(key=True)
            count: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return (
                    event.groupby("user_id", "page_id")
                    .aggregate(
                        timespent_sum=ExpDecaySum(
                            of="timespent",
                            window=Continuous("forever"),
                            half_life="1d",
                        )
                    )
                    .groupby("user_id")
                    .aggregate(count=Count(window=Continuous("1h")))
                )

    assert (
        str(e.value)
        == "Cannot add node 'Aggregate' after a terminal node in pipeline : `pipeline_window`."
    )

    with pytest.raises(TypeError) as e:

        @dataset(index=True)
        class A15:
            user_id: str = field(key=True)
            timespent_sum: float
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").aggregate(
                    timespent_sum=ExpDecaySum(
                        of="page_id",
                        window=Continuous("forever"),
                        half_life="1d",
                    )
                )

    assert (
        str(e.value)
        == "Cannot take exponential decay sum of field page_id of type str"
    )
