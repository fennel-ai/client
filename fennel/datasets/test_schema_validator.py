import json
from datetime import datetime
from typing import Optional, List

import pandas as pd
import pytest

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Sum, Min, Max
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs
from fennel.lib.window import Window


@meta(owner="test@test.com")
@dataset
class MovieRating:
    movie: str = field(key=True)
    rating: float
    t: datetime


@meta(owner="test@test.com")
@dataset
class MovieRevenue:
    movie: str = field(key=True)
    revenue: int
    t: datetime


def test_join_schema_validation():
    with pytest.raises(TypeError) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class MovieStats:
            movie: str = field(key=True)
            rating: float
            revenue: int
            t: datetime

            @pipeline(version=1)
            @inputs(MovieRating, MovieRevenue)
            def pipeline_join(cls, rating: Dataset, revenue: Dataset):
                return rating.join(revenue, how="left", on=[str(cls.movie)])

    assert (
        str(e.value)
        == """[TypeError('Field `revenue` has type `Optional[int]` in `pipeline pipeline_join output value` schema but type `int` in `MovieStats value` schema.')]"""
    )


def test_drop_schema_validation_drop_keys():
    with pytest.raises(ValueError) as e:

        @meta(owner="abc@xyx.com")
        @dataset
        class A:
            x: int = field(key=True)
            y: int
            t: datetime

        @meta(owner="abc@xyz.ai")
        @dataset
        class B:
            y: int
            t: datetime

            @pipeline()
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.drop(["x", "t"])

    assert (
        str(e.value)
        == """Field `x` is not a non-key non-timestamp field in schema of drop node input '[Dataset:A]'. Value fields are: ['y']"""
    )


def test_drop_schema_validation_drop_timestamp():
    with pytest.raises(ValueError) as e:

        @meta(owner="abc@xyx.com")
        @dataset
        class A:
            x: int = field(key=True)
            y: int
            t: datetime

        @meta(owner="abc@xyz.ai")
        @dataset
        class B:
            x: int = field(key=True)
            t: datetime

            @pipeline()
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.drop(["t"])

    assert (
        str(e.value)
        == """Field `t` is not a non-key non-timestamp field in schema of drop node input '[Dataset:A]'. Value fields are: ['y']"""
    )


def test_drop_schema_validation_drop_empty():
    with pytest.raises(ValueError) as e:

        @meta(owner="abc@xyx.com")
        @dataset
        class A:
            x: int = field(key=True)
            y: int
            t: datetime

        @meta(owner="abc@xyz.ai")
        @dataset
        class B:
            x: int = field(key=True)
            y: int
            t: datetime

            @pipeline()
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.drop([])

    assert (
        str(e.value)
        == """invalid drop - '[Pipeline:my_pipeline]->drop node' must have at least one column to drop"""
    )


def test_rename_duplicate_names_one():
    with pytest.raises(ValueError) as e:

        @meta(owner="abc@xyx.com")
        @dataset
        class A:
            x: int = field(key=True)
            y: int
            t: datetime

        @meta(owner="abc@xyz.ai")
        @dataset
        class B:
            a: int = field(key=True)
            b: int = field(key=True)
            t: datetime

            @pipeline()
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.rename({"x": "a", "y": "a"})

    assert (
        str(e.value)
        == """Field `a` already exists in schema of rename node '[Pipeline:my_pipeline]->rename node'."""
    )


def test_rename_duplicate_names_two():
    with pytest.raises(ValueError) as e:

        @meta(owner="abc@xyx.com")
        @dataset
        class A:
            x: int = field(key=True)
            y: int
            t: datetime

        @meta(owner="abc@xyz.ai")
        @dataset
        class B:
            x: int = field(key=True)
            y: int = field(key=True)
            t: datetime

            @pipeline()
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.rename({"x": "y"})

    assert (
        str(e.value)
        == """Field `y` already exists in schema of rename node '[Pipeline:my_pipeline]->rename node'."""
    )


@meta(owner="test@test.com")
@dataset
class RatingActivity:
    userid: int
    rating: float
    movie: str
    t: datetime


def test_add_key():
    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @dataset
        class PositiveRatingActivity:
            userid: int
            rating: float
            movie: str = field(key=True)
            t: datetime

            @pipeline(version=1)
            @inputs(RatingActivity)
            def filter_positive_ratings(cls, rating: Dataset):
                return rating.filter(lambda df: df[df["rating"] >= 3.5])

    assert (
        str(e.value)
        == """[TypeError('Field `movie` is present in `PositiveRatingActivity` `key` schema but not present in `pipeline filter_positive_ratings output key` schema.'), TypeError('Field `movie` is present in `pipeline filter_positive_ratings output` `value` schema but not present in `PositiveRatingActivity value` schema.')]"""
    )


@meta(owner="me@fennel.ai")
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset(history="4m")
class MerchantInfo:
    merchant_id: int = field(key=True)
    category: str
    location: str
    timestamp: datetime


def test_aggregation_sum():
    with pytest.raises(Exception) as e:

        @meta(owner="me@fennel.ai")
        @dataset
        class FraudReportAggregatedDataset:
            category: str = field(key=True)
            timestamp: datetime
            sum_categ_fraudulent_transactions_7d: int

            @pipeline(version=1)
            @inputs(Activity, MerchantInfo)
            def create_fraud_dataset(
                cls, activity: Dataset, merchant_info: Dataset
            ):
                def extract_info(df: pd.DataFrame) -> pd.DataFrame:
                    df_json = df["metadata"].apply(json.loads).apply(pd.Series)
                    df_timestamp = pd.concat([df_json, df["timestamp"]], axis=1)
                    return df_timestamp

                def fillna(df: pd.DataFrame) -> pd.DataFrame:
                    df["category"].fillna("unknown", inplace=True)
                    return df

                filtered_ds = activity.filter(
                    lambda df: df[df["action_type"] == "report"]
                )
                ds = filtered_ds.transform(
                    extract_info,
                    schema={
                        "transaction_amount": float,
                        "merchant_id": int,
                        "timestamp": datetime,
                    },
                )
                ds = ds.join(
                    merchant_info,
                    how="left",
                    on=["merchant_id"],
                )
                new_schema = ds.schema()
                new_schema.update(merchant_info.schema())
                new_schema["category"] = str
                ds = ds.transform(fillna, schema=new_schema)
                return ds.groupby("category").aggregate(
                    [
                        Sum(
                            window=Window("1w"),
                            of="transaction_amount",
                            into_field=str(
                                cls.sum_categ_fraudulent_transactions_7d
                            ),
                        ),
                    ]
                )

    assert (
        str(e.value)
        == """[TypeError('Field `sum_categ_fraudulent_transactions_7d` has type `float` in `pipeline create_fraud_dataset output value` schema but type `int` in `FraudReportAggregatedDataset value` schema.')]"""
    )


def test_aggregation_min_max():
    with pytest.raises(TypeError) as e:

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class A1:
            a: str = field(key=True)
            b: int
            t: datetime

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class B1:
            a: str = field(key=True)
            b_min: int
            t: datetime

            @pipeline(version=1)
            @inputs(A1)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    [
                        Min(
                            of="b",
                            into_field="b_min",
                            window=Window("1d"),
                            default=0.91,
                        ),
                    ]
                )

    assert (
        str(e.value)
        == """invalid min: default value `0.91` not of type `int`"""
    )
    with pytest.raises(TypeError) as e:

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class A2:
            a: str = field(key=True)
            b: int
            t: datetime

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class B2:
            a: str = field(key=True)
            b_max: int
            t: datetime

            @pipeline(version=1)
            @inputs(A2)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    [
                        Max(
                            of="b",
                            into_field="b_max",
                            window=Window("1d"),
                            default=1.91,
                        ),
                    ]
                )

    assert (
        str(e.value)
        == """invalid max: default value `1.91` not of type `int`"""
    )
    with pytest.raises(TypeError) as e:

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class A3:
            a: str = field(key=True)
            b: str
            t: datetime

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class B3:
            a: str = field(key=True)
            b_max: str
            t: datetime

            @pipeline(version=1)
            @inputs(A3)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    [
                        Max(
                            of="b",
                            into_field="b_max",
                            window=Window("1d"),
                            default=2.91,
                        ),
                    ]
                )

    assert (
        str(e.value) == """invalid max: type of field `b` is not int or float"""
    )


@meta(owner="test@test.com")
@dataset
class A:
    a1: str = field(key=True)
    b1: float
    t: datetime


def test_transform():
    with pytest.raises(Exception) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class A1:
            a1: str = field(key=True)
            b1: float
            t: datetime

            @pipeline(version=1)
            @inputs(A)
            def transform(cls, a: Dataset):
                return a.transform(
                    lambda df: df,
                    schema={
                        "a1": int,
                        "b1": float,
                        "t": datetime,
                    },
                )

    assert (
        str(e.value)
        == """Key field a1 has type str in input schema of transform but type int in output schema of '[Pipeline:transform]->transform node'."""
    )

    with pytest.raises(Exception) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class A2:
            a1: str = field(key=True)
            b1: float
            t: datetime

            @pipeline(version=1)
            @inputs(A)
            def transform(cls, a: Dataset):
                return a.transform(
                    lambda df: df,
                    schema={
                        "a1": str,
                        "b1": int,
                        "t": datetime,
                    },
                )

    assert (
        str(e.value)
        == """[TypeError('Field `b1` has type `int` in `pipeline transform output value` schema but type `float` in `A2 value` schema.')]"""
    )

    with pytest.raises(Exception) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class A3:
            a1: str = field(key=True)
            b1: int
            t: datetime

            @pipeline(version=1)
            @inputs(A)
            def transform(cls, a: Dataset):
                return a.transform(
                    lambda df: df,
                    schema={
                        "a2": str,
                        "b1": int,
                        "t": datetime,
                    },
                )

    assert (
        str(e.value)
        == """Key field a1 must be present in schema of '[Pipeline:transform]->transform node'."""
    )

    with pytest.raises(Exception) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class A4:
            a1: str = field(key=True)
            b1: int
            t: datetime

            @pipeline(version=1)
            @inputs(A)
            def transform(cls, a: Dataset):
                return a.transform(
                    lambda df: df,
                    schema={
                        "a1": str,
                        "b1": int,
                        "d": datetime,
                    },
                )

    assert (
        str(e.value)
        == """Timestamp field t must be present in schema of '[Pipeline:transform]->transform node'."""
    )


@meta(owner="test@test.com")
@dataset
class B:
    b1: str = field(key=True)
    b2: int
    t: datetime


def test_join_schema_validation_value():
    with pytest.raises(ValueError) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class C:
            movie: str = field(key=True)
            rating: float
            revenue: int
            t: datetime

            @pipeline(version=1)
            @inputs(A, B)
            def pipeline_join(cls, a: Dataset, b: Dataset):
                return a.join(
                    b, how="left", left_on=["a1"], right_on=["b1", "b2"]
                )

    assert (
        str(e.value)
        == """right_on field ['b1', 'b2'] are not the key fields of the right dataset B."""
    )


@meta(owner="test@test.com")
@dataset
class C:
    b1: int = field(key=True)
    b2: Optional[int]
    b3: str
    t: datetime


@meta(owner="test@test.com")
@dataset
class E:
    a1: int = field(key=True)
    b2: Optional[int]
    b3: str
    t: datetime


def test_join_schema_validation_type():
    with pytest.raises(TypeError) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class D:
            a1: str = field(key=True)
            b1: float
            b2: Optional[int]
            b3: Optional[str]
            t: datetime

            @pipeline(version=1)
            @inputs(A, C)
            def pipeline_join(cls, a: Dataset, c: Dataset):
                return a.join(c, how="left", left_on=["a1"], right_on=["b1"])

    assert (
        str(e.value)
        == """Key field a1 has type str in left schema but, key field b1 has type int in right schema."""
    )

    with pytest.raises(TypeError) as e:

        @meta(owner="aditya@fennel.ai")
        @dataset
        class F:
            a1: str = field(key=True)
            b1: float
            b2: Optional[int]
            b3: Optional[str]
            t: datetime

            @pipeline(version=1)
            @inputs(A, E)
            def pipeline_join(cls, a: Dataset, e: Dataset):
                return a.join(e, how="left", on=["a1"])

    assert (
        str(e.value)
        == """Key field a1 has type str in left schema but type int in right schema."""
    )


# dedup is not supported on keyed datasets
def test_dedup_ds_with_key_fails():
    with pytest.raises(ValueError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class MovieStats:
            movie: str = field(key=True)
            rating: float
            revenue: int
            t: datetime

            @pipeline(version=1)
            @inputs(MovieRating)
            def pipeline_dedup(cls, rating: Dataset):
                return rating.dedup(by=[MovieRating.movie])

    assert (
        str(e.value)
        == """invalid dedup: input schema '[Dataset:MovieRating]' has key columns"""
    )


# Schema of deduped dataset should match source dataset
def test_dedup_schema_different_fails():
    with pytest.raises(TypeError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class RatingActivity:
            user: str
            movie: str
            rating: float
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class DedupedRatingActivity:
            movie: str
            rating: float
            t: datetime

            @pipeline(version=1)
            @inputs(RatingActivity)
            def pipeline_dedup(cls, rating: Dataset):
                return rating.dedup()

    assert (
        str(e.value)
        == """[TypeError('Field `user` is present in `pipeline pipeline_dedup output` `value` schema but not present in `DedupedRatingActivity value` schema.')]"""
    )


# Schema of deduped dataset should by on a field present in the original dataset
def test_dedup_on_missing_field():
    with pytest.raises(ValueError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class RatingActivity:
            user: str
            movie: str
            rating: float
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class DedupedRatingActivity:
            user: str
            movie: str
            rating: float
            t: datetime

            @pipeline(version=1)
            @inputs(RatingActivity)
            def pipeline_dedup(cls, rating: Dataset):
                return rating.dedup(by=["director"])

    assert (
        str(e.value)
        == """invalid dedup: field `director` not present in input schema '[Dataset:RatingActivity]'"""
    )


def test_explode_fails_on_keyed_column():
    with pytest.raises(ValueError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: List[str] = field(key=True)
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class ExplodedHits:
            director: str = field(key=True)
            movie: str
            revenue: int
            t: datetime

            @pipeline(version=1)
            @inputs(SingleHits)
            def pipeline_exploded(cls, hits: Dataset):
                return hits.explode(columns=["director"])

    assert (
        str(e.value)
        == """Field `director` is not a non-key non-timestamp field in schema of explode node input '[Dataset:SingleHits]'. Value fields are: ['movie', 'revenue']"""
    )


def test_explode_fails_on_missing_column():
    with pytest.raises(ValueError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: List[str] = field(key=True)
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class ExplodedHits:
            director: str = field(key=True)
            movie: str
            revenue: int
            t: datetime

            @pipeline(version=1)
            @inputs(SingleHits)
            def pipeline_exploded(cls, hits: Dataset):
                return hits.explode(columns=["actor"])

    assert (
        str(e.value)
        == """Column `actor` in explode not present in input '[Dataset:SingleHits]': ['director', 'movie', 'revenue', 't']"""
    )


def test_explode_fails_on_primitive_column():
    with pytest.raises(ValueError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: List[str]
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class ExplodedHits:
            director: List[str]
            movie: str
            revenue: int
            t: datetime

            @pipeline(version=1)
            @inputs(SingleHits)
            def pipeline_exploded(cls, hits: Dataset):
                return hits.explode(columns=["movie"])

    assert str(e.value) == """Column `movie` in explode is not of type List"""
