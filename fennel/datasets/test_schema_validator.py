import json
from datetime import datetime
from typing import Optional, List

import pandas as pd
import pytest

from fennel.datasets import (
    Count,
    dataset,
    field,
    pipeline,
    Dataset,
    Sum,
    Min,
    Max,
)
from fennel.dtypes import Window, Continuous, Session
from fennel.lib import meta, inputs


@meta(owner="test@test.com")
@dataset
class MovieRating:
    movie: str = field(key=True)
    rating: float
    t: datetime


@meta(owner="test@test.com")
@dataset(index=True)
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

            @pipeline
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

            @pipeline
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.drop(["x", "t"])

    assert (
        str(e.value)
        == """Field `x` is a key or timestamp field in schema of drop node input '[Dataset:A]'. Value fields are: ['y']"""
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

            @pipeline
            @inputs(A)
            def my_pipeline(cls, a: Dataset):
                return a.drop(["t"])

    assert (
        str(e.value)
        == """Field `t` is a key or timestamp field in schema of drop node input '[Dataset:A]'. Value fields are: ['y']"""
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

            @pipeline
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

            @pipeline
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

            @pipeline
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

            @pipeline
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
@dataset(index=True, history="4m")
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

            @pipeline
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
                    Sum(
                        window=Continuous("1w"),
                        of="transaction_amount",
                        into_field=str(
                            cls.sum_categ_fraudulent_transactions_7d
                        ),
                    ),
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

            @pipeline
            @inputs(A1)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    Min(
                        of="b",
                        into_field="b_min",
                        window=Continuous("1d"),
                        default=0.91,
                    ),
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

            @pipeline
            @inputs(A2)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    Max(
                        of="b",
                        into_field="b_max",
                        window=Continuous("1d"),
                        default=1.91,
                    ),
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

            @pipeline
            @inputs(A3)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    Max(
                        of="b",
                        into_field="b_max",
                        window=Continuous("1d"),
                        default=2.91,
                    ),
                )

    assert (
        str(e.value) == """invalid max: type of field `b` is not int or float"""
    )


def test_aggregation_along():
    with pytest.raises(ValueError) as e:

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

            @pipeline
            @inputs(A1)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    Min(
                        of="b",
                        into_field="b_min",
                        window=Continuous("1d"),
                        default=0.91,
                    ),
                    along="transaction_time",
                )

    assert (
        str(e.value)
        == """error with along kwarg of aggregate operator: no datetime field of name `transaction_time`"""
    )
    with pytest.raises(ValueError) as e:

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class A2:
            a: str = field(key=True)
            b: int
            t: datetime = field(timestamp=True)
            transaction_time: int

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class B2:
            a: str = field(key=True)
            b_max: int
            t: datetime

            @pipeline
            @inputs(A2)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    Max(
                        of="b",
                        into_field="b_max",
                        window=Continuous("1d"),
                        default=1.91,
                    ),
                    along="transaction_time",
                )

    assert (
        str(e.value)
        == """error with along kwarg of aggregate operator: `transaction_time` is not of type datetime"""
    )
    with pytest.raises(ValueError) as e:

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class A4:
            a: str = field(key=True)
            b: int
            t: datetime = field(timestamp=True)
            transaction_time: int

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class B4:
            a: str = field(key=True)
            b_max: int
            count: int
            t: datetime

            @pipeline
            @inputs(A2)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    emit=Max(
                        of="b",
                        into_field="b_max",
                        window=Continuous("1d"),
                        default=1.91,
                    ),
                )

    assert (
        str(e.value)
        == """`emit` is a reserved kwarg for aggregate operator and can not be used for an aggregation column"""
    )

    with pytest.raises(ValueError) as e:

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class A3:
            a: str = field(key=True)
            b: int
            t: datetime = field(timestamp=True)
            transaction_time: int

        @meta(owner="nikhil@fennel.ai")
        @dataset
        class B3:
            a: str = field(key=True)
            b_max: int
            t: datetime

            @pipeline
            @inputs(A3)
            def pipeline(cls, a: Dataset):
                return a.groupby("a").aggregate(
                    b_max=Max(
                        of="b",
                        into_field="b_max",
                        window=Continuous("1d"),
                        default=1.91,
                    ),
                    along=Max(
                        of="b",
                        into_field="b_max",
                        window=Continuous("1d"),
                        default=1.91,
                    ),
                )

    assert (
        str(e.value)
        == "`along` is a reserved kwarg for aggregate operator and can not be used for an aggregation column"
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

            @pipeline
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

            @pipeline
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

            @pipeline
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

            @pipeline
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
@dataset(index=True)
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

            @pipeline
            @inputs(A, B)
            def pipeline_join(cls, a: Dataset, b: Dataset):
                return a.join(
                    b, how="left", left_on=["a1"], right_on=["b1", "b2"]
                )

    assert (
        str(e.value)
        == """right_on field `['b1', 'b2']` are not the key fields of the right dataset `B` for `'[Pipeline:pipeline_join]->join node'`."""
    )


@meta(owner="test@test.com")
@dataset(index=True)
class C:
    b1: int = field(key=True)
    b2: Optional[int]
    b3: str
    t: datetime


@meta(owner="test@test.com")
@dataset(index=True)
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

            @pipeline
            @inputs(A, C)
            def pipeline_join(cls, a: Dataset, c: Dataset):
                return a.join(c, how="left", left_on=["a1"], right_on=["b1"])

    assert (
        str(e.value)
        == """Key field `a1` has type `str` in left schema but, key field `b1` has type `int` in right schema for `'[Pipeline:pipeline_join]->join node'`"""
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

            @pipeline
            @inputs(A, E)
            def pipeline_join(cls, a: Dataset, e: Dataset):
                return a.join(e, how="left", on=["a1"])

    assert (
        str(e.value)
        == """Key field `a1` has type `str` in left schema but type `int` in right schema for `'[Pipeline:pipeline_join]->join node'`"""
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

            @pipeline
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

            @pipeline
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

            @pipeline
            @inputs(RatingActivity)
            def pipeline_dedup(cls, rating: Dataset):
                return rating.dedup(by=["director"])

    assert (
        str(e.value)
        == """invalid dedup: field `director` not present in input schema '[Dataset:RatingActivity]'"""
    )


def test_explode_fails_on_keyed_column():
    with pytest.raises(TypeError) as e:

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

            @pipeline
            @inputs(SingleHits)
            def pipeline_exploded(cls, hits: Dataset):
                return hits.explode(columns=["director"])

    assert (
        str(e.value)
        == """Explode over keyed datasets is not defined. Found dataset with keys `['director']` in pipeline `pipeline_exploded`"""
    )


def test_explode_fails_on_missing_column():
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
            director: str = field(key=True)
            movie: str
            revenue: int
            t: datetime

            @pipeline
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

            @pipeline
            @inputs(SingleHits)
            def pipeline_exploded(cls, hits: Dataset):
                return hits.explode(columns=["movie"])

    assert str(e.value) == """Column `movie` in explode is not of type List"""


def test_first_without_key_fails():
    with pytest.raises(ValueError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: str
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class FirstHit:
            director: str
            movie: str
            revenue: int
            t: datetime

            @pipeline
            @inputs(SingleHits)
            def pipeline_first(cls, hits: Dataset):
                return hits.groupby().first()

    assert (
        str(e.value)
        == """'group_by' before 'first' in pipeline_first must specify at least one key"""
    )


def test_first_incorrect_schema_nokey():
    with pytest.raises(TypeError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: str
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class FirstHit:
            director: str
            movie: str
            revenue: int
            t: datetime

            @pipeline
            @inputs(SingleHits)
            def pipeline_first(cls, hits: Dataset):
                return hits.groupby("director").first()

    assert (
        str(e.value)
        == """[TypeError('Field `director` is present in `pipeline pipeline_first output` `key` schema but not present in `FirstHit key` schema.'), TypeError('Field `director` is present in `FirstHit` `value` schema but not present in `pipeline pipeline_first output value` schema.')]"""
    )


def test_first_incorrect_schema_missing_field():
    with pytest.raises(TypeError) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: str
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class FirstHit:
            director: str = field(key=True)
            movie: str
            t: datetime

            @pipeline
            @inputs(SingleHits)
            def pipeline_first(cls, hits: Dataset):
                return hits.groupby("director").first()

    assert (
        str(e.value)
        == """[TypeError('Field `revenue` is present in `pipeline pipeline_first output` `value` schema but not present in `FirstHit value` schema.')]"""
    )


def test_first_wrong_field():
    with pytest.raises(Exception) as e:

        @meta(owner="abhay@fennel.ai")
        @dataset
        class SingleHits:
            director: str
            movie: str
            revenue: int
            t: datetime

        @meta(owner="abhay@fennel.ai")
        @dataset
        class FirstHit:
            actor: str = field(key=True)
            movie: str
            revenue: int
            t: datetime

            @pipeline
            @inputs(SingleHits)
            def pipeline_first(cls, hits: Dataset):
                return hits.groupby("actor").first()

    assert (
        str(e.value)
        == """field `actor` not found in schema of `'[Dataset:SingleHits]'`"""
    )


def test_drop_null():
    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset
        class C1:
            b1: int = field(key=True)
            b2: Optional[int]
            b3: str
            t: datetime

            @pipeline
            @inputs(C)
            def drop_null_noargs(cls, c: Dataset):
                return c.dropnull()

    assert (
        str(e.value)
        == """[TypeError('Field `b2` has type `int` in `pipeline drop_null_noargs output value` schema but type `Optional[int]` in `C1 value` schema.')]"""
    )
    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class C2:
            b1: int = field(key=True)
            b2: int
            b3: str
            t: datetime

            @pipeline
            @inputs(C)
            def drop_null_non_opt(cls, c: Dataset):
                return c.dropnull("b1")

    assert (
        str(e.value)
        == "invalid dropnull `b1` has type `int` expected Optional type"
    )
    with pytest.raises(ValueError) as e:

        @meta(owner="test@test.com")
        @dataset
        class C3:
            b1: int = field(key=True)
            b2: int
            b3: str
            t: datetime

            @pipeline
            @inputs(C)
            def drop_null_non_present(cls, c: Dataset):
                return c.dropnull("b4")

    assert (
        str(e.value)
        == "invalid dropnull column `b4` not present in `'[Dataset:C]'`"
    )


def test_assign():
    @meta(owner="test@test.com")
    @dataset
    class RatingActivity:
        userid: int = field(key=True)
        rating: float
        movie: str
        t: datetime

    with pytest.raises(Exception) as e:

        @dataset
        class RatingActivity1:
            t: datetime
            userid: int = field(key=True)
            movie: str
            rating: float

            @pipeline
            @inputs(RatingActivity)
            def create_dataset(cls, activity: Dataset):
                return activity.assign(
                    name="t",
                    dtype=float,
                    func=lambda df: float(df["t"]),
                )

    assert (
        str(e.value)
        == "Field `t` is a key or timestamp field in schema of assign node input '[Dataset:RatingActivity]'. Value fields are: ['rating', 'movie']"
    )

    with pytest.raises(Exception) as e:

        @dataset
        class RatingActivity2:
            t: datetime
            userid: int = field(key=True)
            movie: str
            rating: float

            @pipeline
            @inputs(RatingActivity)
            def create_dataset(cls, activity: Dataset):
                return activity.assign(
                    name="",
                    dtype=float,
                    func=lambda df: float(df["t"]),
                )

    assert (
        str(e.value)
        == "invalid assign - '[Pipeline:create_dataset]->assign node' must specify a column to assign"
    )

    with pytest.raises(Exception) as e:

        @dataset
        class RatingActivity3:
            t: datetime
            userid: int = field(key=True)
            movie: str
            rating: float

            @pipeline
            @inputs(RatingActivity)
            def create_dataset(cls, activity: Dataset):
                return activity.assign(
                    name="",
                    dtype=float,
                    func=lambda df: float(df["t"]),
                )

    assert (
        str(e.value)
        == "invalid assign - '[Pipeline:create_dataset]->assign node' must specify a column to assign"
    )
    with pytest.raises(Exception) as e:

        @dataset
        class RatingActivity4:
            t: datetime
            userid: int = field(key=True)
            movie: str
            rating: float

            @pipeline
            @inputs(RatingActivity)
            def create_dataset(cls, activity: Dataset):
                return activity.assign(
                    name="userid",
                    dtype=float,
                    func=lambda df: float(df["userid"]),
                )

    assert (
        str(e.value)
        == "Field `userid` is a key or timestamp field in schema of assign node input '[Dataset:RatingActivity]'. Value fields are: ['rating', 'movie']"
    )


def test_window_without_key_fails():
    with pytest.raises(ValueError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            user_id: str
            window: Window
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(window=Session("10m")).aggregate()

    assert (
        str(e.value)
        == "There should be at least one key in 'groupby' to use 'window'"
    )


def test_window_incorrect_schema_optional_key():
    with pytest.raises(TypeError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: Optional[str]
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            user_id: str = field(key=True)
            window: Window = field(key=True)
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(
                    "user_id", window=Session("10m")
                ).aggregate()

    assert (
        str(e.value)
        == """[TypeError('Field `user_id` has type `Optional[str]` in `pipeline pipeline_window output key` schema but type `str` in `Sessions key` schema.')]"""
    )


def test_window_incorrect_schema_nokey():
    with pytest.raises(TypeError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            user_id: str
            window: Window
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(
                    "user_id", window=Session("10m")
                ).aggregate()

    assert (
        str(e.value)
        == """[TypeError('Field `user_id` is present in `pipeline pipeline_window output` `key` schema but not present in `Sessions key` schema.'), TypeError('Field `user_id` is present in `Sessions` `value` schema but not present in `pipeline pipeline_window output value` schema.')]"""
    )


def test_window_incorrect_schema_missing_field():
    with pytest.raises(TypeError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            user_id: str = field(key=True)
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(
                    "user_id", window=Session("10m")
                ).aggregate()

    assert (
        str(e.value)
        == """[TypeError('Field `window` is present in `pipeline pipeline_window output` `key` schema but not present in `Sessions key` schema.')]"""
    )


def test_window_incorrect_schema_dtype_field():
    with pytest.raises(TypeError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            user_id: str = field(key=True)
            window: str = field(key=True)
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(
                    "user_id", window=Session("10m")
                ).aggregate()

    assert (
        str(e.value)
        == """[TypeError('Field `window` has type `Window` in `pipeline pipeline_window output key` schema but type `str` in `Sessions key` schema.')]"""
    )


def test_window_wrong_field():
    with pytest.raises(Exception) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            u_id: str = field(key=True)
            window: Window = field(key=True)
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(
                    "u_id", window=Session("10m")
                ).aggregate()

    assert (
        str(e.value)
        == """field `u_id` not found in schema of `'[Dataset:PageViewEvent]'`"""
    )


def test_window_invalid_gap():
    with pytest.raises(ValueError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str
            page_id: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Sessions:
            user_id: str = field(key=True)
            window: Window = field(key=True)
            t: datetime

            @pipeline
            @inputs(PageViewEvent)
            def pipeline_window(cls, app_event: Dataset):
                return app_event.groupby(
                    "user_id", window=Session("10m 5yy")
                ).aggregate()

    assert (
        str(e.value)
        == "Failed when parsing gap : Invalid character `y` in duration `10m 5yy`."
    )


def test_join_with_wrong_right_index():
    with pytest.raises(ValueError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset(online=True)
        class Users:
            user_id: str = field(key=True)
            age: int
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class Login:
            user_id: str
            cookie: str
            t: datetime

        @meta(owner="nitin@fennel.ai")
        @dataset
        class LoginEvent:
            user_id: str
            cookie: str
            age: int
            t: datetime

            @pipeline
            @inputs(Login, Users)
            def pipeline(cls, login: Dataset, users: Dataset):
                return login.join(users, on=["user_id"], how="inner")

    assert (
        str(e.value)
        == """`offline` needs to be set on index of the right dataset `Users` for `'[Pipeline:pipeline]->join node'`."""
    )


def test_conflicting_key_field_error():
    with pytest.raises(ValueError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent:
            user_id: str = field(key=True)
            page_id: str = field(erase_key=True)
            t: datetime = field(timestamp=True)

    assert str(e.value) == """Non key field cannot be an erase key field."""

    with pytest.raises(ValueError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class PageViewEvent1:
            user_id: str = field(key=True)
            page_id: str = field()
            t: datetime = field(erase_key=True, timestamp=True)

    assert str(e.value) == """Non key field cannot be an erase key field."""


def test_aggregation_after_eager_emit_not_allowed():
    @meta(owner="nitin@fennel.ai")
    @dataset
    class A1:
        user_id: str
        page_id: str
        event_id: str
        t: datetime

    with pytest.raises(ValueError) as e:

        @meta(owner="nitin@fennel.ai")
        @dataset
        class A2:
            user_id: str = field(key=True)
            count: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return (
                    event.groupby("user_id", "page_id")
                    .aggregate(
                        count=Count(window=Continuous("1h")), emit="final"
                    )
                    .groupby("user_id")
                    .aggregate(count=Count(window=Continuous("1h")))
                    .groupby("user_id")
                    .aggregate(count=Count(window=Continuous("1h")))
                )

    assert (
        str(e.value)
        == "Cannot add node 'Aggregate' after a terminal node in pipeline : `pipeline_window`."
    )
