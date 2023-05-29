import json
from datetime import datetime

import pandas as pd
import pytest
from typing import Optional

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Sum
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
                return rating.left_join(revenue, on=[str(cls.movie)])

    assert (
        str(e.value)
        == """[TypeError('Field `revenue` has type `Optional[int]` in `pipeline pipeline_join output value` schema but type `int` in `MovieStats value` schema.')]"""
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


def test_aggregation():
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
                ds = ds.left_join(
                    merchant_info,
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
                return a.left_join(b, left_on=["a1"], right_on=["b1", "b2"])

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
                return a.left_join(c, left_on=["a1"], right_on=["b1"])

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
                return a.left_join(e, on=["a1"])

    assert (
        str(e.value)
        == """Key field a1 has type str in left schema but type int in right schema."""
    )
