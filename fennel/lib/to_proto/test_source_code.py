from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, Dataset, pipeline
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.aggregate import Sum
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.lib.to_proto.source_code import (
    get_featureset_core_code,
    get_dataset_core_code,
)
from fennel.lib.window import Window


@meta(owner="me@fennel.ai")
@dataset
class UserAge:
    name: str = field(key=True)
    age: int
    city: str
    timestamp: datetime


@meta(owner="me@fennel.ai")
@dataset
class UserAgeNonTable:
    name: str
    age: int
    city: str
    timestamp: datetime


@dataset
class UserAgeAggregated:
    city: str = field(key=True)
    timestamp: datetime
    sum_age: int

    @pipeline(id=1)
    @inputs(UserAge)
    def create_user_age_aggregated(cls, user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )

    @pipeline(id=2)
    @inputs(UserAgeNonTable)
    def create_user_age_aggregated2(cls, user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Window("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )


@dataset
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    signup_time: datetime = field(timestamp=True)


@featureset
class UserFeature:
    uid: int = feature(id=1)
    country: str = feature(id=2)
    age: float = feature(id=3)
    dob: datetime = feature(id=4)

    @extractor
    @inputs(uid)
    @outputs(age)
    def get_age(cls, ts: pd.Series, uids: pd.Series):
        dobs = User.lookup(ts=ts, uid=uids, fields=["dob"])  # type: ignore
        ages = [dob - datetime.now() for dob in dobs]
        return pd.Series(ages)

    @extractor(depends_on=[User])
    @inputs(uid)
    @outputs(country)
    def get_country(cls, ts: pd.Series, uids: pd.Series):
        countries, _ = User.lookup(  # type: ignore
            ts=ts, uid=uids, fields=["country"]
        )
        return countries


def test_featureset_source_code():
    expected_source_code = """
@featureset
class UserFeature:
    uid: int = feature(id=1)
    country: str = feature(id=2)
    age: float = feature(id=3)
    dob: datetime = feature(id=4)
"""

    assert expected_source_code == get_featureset_core_code(UserFeature)
    expected_source_code = """
@dataset
class UserAgeAggregated:
    city: str = field(key=True)
    timestamp: datetime
    sum_age: int
"""
    assert expected_source_code == get_dataset_core_code(UserAgeAggregated)
