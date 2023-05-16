from datetime import datetime

import pandas as pd
from typing import Optional

from fennel.datasets import dataset, field, Dataset, pipeline
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.aggregate import Sum
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.lib.to_proto.source_code import (
    get_featureset_core_code,
    get_dataset_core_code,
    lambda_to_python_regular_func,
    remove_source_decorator,
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

    @pipeline(version=1, active=True)
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

    @pipeline(version=2)
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


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def square(x: int) -> int:
    return x**2


def cube(x: int) -> int:
    return x**3


@includes(square)
def power_4(x: int) -> int:
    return square(square(x))


@featureset
class UserInfoExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4)
    age_power_four: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor(depends_on=[UserInfoDataset])
    @includes(power_4, cube)
    @inputs(userid)
    @outputs(age, age_power_four, age_cubed, is_name_common)
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df[str(cls.userid)] = user_id
        df[str(cls.age_power_four)] = power_4(df["age"])
        df[str(cls.age_cubed)] = cube(df["age"])
        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                str(cls.age),
                str(cls.age_power_four),
                str(cls.age_cubed),
                str(cls.is_name_common),
            ]
        ]


def test_source_code_gen():
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

    expected_source_code = """
@featureset
class UserInfoExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4)
    age_power_four: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)
"""
    assert expected_source_code == get_featureset_core_code(UserInfoExtractor)


# fmt: off
def test_lambda_source_code_gen():
    a1 = lambda x: x + 1  # type: ignore # noqa: E731
    expected_source_code = """lambda x: x + 1  # type: ignore # noqa: E731"""
    assert expected_source_code == lambda_to_python_regular_func(a1)

    a2 = lambda x, y: x + \
                      y  # type: ignore # noqa: E731
    expected_source_code = """lambda x, y: x + y  # type: ignore # noqa: E731"""
    assert expected_source_code == lambda_to_python_regular_func(a2)

    def get_lambda(x):
        return x

    y1 = get_lambda(lambda x: x * (x + 2) + x + 4)

    y2 = get_lambda(get_lambda(get_lambda(get_lambda(get_lambda(lambda x:
    x * x + 1 + 2 * 3 + 4)))))
    expected_source_code = """lambda x:x * x + 1 + 2 * 3 + 4"""
    assert expected_source_code == lambda_to_python_regular_func(y2)

    y3 = get_lambda(get_lambda(get_lambda(get_lambda(get_lambda(get_lambda(
        lambda x: x * x + 1 + 2 * 3 + 4))))))
    expected_source_code = """lambda x: x * x + 1 + 2 * 3 + 4"""
    assert expected_source_code == lambda_to_python_regular_func(y3)

    expected_source_code = """lambda x: x * (x + 2) + x + 4"""
    assert expected_source_code == lambda_to_python_regular_func(y1)

    def get_lambda_tuple(x, y, ret_first=True):
        if ret_first:
            return x
        else:
            return y

    z1 = get_lambda_tuple(lambda x, y: x * x, lambda x, y: x * y)
    expected_source_code = """lambda x, y: x * x"""
    assert expected_source_code == lambda_to_python_regular_func(z1)

    z2, _ = get_lambda(
        lambda x: {"a": x, "b": x + 1, "c": len(x)}), 5  # type: ignore
    expected_source_code = """lambda x: {"a": x, "b": x + 1, "c": len(x)}"""
    assert expected_source_code == lambda_to_python_regular_func(z2)

    z3 = get_lambda(lambda x: {"a": x, "b": x + 1, "c": len(x)})
    assert expected_source_code == lambda_to_python_regular_func(z3)


# fmt: on


def test_source_code_removal():
    example_1 = """
@source(
    s3.bucket(bucket_name="fennel-demo-data", prefix="outbrain_1682921988000/documents_meta.csv"),
    every="1d",
)
@meta(owner="xiao@fennel.ai")
@dataset
class DocumentsMeta:
    document_id: int = field(key=True)
    source_id: int
    publisher_id: int
    timestamp: datetime
"""

    expected_1 = """@meta(owner="xiao@fennel.ai")
@dataset
class DocumentsMeta:
    document_id: int = field(key=True)
    source_id: int
    publisher_id: int
    timestamp: datetime
"""

    assert expected_1.rstrip() == remove_source_decorator(example_1).rstrip()

    example_2 = """
@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
"""

    expected_2 = """
@meta(owner="test@test.com")

@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
"""

    assert expected_2.strip() == remove_source_decorator(example_2).strip()
