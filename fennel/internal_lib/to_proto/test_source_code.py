from datetime import datetime
from typing import Optional, List

import pandas as pd
import pytest

from fennel.datasets import dataset, field, Dataset, pipeline, Sum
from fennel.dtypes import Continuous
from fennel.featuresets import featureset, extractor
from fennel.internal_lib.to_proto.source_code import (
    get_featureset_core_code,
    get_dataset_core_code,
    lambda_to_python_regular_func,
    remove_decorators,
)
from fennel.lib import includes, meta, inputs, outputs
from fennel.testing import mock


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

    @pipeline
    @inputs(UserAge)
    def create_user_age_aggregated(cls, user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Continuous("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )

    @pipeline
    @inputs(UserAgeNonTable)
    def create_user_age_aggregated2(cls, user_age: Dataset):
        return user_age.groupby("city").aggregate(
            [
                Sum(
                    window=Continuous("1w"),
                    of="age",
                    into_field="sum_age",
                )
            ]
        )


@dataset(index=True)
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    signup_time: datetime = field(timestamp=True)


@featureset
class UserFeature:
    uid: int
    country: str
    age: float
    dob: datetime

    @extractor
    @inputs("uid")
    @outputs("age")
    def get_age(cls, ts: pd.Series, uids: pd.Series):
        dobs = User.lookup(ts=ts, uid=uids, fields=["dob"])  # type: ignore
        # Using ts instead of datetime.now(timezone.utc) to make extract_historical work as of for the extractor
        ages = ts - dobs
        return pd.Series(ages)

    @extractor(deps=[User])  # type: ignore
    @inputs("uid")
    @outputs("country")
    def get_country(cls, ts: pd.Series, uids: pd.Series):
        countries, _ = User.lookup(  # type: ignore
            ts=ts, uid=uids, fields=["country"]
        )
        return countries


@meta(owner="test@test.com")
@dataset(index=True)
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
    userid: int
    age: int
    age_power_four: int
    age_cubed: int
    is_name_common: bool

    @extractor(deps=[UserInfoDataset])  # type: ignore
    @includes(power_4, cube)
    @inputs("userid")
    @outputs("age", "age_power_four", "age_cubed", "is_name_common")
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df[str(cls.userid)] = user_id
        df[str(cls.age_power_four)] = power_4(df["age"])
        df[str(cls.age_cubed)] = cube(df["age"])
        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                "age",
                "age_power_four",
                "age_cubed",
                "is_name_common",
            ]
        ]


def test_source_code_gen():
    expected_source_code = """
@featureset
class UserFeature:
    uid: int
    country: str
    age: float
    dob: datetime
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
    userid: int
    age: int
    age_power_four: int
    age_cubed: int
    is_name_common: bool
"""
    assert expected_source_code == get_featureset_core_code(UserInfoExtractor)


def get_lambda(x):
    return x


# fmt: off
def test_lambda_source_code_gen():
    a1 = lambda x: x + 1  # type: ignore # noqa: E731
    expected_source_code = """lambda x: x + 1"""
    assert expected_source_code == lambda_to_python_regular_func(a1)

    a2 = lambda x, y: x + y  # type: ignore # noqa: E731
    expected_source_code = """lambda x, y: x + y"""
    assert expected_source_code == lambda_to_python_regular_func(a2)

    y1 = get_lambda(lambda x: x * (x + 2) + x + 4)

    y2 = get_lambda(get_lambda(get_lambda(get_lambda(get_lambda(lambda x:
                                                                x * x + 1 + 2 * 3 + 4)))))
    expected_source_code = """lambda x: x * x + 1 + 2 * 3 + 4"""
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


def extract_keys(df: pd.DataFrame, json_col: str = 'json_payload',
                 keys: List[str] = [], values: List[str] = []) -> pd.DataFrame:
    for key in keys:
        df[key] = df[json_col].apply(lambda x: x[key])

    return df.drop(json_col, axis=1)


def extract_location_index(df: pd.DataFrame, index_col: str,
                           latitude_col: str = 'latitude',
                           longitude_col: str = 'longitude',
                           resolution: int = 2) -> str:
    return df.assign({
        f'{index_col}': lambda x: str(x[latitude_col])[
                                  0:3 + resolution] + "-" + str(
            x[longitude_col])[0:3 + resolution],
    })


def fill_empty(df: pd.DataFrame, col: str):
    cols = list(col)
    df = df.fillna(value={col: '' for col in cols})
    return df


def get_lambda2(x1, x2):
    return x1


def test_longer_lambda_source_code_gen():
    schema = {}
    lambda_func = get_lambda2(lambda df: fill_empty(df, 'device'), schema)
    expected_source_code = """lambda df: fill_empty(df, 'device')"""
    assert expected_source_code == lambda_to_python_regular_func(lambda_func)

    lambda_func = get_lambda(
        lambda x: extract_keys(x, json_col='json_payload',
                               keys=['user_id', 'latitude', 'longitude', 'token'],
                               values=["a", "b", "c", "d"]),
    )
    expected_source_code = """lambda x: extract_keys(x, json_col='json_payload', keys=['user_id', 'latitude', 'longitude', 'token'], values=["a", "b", "c", "d"])"""
    assert expected_source_code == lambda_to_python_regular_func(lambda_func)

    # More complex multi-line lambda
    lambda_func = get_lambda(
        get_lambda(lambda x: extract_keys(x, json_col='json_payload',
                                          keys=['user_id', 'latitude', 'longitude', 'token'],
                                          values=["a", "b", "c", "d"])))
    expected_source_code = """lambda x: extract_keys(x, json_col='json_payload', keys=['user_id', 'latitude', 'longitude', 'token'], values=["a", "b", "c", "d"])"""
    assert expected_source_code == lambda_to_python_regular_func(lambda_func)

    # Create a complex lambda with a lambda inside
    lambda_func = get_lambda(
        lambda x: extract_location_index(
            x, index_col='location_index',
            latitude_col='latitude', longitude_col='longitude',
            resolution=lambda x: x + 1
        )
    )
    expected_source_code = """lambda x: extract_location_index( x, index_col='location_index', latitude_col='latitude', longitude_col='longitude', resolution=lambda x: x + 1 )"""
    assert expected_source_code == lambda_to_python_regular_func(lambda_func)


# fmt: on


def test_decorator_removal():
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

    assert (
        expected_1.rstrip()
        == remove_decorators(example_1, ["source", "sink"]).rstrip()
    )

    example_2 = """
@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
"""

    expected_2 = """@meta(owner="test@test.com")

@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
"""

    assert (
        expected_2.strip()
        == remove_decorators(example_2, ["source", "sink"]).rstrip()
    )

    sink_example = """
@meta(owner="test@test.com")
@sink(kafka_sink.topic("stitchfix_demo"), env="prod", cdc="debezium")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
"""

    expected_sink = """@meta(owner="test@test.com")

@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
"""
    assert (
        expected_sink.strip()
        == remove_decorators(sink_example, ["source", "sink"]).rstrip()
    )

    meta_example = """
@meta(owner="test@test.com")
@sink(kafka_sink.topic("stitchfix_demo"), env="prod", cdc="debezium")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
    """

    expected_meta = """
@sink(kafka_sink.topic("stitchfix_demo"), env="prod", cdc="debezium")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    timestamp: datetime
    """
    assert (
        expected_meta.strip()
        == remove_decorators(meta_example, ["meta"]).rstrip()
    )


@mock
def test_invalid_lambda(client):
    @meta(owner="me@fennel.ai")
    @dataset
    class UserAgeFilterFailed:
        name: str = field(key=True)
        age: int
        city: str
        timestamp: datetime

        @pipeline
        @inputs(UserAge)
        def pipeline(cls, event: Dataset):
            return (
                event.filter(
                    lambda x: (x["age"] >= 10)
                    & (x["age"] >= 20)
                    & (x["age"] >= 30)
                )
                .filter(
                    lambda x: (x["age"] >= 10)
                    & (x["age"] >= 10)
                    & (x["age"] >= 10)
                )
                .filter(
                    lambda x: (x["age"] >= 10)
                    & (x["age"] >= 10)
                    & (x["age"] >= 10)
                )
            )

    with pytest.raises(ValueError) as e:
        client.commit(
            datasets=[UserAge, UserAgeFilterFailed], message="first_message"
        )

    assert (
        str(e.value)
        == """Not able to choose lambda function between lambda x: (x["age"] >= 10) and lambda x: (x["age"] >= 10)& (x["age"] >= 10). If lambda contains more than one line, please define the function within the parentheses."""
    )

    @meta(owner="me@fennel.ai")
    @dataset
    class UserAgeFilterPassed:
        name: str = field(key=True)
        age: int
        city: str
        timestamp: datetime

        @pipeline
        @inputs(UserAge)
        def pipeline(cls, event: Dataset):
            return event.filter(
                lambda x: (
                    (x["age"] >= 10)
                    & (x["age"] >= 10)
                    & (x["age"] >= 10)
                    & (x["age"] >= 10)
                    & (x["age"] >= 10)
                    & (x["age"] >= 10)
                )
            )

    client.commit(
        datasets=[UserAge, UserAgeFilterPassed], message="first_message"
    )
