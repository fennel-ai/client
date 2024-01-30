from datetime import datetime

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.datasets import Dataset, dataset, field, pipeline
from fennel.featuresets import featureset, feature
from fennel.lib.schema import inputs
from fennel.lib.aggregate import Count
from fennel.sources import source, Webhook
from fennel.test_lib import mock

wh = Webhook(name="fennel_webhook")
__owner__ = "fennel <<EMAIL>>"


@source(wh.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: int
    gender: str
    country_code: int
    email: str
    timestamp: datetime = field(timestamp=True)


@dataset
class GenderStats:
    gender: str = field(key=True)
    count: int
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(UserInfoDataset)
    def my_pipeline(cls, user_info: Dataset):
        return user_info.groupby("gender").aggregate(
            Count(window="forever", into_field="count")
        )


@dataset
class CountryStats:
    country_code: int = field(key=True)
    count: int
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(UserInfoDataset)
    def my_pipeline(cls, user_info: Dataset):
        return user_info.groupby("country_code").aggregate(
            Count(window="forever", into_field="count")
        )


@featureset
class UserInfoFeatureset:
    user_id: str = feature(id=1)
    name: str = feature(id=2).extract(field=UserInfoDataset.name, default="None")  # type: ignore
    age: int = feature(id=3).extract(field=UserInfoDataset.age, default=1)  # type: ignore
    gender: str = feature(id=4).extract(field=UserInfoDataset.gender, default="None")  # type: ignore
    country_code: int = feature(id=5).extract(field=UserInfoDataset.country_code, default=1)  # type: ignore
    email: str = feature(id=6).extract(field=UserInfoDataset.email, default="None")  # type: ignore


def _get_changed_datasets(filter_condition):
    @source(wh.endpoint("UserInfoDataset"))
    @dataset
    class UserInfoDataset:
        user_id: int = field(key=True)
        name: str
        age: int
        gender: str
        country_code: int
        email: str
        timestamp: datetime = field(timestamp=True)

    @dataset
    class GenderStats:
        gender: str = field(key=True)
        count: int
        timestamp: datetime = field(timestamp=True)

        @pipeline(version=1)
        @inputs(UserInfoDataset)
        def my_pipeline(cls, user_info: Dataset):
            return (
                user_info.filter(filter_condition)
                .groupby("gender")
                .aggregate(Count(window="forever", into_field="count"))
            )

    return UserInfoDataset, GenderStats


@pytest.mark.integration
@mock
def test_simple_clone(client):
    """
    Cloning should return same datasets and featuresets.
    """
    client.sync(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    client.clone_branch("test-branch", from_branch="main")
    client.switch_branch("test-branch")
    assert len(client.list_datasets()) == 1
    assert len(client.list_featuresets()) == 1


@pytest.mark.integration
@mock
def test_clone_after_log(client):
    """
    Purpose of this test is to make sure, we copy data also when we clone.
    """
    client.sync(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    now = datetime.now()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "male",
            "country_code": 1,
            "email": "john@fennel",
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("UserInfoDataset")
    assert output.shape == (1, 7)
    assert len(client.list_datasets()) == 1
    assert len(client.list_featuresets()) == 1

    client.clone_branch("test-branch", from_branch="main")
    client.switch_branch("test-branch")
    assert len(client.list_datasets()) == 1
    assert len(client.list_featuresets()) == 1
    output = client.get_dataset_df("UserInfoDataset")
    assert output.shape == (1, 7)


@pytest.mark.integration
@mock
def test_webhook_log_to_both_clone_parent(client):
    """
    Testing webhook logging to Dataset A in both the branches
    """
    client.sync(
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch", from_branch="main")

    now = datetime.now()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "male",
            "country_code": 1,
            "email": "john@fennel",
            "timestamp": now,
        },
        {
            "user_id": 2,
            "name": "Rachel",
            "age": 55,
            "gender": "F",
            "country_code": 1,
            "email": "rachel@fennel",
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df(
        "GenderStats",
    )
    assert output.shape == (2, 3)

    client.switch_branch("test-branch")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)


@pytest.mark.integration
@mock
def test_add_dataset_clone_branch(client):
    """
    Cling a branch, then adding one or more datasets in cloned branch. Change should be reflected in cloned branch only.
    """
    client.sync(
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch", from_branch="main")

    client.switch_branch("test-branch")
    client.sync(
        datasets=[UserInfoDataset, GenderStats, CountryStats],
    )

    now = datetime.now()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "male",
            "country_code": 1,
            "email": "john@fennel",
            "timestamp": now,
        },
        {
            "user_id": 2,
            "name": "Rachel",
            "age": 55,
            "gender": "F",
            "country_code": 2,
            "email": "rachel@fennel",
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    output = client.get_dataset_df("CountryStats")
    assert output.shape == (2, 3)

    client.switch_branch("main")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    with pytest.raises(ValueError) as error:
        client.get_dataset_df("CountryStats")
    assert str(error.value) == "Dataset `CountryStats` not found"


@pytest.mark.integration
@mock
def test_change_dataset_clone_branch(client):
    """
    Clone a branch A → B. Verify A & B both give the same answers.
    Then modify A. Ensure B keeps giving the same answers.
    """
    now = datetime.now()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "male",
            "country_code": 1,
            "email": "john@fennel",
            "timestamp": now,
        },
        {
            "user_id": 2,
            "name": "Rachel",
            "age": 55,
            "gender": "F",
            "country_code": 2,
            "email": "rachel@fennel",
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)

    client.sync(
        datasets=[UserInfoDataset, GenderStats],
    )
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    client.clone_branch("test-branch", from_branch="main")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.switch_branch("test-branch")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.sync(
        datasets=_get_changed_datasets(lambda x: x["gender"].isin(["M", "F"])),
    )
    client.switch_branch("main")
    client.sync(
        datasets=[UserInfoDataset, GenderStats],
    )

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.switch_branch("test-branch")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (1, 3)


@pytest.mark.integration
@mock
def test_multiple_clone_branch(client):
    """
    Clone A → B and then again B → C — they are all the same.
    Now modify B and C in different ways - so all three of A, B, C have different graphs/data etc.
    """
    now = datetime.now()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "male",
            "country_code": 1,
            "email": "john@fennel",
            "timestamp": now,
        },
        {
            "user_id": 2,
            "name": "Rachel",
            "age": 55,
            "gender": "F",
            "country_code": 2,
            "email": "rachel@fennel",
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)

    client.sync(
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch-1", from_branch="main")
    client.clone_branch("test-branch-2", from_branch="test-branch-1")

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.switch_branch("test-branch-1")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.switch_branch("test-branch-2")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.sync(
        datasets=_get_changed_datasets(lambda x: x["gender"].isin(["M", "F"])),
    )
    client.switch_branch("test-branch-1")

    client.sync(
        datasets=_get_changed_datasets(lambda x: x["gender"].isin(["m", "f"])),
    )

    client.switch_branch("main")
    client.sync(
        datasets=[UserInfoDataset, GenderStats],
    )

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.switch_branch("test-branch-1")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (0, 3)

    client.switch_branch("test-branch-2")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (1, 3)
