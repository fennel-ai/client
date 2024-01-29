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


@pytest.mark.integration
@mock
def test_simple_clone(client):
    client.init_branch("test-branch-1")
    client.sync(
        branch="test-branch-1",
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    client.clone_branch("test-branch-2", from_branch="test-branch-1")
    assert len(client.list_datasets(branch="test-branch-2")) == 1
    assert len(client.list_featuresets(branch="test-branch-2")) == 1


@pytest.mark.integration
@mock
def test_clone_after_log(client):
    """
    Purpose of this test is to make sure, we don't copy data when we clone just the definitions.
    """
    client.init_branch("test-branch-1")
    client.sync(
        branch="test-branch-1",
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
    response = client.log(
        "fennel_webhook", "UserInfoDataset", df, branch="test-branch"
    )
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("UserInfoDataset", branch="test-branch-1")
    assert output.shape == (1, 7)
    assert len(client.list_datasets(branch="test-branch-1")) == 1
    assert len(client.list_featuresets(branch="test-branch-1")) == 1

    client.clone_branch("test-branch-2", from_branch="test-branch-1")
    assert len(client.list_datasets(branch="test-branch-2")) == 1
    assert len(client.list_featuresets(branch="test-branch-2")) == 1
    output = client.get_dataset_df("UserInfoDataset", branch="test-branch-2")
    assert output.shape == (0, 7)


@pytest.mark.integration
@mock
def test_webhook_log_to_both_clone_parent(client):
    client.init_branch("test-branch-1")
    client.sync(
        branch="test-branch-1",
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch-2", from_branch="test-branch-1")

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

    output = client.get_dataset_df("GenderStats", branch="test-branch-1")
    assert output.shape == (2, 3)

    output = client.get_dataset_df("GenderStats", branch="test-branch-2")
    assert output.shape == (2, 3)


@pytest.mark.integration
@mock
def test_add_dataset_clone_branch(client):

    client.init_branch("test-branch-1")
    client.sync(
        branch="test-branch-1",
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch-2", from_branch="test-branch-1")

    client.sync(
        branch="test-branch-2",
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

    output = client.get_dataset_df("GenderStats", branch="test-branch-1")
    assert output.shape == (2, 3)

    with pytest.raises(ValueError) as error:
        client.get_dataset_df("CountryStats", branch="test-branch-1")
    assert str(error.value) == "Dataset `CountryStats` not found"

    output = client.get_dataset_df("GenderStats", branch="test-branch-2")
    assert output.shape == (2, 3)

    output = client.get_dataset_df("CountryStats", branch="test-branch-2")
    assert output.shape == (2, 3)
