from datetime import datetime

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature
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
def test_simple_create(client):
    client.init_branch("test-branch")
    client.sync(branch="test-branch")


@pytest.mark.integration
@mock
def test_complex_create(client):
    client.init_branch("test-branch-1")
    client.sync(branch="test-branch-1")

    client.init_branch("test-branch-2")
    client.sync(
        branch="test-branch-2",
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    assert len(client.list_datasets(branch="test-branch-1")) == 0
    assert len(client.list_featuresets(branch="test-branch-1")) == 0

    assert len(client.list_datasets(branch="test-branch-2")) == 1
    assert len(client.list_featuresets(branch="test-branch-2")) == 1


@pytest.mark.integration
@mock
def test_log(client):
    client.init_branch("test-branch")
    client.sync(
        branch="test-branch",
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

    output = client.get_dataset_df("UserInfoDataset", branch="test-branch")
    assert output.shape == (1, 7)
