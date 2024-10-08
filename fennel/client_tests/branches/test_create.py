from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature as F
from fennel.testing import mock

wh = Webhook(name="fennel_webhook")
__owner__ = "nitin@fennel.com"


@source(wh.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
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
    user_id: str
    name: str = F(UserInfoDataset.name, default="None")  # type: ignore
    age: int = F(UserInfoDataset.age, default=1)  # type: ignore
    gender: str = F(UserInfoDataset.gender, default="None")  # type: ignore
    country_code: int = F(UserInfoDataset.country_code, default=1)  # type: ignore
    email: str = F(UserInfoDataset.email, default="None")  # type: ignore


@pytest.mark.integration
@mock
def test_simple_create(client):
    client.init_branch("test-branch")
    assert client.branch() == "test-branch"
    client.commit(message="msg")


@pytest.mark.integration
@mock
def test_simple_create_with_checkout(client):
    branch_list = client.list_branches()
    assert len(branch_list) == 1

    client.checkout("checkout-test")
    branch_list = client.list_branches()
    assert len(branch_list) == 1

    client.checkout("checkout-test", init=True)
    assert client.branch() == "checkout-test"
    branch_list = client.list_branches()
    assert len(branch_list) == 2


@pytest.mark.integration
@mock
def test_duplicate_create(client):
    """
    Creating a branch with the name that already exists throws 400
    """
    response = client.init_branch("test-branch")
    assert response.status_code == 200

    with pytest.raises(Exception) as error:
        client.init_branch("test-branch")
    if client.is_integration_client():
        assert (
            str(error.value)
            == "Server returned: 500, can not create branch `test-branch`: already exists"
        )
    else:
        assert str(error.value) == "Branch name: `test-branch` already exists"


@pytest.mark.integration
@mock
def test_complex_create(client):
    """
    Syncing zero datasets in main and few in a branch. The change should be reflected in that branch only.
    """
    client.commit(message="msg")
    client.init_branch("test-branch")
    client.commit(
        message="msg",
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    _, found = client.lookup(
        "UserInfoDataset",
        keys=pd.DataFrame({"user_id": [1, 2]}),
        fields=["age"],
    )
    assert found.to_list() == [False, False]

    client.checkout("main")
    with pytest.raises(Exception) as error:
        _ = client.inspect("UserInfoDataset")
    if client.is_integration_client():
        assert (
            str(error.value)
            == 'Server returned: 404, dataset "UserInfoDataset" not found'
        )
    else:
        assert str(error.value) == "Dataset `UserInfoDataset` not found"


@pytest.mark.integration
@mock
def test_log(client):
    resp = client.init_branch("test-branch")
    assert resp.status_code == 200, resp.json()

    resp = client.commit(
        message="msg",
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )
    assert resp.status_code == 200, resp.json()

    now = datetime.now(timezone.utc)
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

    assert client.branch() == "test-branch"

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    client.sleep()

    _, found = client.lookup(
        "UserInfoDataset",
        keys=pd.DataFrame({"user_id": [1, 2]}),
        fields=["age"],
    )
    assert found.to_list() == [True, False]
