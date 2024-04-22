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
class Request:
    user_id: int


@featureset
class UserInfoFeatureset:
    user_id: int = F(Request.user_id)
    name: str = F(UserInfoDataset.name, default="None")
    age: int = F(UserInfoDataset.age, default=0)
    gender: str = F(UserInfoDataset.gender, default="None")
    country_code: int = F(UserInfoDataset.country_code, default=0)
    email: str = F(UserInfoDataset.email, default="None")


@pytest.mark.integration
@mock
def test_delete_branch(client):
    resp = client.commit(message="msg", datasets=[UserInfoDataset])
    assert resp.status_code == requests.codes.OK

    # can not delete the main branch
    assert client.list_branches() == ["main"]
    with pytest.raises(Exception) as e:
        client.delete_branch("main")
    assert str(e.value) == "Cannot delete the main branch."

    resp = client.clone_branch("test-branch", "main")
    assert resp.status_code == requests.codes.OK
    assert len(client.list_branches()) == 2

    resp = client.delete_branch("test-branch")
    assert resp.status_code == requests.codes.OK

    assert len(client.list_branches()) == 1
    # after deleting the branch, the client should be on the main branch
    assert client.branch() == "main"


@pytest.mark.integration
@mock
def test_complex_delete(client):
    """
    Clone B from A, test extract working from both, then delete B, test extract working only from A.
    """
    client.commit(
        message="msg",
        datasets=[UserInfoDataset],
        featuresets=[Request, UserInfoFeatureset],
    )
    client.clone_branch(name="test-branch", from_branch="main")

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
        {
            "user_id": 2,
            "name": "Rachel",
            "age": 30,
            "gender": "female",
            "country_code": 1,
            "email": "rachel@fennel",
            "timestamp": now,
        },
        {
            "user_id": 3,
            "name": "Joey",
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
    client.sleep()

    output = client.query(
        inputs=["UserInfoFeatureset.user_id"],
        outputs=[UserInfoFeatureset],
        input_dataframe=pd.DataFrame({"UserInfoFeatureset.user_id": [1, 4]}),
    )
    assert output.shape == (2, 6)
    assert output["UserInfoFeatureset.user_id"].tolist() == [1, 4]
    assert output["UserInfoFeatureset.name"].tolist() == ["John", "None"]
    assert output["UserInfoFeatureset.age"].tolist() == [30, 0]
    assert output["UserInfoFeatureset.gender"].tolist() == ["male", "None"]
    assert output["UserInfoFeatureset.country_code"].tolist() == [1, 0]
    assert output["UserInfoFeatureset.email"].tolist() == [
        "john@fennel",
        "None",
    ]

    client.checkout(name="test-branch")
    output = client.query(
        inputs=["Request.user_id"],
        outputs=[UserInfoFeatureset],
        input_dataframe=pd.DataFrame({"Request.user_id": [1, 4]}),
    )
    assert output.shape == (2, 6)
    assert output["UserInfoFeatureset.user_id"].tolist() == [1, 4]
    assert output["UserInfoFeatureset.name"].tolist() == ["John", "None"]
    assert output["UserInfoFeatureset.age"].tolist() == [30, 0]
    assert output["UserInfoFeatureset.gender"].tolist() == ["male", "None"]
    assert output["UserInfoFeatureset.country_code"].tolist() == [1, 0]
    assert output["UserInfoFeatureset.email"].tolist() == [
        "john@fennel",
        "None",
    ]

    resp = client.delete_branch("test-branch")
    assert resp.status_code == requests.codes.OK
    # have to checkout again because delete checks out to main
    client.checkout("test-branch")

    client.sleep()

    with pytest.raises(Exception) as error:
        client.query(
            inputs=["Request.user_id"],
            outputs=[UserInfoFeatureset],
            input_dataframe=pd.DataFrame({"Request.user_id": [1, 4]}),
        )

    if client.is_integration_client():
        assert (
            str(error.value)
            == "Server returned: 500, branch `test-branch` does not exist"
        )
    else:
        assert (
            str(error.value)
            == "\"Branch: `test-branch` not found, please sync this branch and try again. Available branches: ['main']\""
        )
