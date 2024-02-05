from datetime import datetime

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.datasets import Dataset, dataset, field, pipeline
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.schema import inputs, outputs
from fennel.lib.aggregate import Count
from fennel.sources import source, Webhook
from fennel.test_lib import mock

wh = Webhook(name="fennel_webhook")
__owner__ = "nitin@fennel.com"


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


def _get_changed_dataset(filter_condition):
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

    return GenderStats


def _get_source_changed_datasets():
    @source(wh.endpoint("UserInfoDataset2"))
    @dataset
    class UserInfoDataset:
        user_id: int = field(key=True)
        gender: int
        timestamp: datetime = field(timestamp=True)

    @dataset
    class GenderStats:
        gender: int = field(key=True)
        count: int
        timestamp: datetime = field(timestamp=True)

        @pipeline(version=1)
        @inputs(UserInfoDataset)
        def my_pipeline(cls, user_info: Dataset):
            return user_info.groupby("gender").aggregate(
                Count(window="forever", into_field="count")
            )

    return UserInfoDataset, GenderStats


def _get_changed_featureset():
    @featureset
    class UserInfoFeatureset:
        user_id: str = feature(id=1)
        name: str = feature(id=2).extract(field=UserInfoDataset.name, default="None")  # type: ignore
        age: int = feature(id=3)
        gender: str = feature(id=4).extract(field=UserInfoDataset.gender, default="None")  # type: ignore
        country_code: int = feature(id=5)
        email: str = feature(id=6).extract(field=UserInfoDataset.email, default="None")  # type: ignore

        @extractor(depends_on=[UserInfoDataset])
        @inputs(user_id)
        @outputs(age, country_code)
        def my_extractor(cls, ts: pd.Series, user_id: pd.Series):
            df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
            df["age"] = df["age"].fillna(1) * 10
            df["country_code"] = df["country_code"].fillna(100)
            return df[["age", "country_code"]]

    return UserInfoFeatureset


@pytest.mark.integration
@mock
def test_simple_clone(client):
    """
    Cloning should return same datasets and featuresets.
    """
    client.commit(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    client.clone_branch("test-branch", from_branch="main")
    assert client.get_branch() == "test-branch"

    test_branch_datasets = client.get_datasets()
    test_branch_featuresets = client.get_featuresets()

    client.checkout("main")

    for x, y in zip(client.get_datasets(), test_branch_datasets):
        assert x._name == y._name
    for x, y in zip(client.get_featuresets(), test_branch_featuresets):
        assert x._name == y._name


@pytest.mark.integration
@mock
def test_clone_after_log(client):
    """
    Purpose of this test is to make sure, we copy data also when we clone.
    """
    client.commit(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )

    now = datetime.utcnow()
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

    client.clone_branch("test-branch", from_branch="main")
    assert client.get_branch() == "test-branch"
    output = client.get_dataset_df("UserInfoDataset")
    assert output.shape == (1, 7)


@pytest.mark.integration
@mock
def test_webhook_log_to_both_clone_parent(client):
    """
    Testing webhook logging to Dataset A in both the branches
    """
    client.commit(
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

    assert client.get_branch() == "test-branch"
    output = client.get_dataset_df(
        "GenderStats",
    )
    assert output.shape == (2, 3)

    client.checkout("main")
    assert client.get_branch() == "main"
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)


@pytest.mark.integration
@mock
def test_add_dataset_clone_branch(client):
    """
    Clone a branch, then adding one or more datasets in cloned branch. Change should be reflected in cloned branch only.
    """
    client.commit(
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch", from_branch="main")

    client.commit(
        datasets=[UserInfoDataset, GenderStats, CountryStats],
    )

    now = datetime.utcnow()
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

    client.checkout("main")
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
    now = datetime.utcnow()
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

    client.commit(
        datasets=[UserInfoDataset, GenderStats],
    )
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.clone_branch("test-branch", from_branch="main")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.commit(
        datasets=[
            UserInfoDataset,
            _get_changed_dataset(lambda x: x["gender"].isin(["M", "F"])),
        ]
    )
    client.checkout("main")
    client.commit(
        datasets=[UserInfoDataset, GenderStats],
    )

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.checkout("test-branch")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (1, 3)


@pytest.mark.integration
@mock
def test_multiple_clone_branch(client):
    """
    Clone A → B and then again B → C — they are all the same.
    Now modify B and C in different ways - so all three of A, B, C have different graphs/data etc.
    """
    now = datetime.utcnow()
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

    client.commit(
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch-1", from_branch="main")
    client.clone_branch("test-branch-2", from_branch="test-branch-1")

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.checkout("test-branch-1")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.checkout("main")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.commit(
        datasets=[UserInfoDataset, GenderStats],
    )

    client.checkout("test-branch-2")
    client.commit(
        datasets=[
            UserInfoDataset,
            _get_changed_dataset(lambda x: x["gender"].isin(["M", "F"])),
        ]
    )

    client.checkout("test-branch-1")
    client.commit(
        datasets=[
            UserInfoDataset,
            _get_changed_dataset(lambda x: x["gender"].isin(["m", "f"])),
        ]
    )

    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (0, 3)

    client.checkout("main")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)

    client.checkout("test-branch-2")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (1, 3)


@pytest.mark.integration
@mock
def test_change_source_dataset_clone_branch(client):
    """
    We have branch A, with src -> pipeline. Clone A -> B.
    In B we modify the source dataset itself. ( different webhook ) now the derived pipelines give different answers.
    """
    client.commit(
        datasets=[UserInfoDataset, GenderStats],
    )
    client.clone_branch("test-branch", from_branch="main")

    client.commit(
        datasets=_get_source_changed_datasets(),
    )

    now = datetime.utcnow()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "M",
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

    now = datetime.utcnow()
    data = [
        {
            "user_id": 1,
            "gender": 0,
            "timestamp": now,
        },
        {
            "user_id": 2,
            "gender": 1,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "UserInfoDataset2", df)
    assert response.status_code == requests.codes.OK, response.json()

    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)
    assert output["gender"].tolist() == [0, 1]
    assert output["count"].tolist() == [1, 1]

    client.checkout("main")
    output = client.get_dataset_df("GenderStats")
    assert output.shape == (2, 3)
    assert output["gender"].tolist() == ["M", "F"]
    assert output["count"].tolist() == [1, 1]


@pytest.mark.integration
@mock
def test_change_extractor_clone_branch(client):
    """
    We have branch A, with src -> pipeline -> extractor. Clone A -> B.
    In B we modify extractor. Now the extract give different answers.
    """
    client.commit(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatureset],
    )
    client.clone_branch("test-branch", from_branch="main")

    client.commit(
        datasets=[UserInfoDataset],
        featuresets=[_get_changed_featureset()],
    )

    now = datetime.utcnow()
    data = [
        {
            "user_id": 1,
            "name": "John",
            "age": 30,
            "gender": "M",
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

    output = client.query(
        inputs=["UserInfoFeatureset.user_id"],
        outputs=[UserInfoFeatureset],
        input_dataframe=pd.DataFrame({"UserInfoFeatureset.user_id": [1, 2, 3]}),
    )
    assert output.shape == (3, 6)
    assert output["UserInfoFeatureset.user_id"].tolist() == [1, 2, 3]
    assert output["UserInfoFeatureset.name"].tolist() == [
        "John",
        "Rachel",
        "None",
    ]
    assert output["UserInfoFeatureset.age"].tolist() == [300, 550, 10]
    assert output["UserInfoFeatureset.gender"].tolist() == ["M", "F", "None"]
    assert output["UserInfoFeatureset.country_code"].tolist() == [1, 2, 100]
    assert output["UserInfoFeatureset.email"].tolist() == [
        "john@fennel",
        "rachel@fennel",
        "None",
    ]

    client.checkout("main")
    output = client.query(
        inputs=["UserInfoFeatureset.user_id"],
        outputs=[UserInfoFeatureset],
        input_dataframe=pd.DataFrame({"UserInfoFeatureset.user_id": [1, 2, 3]}),
    )
    assert output.shape == (3, 6)
    assert output["UserInfoFeatureset.user_id"].tolist() == [1, 2, 3]
    assert output["UserInfoFeatureset.name"].tolist() == [
        "John",
        "Rachel",
        "None",
    ]
    assert output["UserInfoFeatureset.age"].tolist() == [30, 55, 1]
    assert output["UserInfoFeatureset.gender"].tolist() == ["M", "F", "None"]
    assert output["UserInfoFeatureset.country_code"].tolist() == [1, 2, 1]
    assert output["UserInfoFeatureset.email"].tolist() == [
        "john@fennel",
        "rachel@fennel",
        "None",
    ]
