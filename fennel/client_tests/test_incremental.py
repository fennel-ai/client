from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel.connectors import Webhook, source
from fennel.datasets import Count, Dataset, dataset, field, pipeline
from fennel.dtypes import Continuous
from fennel.featuresets import featureset, extractor
from fennel.lib import inputs, outputs
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


def transform_age(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.assign(age=lambda x: x["age"] * 2)
    return dataframe


def first_commit(client):
    @source(
        webhook.endpoint("UserInfo"),
        disorder="14d",
        cdc="append",
    )
    @dataset
    class UserInfo:
        user_id: int
        age: int
        created_at: datetime

    @dataset(index=True)
    class AgeStats:
        age: int = field(key=True)
        count: int
        created_at: datetime

        @pipeline
        @inputs(UserInfo)
        def pipeline(cls, event: Dataset):
            return event.groupby("age").aggregate(
                count=Count(of="user_id", window=Continuous("forever"))
            )

    @featureset
    class AgeStatsFeatures:
        age: int
        count: int

        @extractor(deps=[AgeStats], version=1)
        @inputs("age")
        @outputs("count")
        def my_extractor(cls, ts: pd.Series, ages: pd.Series):
            res, _ = AgeStats.lookup(ts, age=ages)  # type: ignore
            return res["count"].fillna(-1)

    client.commit(
        datasets=[UserInfo, AgeStats],
        featuresets=[AgeStatsFeatures],
        message="first_commit",
    )
    return True


def second_commit(client):
    @source(
        webhook.endpoint("UserInfo"),
        disorder="14d",
        cdc="upsert",
    )
    @dataset(version=2, index=True)
    class UserInfo:
        user_id: int = field(key=True)
        email: str
        age: int
        created_at: datetime

    @dataset(version=2, index=True)
    class AgeStats:
        age: int = field(key=True)
        count: int
        created_at: datetime

        @pipeline
        @inputs(UserInfo)
        def pipeline(cls, event: Dataset):
            return (
                event.transform(transform_age)
                .groupby("age")
                .aggregate(
                    count=Count(of="user_id", window=Continuous("forever"))
                )
            )

    @featureset
    class AgeStatsFeatures:
        age: int
        count: int

        @extractor(deps=[AgeStats], version=2)
        @inputs("age")
        @outputs("count")
        def my_extractor(cls, ts: pd.Series, ages: pd.Series):
            res, _ = AgeStats.lookup(ts, age=ages)  # type: ignore
            return res["count"].fillna(0)

    client.commit(
        datasets=[UserInfo, AgeStats],
        featuresets=[AgeStatsFeatures],
        message="second_commit",
        incremental=True,
    )
    return True


@pytest.mark.integration
@mock
def test_incremental(client):
    now = datetime.now(timezone.utc)
    assert first_commit(client=client) is True

    # Logging to dataset
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "age": [10, 10, 15, 15],
            "created_at": [now, now, now, now],
        }
    )
    client.log("fennel_webhook", "UserInfo", df)
    client.sleep()

    # Lookup
    output = client.query(
        inputs=["AgeStatsFeatures.age"],
        outputs=["AgeStatsFeatures.count"],
        input_dataframe=pd.DataFrame(
            {"AgeStatsFeatures.age": [10, 15, 20, 30]}
        ),
    )
    assert output["AgeStatsFeatures.count"].tolist() == [2, 2, -1, -1]

    assert second_commit(client=client) is True

    # Logging to dataset
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4],
            "age": [10, 10, 15, 15],
            "email": ["<EMAIL>", "<EMAIL>", "<EMAIL>", "<EMAIL>"],
            "created_at": [now, now, now, now],
        }
    )
    client.log("fennel_webhook", "UserInfo", df)
    client.sleep()

    # Lookup
    output = client.query(
        inputs=["AgeStatsFeatures.age"],
        outputs=["AgeStatsFeatures.count"],
        input_dataframe=pd.DataFrame(
            {"AgeStatsFeatures.age": [10, 15, 20, 30]}
        ),
    )
    assert output["AgeStatsFeatures.count"].tolist() == [0, 0, 2, 2]


@pytest.mark.integration
@mock
def test_dataset_version_change(client):

    @source(
        webhook.endpoint("Dataset1"),
        disorder="14d",
        cdc="upsert",
    )
    @dataset
    class Dataset1:
        user_id: int = field(key=True)
        email: str
        created_at: datetime

    # Try syncing the same dataset through incremental should work.
    client.commit(
        datasets=[Dataset1], incremental=True, message="second_commit"
    )

    # Try changing the dataset without increasing the version should fail
    @source(
        webhook.endpoint("Dataset1"),
        disorder="14d",
        cdc="upsert",
    )
    @dataset
    class Dataset1:
        user_id: int = field(key=True)
        email: str
        gender: str
        created_at: datetime

    if client.is_integration_client():
        with pytest.raises(Exception) as e:
            client.commit(
                datasets=[Dataset1], incremental=True, message="second_commit"
            )
        assert "please increment dataset version" in str(e.value).lower()
    else:
        with pytest.raises(ValueError) as e:
            client.commit(
                datasets=[Dataset1], incremental=True, message="second_commit"
            )
        assert str(e.value) == "Please update version of dataset: `Dataset1`"

    # Try changing the dataset with increasing the version should pass
    @source(
        webhook.endpoint("Dataset1"),
        disorder="14d",
        cdc="upsert",
    )
    @dataset(version=2)
    class Dataset1:
        user_id: int = field(key=True)
        email: str
        gender: str
        created_at: datetime

    client.commit(
        datasets=[Dataset1], incremental=True, message="second_commit"
    )
