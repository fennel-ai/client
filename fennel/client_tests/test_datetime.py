from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import Webhook, source
from fennel.datasets import dataset, Dataset, field, pipeline
from fennel.lib import inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "eng@fennel.ai"


def function(df: pd.DataFrame) -> pd.Series:
    try:
        return (
            df["updated_at"] - pd.Timestamp("1970-01-01", tz="UTC")
        ).dt.total_seconds()
    except Exception as e:
        raise Exception(
            f'Column Type : {df["updated_at"].dtype} and error : {e}'
        )


@source(webhook.endpoint("Transactions"), disorder="14d", cdc="append")
@dataset
class Events:
    id: int
    created_at: datetime = field(timestamp=True)
    updated_at: datetime
    user_id: int


@dataset(index=True)
class EventsTransformed:
    id: int
    created_at: datetime = field(timestamp=True)
    updated_at: datetime
    user_id: int = field(key=True)
    difference: float

    @pipeline
    @inputs(Events)
    def pipeline(cls, event: Dataset):
        return (
            event.assign(
                "difference",
                float,
                function,
            )
            .groupby("user_id")
            .latest()
        )


@pytest.mark.integration
@mock
def test_datetime_transform(client):
    response = client.commit(
        message="msg",
        datasets=[Events, EventsTransformed],
        featuresets=[],
    )
    assert response.status_code == requests.codes.OK, response.json()

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 1],
            "id": [1, 2, 3, 4, 5],
            "created_at": [now, now, now, now, now],
            "updated_at": [now, now, now, now, now],
        }
    )
    response = client.log("fennel_webhook", "Transactions", df)
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep()
    # Querying UserInfoFeatures
    result, _ = client.lookup(
        dataset=EventsTransformed,
        keys=pd.DataFrame(
            {"user_id": [1]},
        ),
    )

    assert result.shape == (1, 5)
