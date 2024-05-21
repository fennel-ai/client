from datetime import datetime, timezone

import pandas as pd

from fennel.connectors import Webhook, source
from fennel.datasets import Count, Dataset, dataset, field, pipeline
from fennel.dtypes import Continuous
from fennel.lib import inputs
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@mock
def test_aggregation_after_aggregation(client):

    @source(webhook.endpoint("A1"), cdc="append", disorder="14d")
    @dataset
    class A1:
        user_id: str
        page_id: str
        event_id: str
        t: datetime

    @dataset(index=True)
    class A2:
        user_id: str = field(key=True)
        count: int
        t: datetime

        @pipeline
        @inputs(A1)
        def pipeline_window(cls, event: Dataset):
            return (
                event.groupby("user_id", "page_id")
                .aggregate(count=Count(window=Continuous("1h")), emit="final")
                .groupby("user_id")
                .aggregate(count=Count(window=Continuous("1h")))
            )

    client.commit(datasets=[A1, A2], message="first_commit")

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 3, 4, 5, 5, 5],
            "page_id": [1, 1, 1, 1, 2, 2, 3, 3, 3, 3],
            "event_id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "t": [now, now, now, now, now, now, now, now, now, now],
        }
    )

    client.log("fennel_webhook", "A1", df)
    client.sleep()

    df, _ = client.lookup(
        A2,
        keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
    )
    assert df["count"].tolist() == [1, 2, 1, 1, 1]
