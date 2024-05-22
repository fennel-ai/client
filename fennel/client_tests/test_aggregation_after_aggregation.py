from datetime import datetime, timezone, date, timedelta

import pandas as pd

from fennel.connectors import Webhook, source
from fennel.datasets import (
    Average,
    Count,
    Dataset,
    dataset,
    field,
    pipeline,
    Sum,
)
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


@mock
def test_transactions_daily_stats(client):

    @source(webhook.endpoint("Transactions"), cdc="append", disorder="14d")
    @dataset
    class Transactions:
        user_id: str
        amount: float
        t: datetime

    @dataset(index=True)
    class Stats:
        user_id: str = field(key=True)
        daily_sum_avg: float
        daily_sum_avg_1d: float
        daily_sum_avg_2d: float
        daily_sum_avg_3d: float
        daily_count_avg: float
        daily_count_avg_1d: float
        daily_count_avg_2d: float
        daily_count_avg_3d: float
        t: datetime

        @pipeline
        @inputs(Transactions)
        def pipeline(cls, event: Dataset):
            return (
                event.assign("txn_date", date, lambda x: x["t"].dt.date)
                .groupby("user_id", "txn_date")
                .aggregate(
                    count=Count(window=Continuous("forever")),
                    sum=Sum(window=Continuous("forever"), of="amount"),
                    emit="final",
                )
                .groupby("user_id")
                .aggregate(
                    daily_sum_avg=Average(
                        window=Continuous("forever"), of="sum"
                    ),
                    daily_sum_avg_1d=Average(window=Continuous("1d"), of="sum"),
                    daily_sum_avg_2d=Average(window=Continuous("2d"), of="sum"),
                    daily_sum_avg_3d=Average(window=Continuous("3d"), of="sum"),
                    daily_count_avg=Average(
                        window=Continuous("forever"), of="count"
                    ),
                    daily_count_avg_1d=Average(
                        window=Continuous("1d"), of="count"
                    ),
                    daily_count_avg_2d=Average(
                        window=Continuous("2d"), of="count"
                    ),
                    daily_count_avg_3d=Average(
                        window=Continuous("3d"), of="count"
                    ),
                )
            )

    client.commit(datasets=[Transactions, Stats], message="first_commit")

    now = datetime.now(timezone.utc)
    now_1d = datetime.now(timezone.utc) - timedelta(days=1)
    now_2d = datetime.now(timezone.utc) - timedelta(days=2)
    now_3d = datetime.now(timezone.utc) - timedelta(days=3)

    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 1, 2, 2, 2, 3, 3],
            "amount": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            "t": [
                now,
                now_1d,
                now_1d,
                now_2d,
                now_1d,
                now_1d,
                now_2d,
                now_3d,
                now_3d,
                now_3d,
            ],
        }
    )

    client.log("fennel_webhook", "Transactions", df)
    client.sleep()

    df, _ = client.lookup(
        Stats,
        keys=pd.DataFrame({"user_id": [1, 2, 3]}),
    )
    assert df["daily_sum_avg"].tolist() == [500, 700, 1900]
    assert df["daily_sum_avg_1d"].tolist() == [550, 600, 0]
    assert df["daily_sum_avg_2d"].tolist() == [500, 650, 0]
    assert df["daily_sum_avg_3d"].tolist() == [500, 700, 1900]
    assert df["daily_count_avg"].tolist() == [1.6666666666666667, 1, 2]
    assert df["daily_count_avg_1d"].tolist() == [2, 1, 0]
    assert df["daily_count_avg_2d"].tolist() == [1.6666666666666667, 1, 0]
    assert df["daily_count_avg_3d"].tolist() == [1.6666666666666667, 1, 2]
