from datetime import datetime, timezone, timedelta

import pandas as pd
import pytest

from fennel.connectors import Webhook, source
from fennel.datasets import (
    Average,
    Count,
    Dataset,
    dataset,
    field,
    Max,
    pipeline,
    Sum,
)
from fennel.dtypes import Tumbling, Hopping, Window, Continuous
from fennel.lib import inputs, bucketize
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("Transactions"), cdc="append", disorder="14d")
@dataset
class Transactions:
    id: str
    user_id: str
    amount: float
    t: datetime


@mock
def test_discrete_hopping_tumbling_window_aggregation(client):

    @dataset(index=True)
    class Stats:
        user_id: str = field(key=True)
        sum: float
        count: int
        t: datetime

        @pipeline
        @inputs(Transactions)
        def pipeline(cls, event: Dataset):
            return (
                event.dedup("id")
                .groupby("user_id")
                .aggregate(
                    count=Count(window=Tumbling("1d")),
                    sum=Sum(window=Tumbling("1d"), of="amount"),
                )
            )

    client.commit(datasets=[Transactions, Stats], message="first_commit")

    now = datetime.now(timezone.utc)
    now_1d = datetime.now(timezone.utc) - timedelta(days=1)
    now_2d = datetime.now(timezone.utc) - timedelta(days=2)
    now_3d = datetime.now(timezone.utc) - timedelta(days=3)

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
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
    assert df["count"].tolist() == [3, 1, 0]


@mock
def test_discrete_hopping_window_aggregation(client):

    data = pd.read_csv("fennel/client_tests/data/app_events.csv").assign(
        timestamp=lambda x: pd.to_datetime(x["timestamp"], utc=True)
    )

    @source(webhook.endpoint("AppEvents"), cdc="append", disorder="14d")
    @dataset
    class AppEvents:
        user_id: int
        page_id: int
        timestamp: datetime

    @dataset(index=True)
    class EventCount:
        user_id: int = field(key=True)
        count: int
        timestamp: datetime

        @pipeline
        @inputs(AppEvents)
        def pipeline(cls, event: Dataset):
            return event.groupby("user_id").aggregate(
                count=Count(window=Hopping("1d", "1h")),
            )

    client.commit(datasets=[AppEvents, EventCount], message="first_commit")
    client.log("fennel_webhook", "AppEvents", data)

    # Test online lookups
    df, _ = client.lookup(
        EventCount,
        keys=pd.DataFrame({"user_id": [1, 2, 3]}),
    )
    assert df["count"].tolist() == [0, 3, 2]

    # Test offline lookups
    df, _ = client.lookup(
        EventCount,
        keys=pd.DataFrame({"user_id": [1, 1, 2, 3]}),
        timestamps=pd.Series(
            [
                datetime(2023, 5, 2, 13, 30, tzinfo=timezone.utc),
                datetime(2023, 5, 3, 14, 30, tzinfo=timezone.utc),
                datetime(2023, 5, 3, 13, 30, tzinfo=timezone.utc),
                datetime(2023, 5, 5, 15, 30, tzinfo=timezone.utc),
            ]
        ),
    )
    assert df["count"].tolist() == [2, 3, 1, 1]


@mock
def test_keyed_discrete_hopping_window_aggregation(client):

    data = pd.read_csv("fennel/client_tests/data/app_events.csv").assign(
        timestamp=lambda x: pd.to_datetime(x["timestamp"], utc=True)
    )

    @source(webhook.endpoint("AppEvents"), cdc="append", disorder="14d")
    @dataset
    class AppEvents:
        user_id: int
        page_id: int
        timestamp: datetime

    @dataset(index=True)
    class EventCount:
        user_id: int = field(key=True)
        window: Window = field(key=True)
        count: int
        max_page_id: int
        timestamp: datetime

        @pipeline
        @inputs(AppEvents)
        def pipeline(cls, event: Dataset):
            return event.groupby(
                "user_id", window=Hopping("1d", "1h")
            ).aggregate(
                count=Count(),
                max_page_id=Max(of="page_id", default=0),
                emit="final",
            )

    @dataset(index=True)
    class EventCountAggregate:
        user_id: int = field(key=True)
        avg: float
        timestamp: datetime

        @pipeline
        @inputs(EventCount)
        def pipeline(cls, event: Dataset):
            return event.groupby("user_id").aggregate(
                avg=Average(of="count", window=Continuous("forever")),
            )

    client.commit(
        datasets=[AppEvents, EventCount, EventCountAggregate],
        message="first_commit",
    )
    client.log("fennel_webhook", "AppEvents", data)

    # Test online lookups
    window = Hopping("1d", "1h")
    df, _ = client.lookup(
        EventCount,
        keys=pd.DataFrame(
            {
                "user_id": [1, 2, 3],
                "window": [
                    bucketize(
                        datetime(2023, 5, 2, 13, 0, tzinfo=timezone.utc), window
                    ),
                    bucketize(
                        datetime(2023, 5, 3, 13, 0, tzinfo=timezone.utc), window
                    ),
                    bucketize(
                        datetime(2023, 5, 5, 15, 0, tzinfo=timezone.utc), window
                    ),
                ],
            }
        ),
    )
    assert df["count"].tolist() == [2, 1, 1]

    df, _ = client.lookup(
        EventCountAggregate,
        keys=pd.DataFrame({"user_id": [1, 2, 3]}),
    )
    expected = [2.454, 1.613, 1.537]
    assert df["avg"].tolist() == pytest.approx(expected, abs=1e-3)


@mock
def test_discrete_aggregation_with_lookback(client):

    @dataset(index=True)
    class Stats:
        user_id: str = field(key=True)
        sum: float
        count: int
        t: datetime

        @pipeline
        @inputs(Transactions)
        def pipeline(cls, event: Dataset):
            return (
                event.dedup("id")
                .groupby("user_id")
                .aggregate(
                    count=Count(window=Tumbling("1h", lookback="1h")),
                    sum=Sum(
                        window=Hopping("1h", "30m", lookback="1h"), of="amount"
                    ),
                )
            )

    client.commit(datasets=[Transactions, Stats], message="first_commit")

    now = datetime(2024, 1, 1, 15, 30, 0, tzinfo=timezone.utc)
    now_1h = now - timedelta(hours=1)
    now_2h = now - timedelta(hours=2)
    now_3h = now - timedelta(hours=3)

    # Frontier -> 2024-01-01 15:30:00
    # With lookback as one hour
    # Count -> (2024-01-01 13:00:00, 2024-01-01 14:00:00)
    # Sum -> (2024-01-01 13:30:00, 2024-01-01 14:30:00)

    df = pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "user_id": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4],
            "amount": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            "t": [
                now_1h,
                now_2h,
                now_3h,
                now_1h,
                now_2h,
                now_3h,
                now_1h,
                now_2h,
                now_3h,
                now,
            ],
        }
    )
    client.log("fennel_webhook", "Transactions", df)
    client.sleep()

    df, _ = client.lookup(
        Stats,
        keys=pd.DataFrame({"user_id": [1, 2, 3]}),
        timestamps=pd.Series([now, now, now]),
    )
    assert df["count"].tolist() == [1, 1, 1]
    assert df["sum"].tolist() == [200, 500, 800]

    df, _ = client.lookup(
        Stats,
        keys=pd.DataFrame({"user_id": [1, 2, 3]}),
    )
    assert df["count"].tolist() == [1, 1, 1]
    assert df["sum"].tolist() == [200, 500, 800]
