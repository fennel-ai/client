from datetime import datetime, timezone, timedelta
from typing import Optional, List

import pandas as pd
import pytest

from fennel.connectors import Webhook, source
from fennel.datasets import (
    Average,
    LastK,
    Count,
    Dataset,
    dataset,
    field,
    pipeline,
    Sum,
    Min,
    Max,
    Quantile,
)
from fennel.dtypes import Continuous
from fennel.lib import inputs
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@pytest.mark.integration
@mock
def test_aggregation_with_optional_columns(client):
    @source(webhook.endpoint("Transactions"), cdc="append", disorder="14d")
    @dataset
    class Transactions:
        user_id: int
        amount: Optional[float]
        t: datetime

    @dataset(index=True)
    class Stats:
        user_id: int = field(key=True)
        count_without_null: int
        count: int
        last_10_without_null: List[float]
        last_10: List[Optional[float]]
        sum: float
        t: datetime
        min: float
        max: float
        avg: float
        median: float

        @pipeline
        @inputs(Transactions)
        def pipeline_window(cls, event: Dataset):
            return event.groupby("user_id").aggregate(
                count_without_null=Count(
                    window=Continuous("forever"), dropnull=True, of="amount"
                ),
                count=Count(window=Continuous("forever")),
                last_10_without_null=LastK(
                    limit=10,
                    dedup=True,
                    dropnull=True,
                    of="amount",
                    window=Continuous("forever"),
                ),
                last_10=LastK(
                    limit=10,
                    dedup=True,
                    of="amount",
                    window=Continuous("forever"),
                ),
                sum=Sum(
                    of="amount",
                    window=Continuous("forever"),
                ),
                min=Min(
                    of="amount",
                    window=Continuous("forever"),
                    default=0.0,
                ),
                max=Max(
                    of="amount",
                    window=Continuous("forever"),
                    default=0.0,
                ),
                avg=Average(
                    of="amount",
                    window=Continuous("forever"),
                    default=0.0,
                ),
                median=Quantile(
                    of="amount",
                    p=0.5,
                    approx=True,
                    window=Continuous("forever"),
                    default=0.0,
                ),
            )

    client.commit(datasets=[Transactions, Stats], message="first_commit")

    now = datetime.now(timezone.utc)
    now_1d = datetime.now(timezone.utc) - timedelta(days=1)
    now_2d = datetime.now(timezone.utc) - timedelta(days=2)
    now_3d = datetime.now(timezone.utc) - timedelta(days=3)

    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 1, 1, 2, 2, 2, 3, 3],
            "amount": [100, None, 300, 400, 500, None, 700, 800, None, 1000],
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
    assert df["count_without_null"].tolist() == [4, 2, 1]
    assert df["count"].tolist() == [5, 3, 2]
    assert df["min"].tolist() == [100.0, 700.0, 1000.0]
    assert df["max"].tolist() == [500.0, 800.0, 1000.0]
    assert df["avg"].tolist() == [325.0, 750.0, 1000.0]
    assert df["median"].tolist() == [
        pytest.approx(400.0, abs=5e-1),
        pytest.approx(800.0, abs=5e-1),
        pytest.approx(1000.0, abs=5e-1),
    ]

    # Doing because order of NAN comes different sometimes from server.
    if not client.is_integration_client():
        assert df["last_10_without_null"].tolist() == [
            [100.0, 500.0, 300.0, 400.0],
            [700.0, 800.0],
            [1000.0],
        ]
        assert df["last_10"].tolist() == [
            [100.0, 500.0, 300.0, pd.NA, 400.0],
            [pd.NA, 700.0, 800.0],
            [1000.0, pd.NA],
        ]
    assert df["sum"].tolist() == [1300.0, 1500.0, 1000.0]
