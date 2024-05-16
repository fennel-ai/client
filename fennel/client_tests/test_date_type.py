from datetime import datetime, date, timedelta, timezone
from decimal import Decimal as PythonDecimal

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import Webhook, source
from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Dataset,
    Average,
    Count,
    Sum,
    Max,
    Min,
    Stddev,
    Quantile,
)
from fennel.dtypes import Decimal, Continuous
from fennel.featuresets import featureset, feature as F
from fennel.lib import inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "eng@fennel.ai"


@source(webhook.endpoint("Transactions"), disorder="14d", cdc="append")
@dataset
class Transactions:
    user_id: int
    amount: Decimal[2]
    is_debit: bool
    timestamp: datetime = field(timestamp=True)


@dataset(index=True)
class DebitDataset:
    user_id: int = field(key=True)
    txn_date: date = field(key=True)
    avg: float
    sum: Decimal[2]
    min: Decimal[2]
    max: Decimal[2]
    stddev: float
    median: float
    count: int
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(Transactions)
    def pipeline(cls, event: Dataset):
        return (
            event.filter(lambda x: x["is_debit"] == True)  # noqa: E712
            .assign("txn_date", date, lambda x: x["timestamp"].dt.date)
            .groupby("user_id", "txn_date")
            .aggregate(
                Count(into_field="count", window=Continuous("forever")),
                Average(
                    of="amount", into_field="avg", window=Continuous("forever")
                ),
                Sum(
                    of="amount", into_field="sum", window=Continuous("forever")
                ),
                Min(
                    of="amount",
                    into_field="min",
                    window=Continuous("forever"),
                    default=0.0,
                ),
                Max(
                    of="amount",
                    into_field="max",
                    window=Continuous("forever"),
                    default=0.0,
                ),
                Stddev(
                    of="amount",
                    into_field="stddev",
                    window=Continuous("forever"),
                    default=0.0,
                ),
                Quantile(
                    of="amount",
                    into_field="median",
                    window=Continuous("forever"),
                    approx=True,
                    default=0.0,
                    p=0.5,
                ),
            )
        )


@featureset
class DebitFeatures:
    user_id: int
    txn_date: date
    count: int = F(DebitDataset.count, default=0)
    avg: float = F(DebitDataset.avg, default=0.0)
    sum: Decimal[2] = F(DebitDataset.sum, default=0.0)
    min: Decimal[2] = F(DebitDataset.min, default=0.0)
    max: Decimal[2] = F(DebitDataset.max, default=0.0)
    stddev: float = F(DebitDataset.stddev, default=0.0)
    median: float = F(DebitDataset.median, default=0.0)


@pytest.mark.integration
@mock
def test_date_type(client):
    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[Transactions, DebitDataset],
        featuresets=[DebitFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()

    now = datetime.now(timezone.utc)
    now_l1d = now - timedelta(days=1)
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 2, 2, 1],
            "amount": [
                1200.10,
                1000.10,
                1400.10,
                90.10,
                1100.10,
            ],
            "is_debit": [True, False, True, True, False],
            "timestamp": [now, now_l1d, now_l1d, now_l1d, now],
        }
    )
    response = client.log("fennel_webhook", "Transactions", df)
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep()

    # Querying UserInfoFeatures
    df = client.query(
        inputs=[DebitFeatures.user_id, DebitFeatures.txn_date],
        outputs=[
            DebitFeatures.count,
            DebitFeatures.max,
            DebitFeatures.min,
            DebitFeatures.stddev,
            DebitFeatures.median,
            DebitFeatures.sum,
            DebitFeatures.avg,
        ],
        input_dataframe=pd.DataFrame(
            {
                "DebitFeatures.user_id": [1, 1, 2, 2, 3],
                "DebitFeatures.txn_date": [
                    str(now.date()),
                    str(now_l1d.date()),
                    str(now.date()),
                    str(now_l1d.date()),
                    str(now.date()),
                ],
            },
        ),
    )
    assert df.shape == (5, 7)
    assert df["DebitFeatures.count"].tolist() == [1, 0, 0, 2, 0]
    assert df["DebitFeatures.avg"].tolist() == [
        pytest.approx(1200.1),
        0.0,
        0.0,
        745.1,
        0.0,
    ]
    assert df["DebitFeatures.min"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("0.00"),
        PythonDecimal("0.00"),
        PythonDecimal("90.10"),
        PythonDecimal("0.00"),
    ]
    assert df["DebitFeatures.max"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("0.00"),
        PythonDecimal("0.00"),
        PythonDecimal("1400.10"),
        PythonDecimal("0.00"),
    ]
    assert df["DebitFeatures.sum"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("0.00"),
        PythonDecimal("0.00"),
        PythonDecimal("1490.20"),
        PythonDecimal("0.00"),
    ]
    if not client.is_integration_client():
        assert df["DebitFeatures.median"].tolist() == [
            pytest.approx(1200.1),
            0.0,
            0.0,
            pytest.approx(1400.1),
            0.0,
        ]
    assert df["DebitFeatures.stddev"].tolist() == [
        0,
        0,
        0,
        655.0,
        0,
    ]
