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

default_ts = datetime.fromtimestamp(0, timezone.utc)
default_date = default_ts.date()


@source(webhook.endpoint("Transactions"), disorder="14d", cdc="append")
@dataset
class Transactions:
    user_id: int
    amount: Decimal[2]
    is_debit: bool
    transaction_ts: datetime
    transaction_date: date
    timestamp: datetime = field(timestamp=True)


@dataset(index=True)
class DebitDataset:
    user_id: int = field(key=True)
    txn_date: date = field(key=True)
    avg: float
    sum: Decimal[2]
    min: Decimal[2]
    max: Decimal[2]
    earliest_transaction_ts: datetime
    latest_transaction_ts: datetime
    earliest_transaction_date: date
    latest_transaction_date: date
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
                Min(
                    of="transaction_ts",
                    into_field="earliest_transaction_ts",
                    window=Continuous("forever"),
                    default=datetime(1970, 1, 1, tzinfo=timezone.utc),
                ),
                Max(
                    of="transaction_ts",
                    into_field="latest_transaction_ts",
                    window=Continuous("forever"),
                    default=datetime(1970, 1, 1, tzinfo=timezone.utc),
                ),
                Min(
                    of="transaction_date",
                    into_field="earliest_transaction_date",
                    window=Continuous("forever"),
                    default=date(1970, 1, 1),
                ),
                Max(
                    of="transaction_date",
                    into_field="latest_transaction_date",
                    window=Continuous("forever"),
                    default=date(1970, 1, 1),
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
    earliest_transaction_ts: datetime = F(
        DebitDataset.earliest_transaction_ts, default=default_ts
    )
    latest_transaction_ts: datetime = F(
        DebitDataset.latest_transaction_ts, default=default_ts
    )
    earliest_transaction_date: date = F(
        DebitDataset.earliest_transaction_date, default=default_date
    )
    latest_transaction_date: date = F(
        DebitDataset.latest_transaction_date, default=default_date
    )
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

    # microseconds are dropped when df is converted to json before
    # passing to backend
    now = datetime.now(timezone.utc).replace(microsecond=0)
    now_l1d = now - timedelta(days=1)
    now_date = now.date()
    now_l1d_date = now_l1d.date()
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
            "transaction_ts": [now, now_l1d, now_l1d, now, now],
            "transaction_date": [
                now_date,
                now_l1d_date,
                now_l1d_date,
                now_date,
                now_date,
            ],
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
            DebitFeatures.earliest_transaction_ts,
            DebitFeatures.latest_transaction_ts,
            DebitFeatures.earliest_transaction_date,
            DebitFeatures.latest_transaction_date,
            DebitFeatures.stddev,
            DebitFeatures.median,
            DebitFeatures.sum,
            DebitFeatures.avg,
        ],
        input_dataframe=pd.DataFrame(
            {
                "DebitFeatures.user_id": [1, 1, 2, 2, 3],
                "DebitFeatures.txn_date": [
                    str(now_date),
                    str(now_l1d_date),
                    str(now_date),
                    str(now_l1d_date),
                    str(now_date),
                ],
            },
        ),
    )
    assert df.shape == (5, 11)
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
    assert df["DebitFeatures.earliest_transaction_ts"].tolist() == [
        now,
        default_ts,
        default_ts,
        now_l1d,
        default_ts,
    ]
    assert df["DebitFeatures.latest_transaction_ts"].tolist() == [
        now,
        default_ts,
        default_ts,
        now,
        default_ts,
    ]
    assert df["DebitFeatures.earliest_transaction_date"].tolist() == [
        now_date,
        default_date,
        default_date,
        now_l1d_date,
        default_date,
    ]
    assert df["DebitFeatures.latest_transaction_date"].tolist() == [
        now_date,
        default_date,
        default_date,
        now_date,
        default_date,
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
