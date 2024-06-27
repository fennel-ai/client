from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from fennel.connectors import Webhook, source
from fennel.datasets import dataset, Dataset, field, pipeline, Quantile
from fennel.dtypes import Continuous
from fennel.featuresets import featureset, feature as F
from fennel.lib import inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "nitin@fennel.ai"


@source(webhook.endpoint("Transactions"), cdc="append", disorder="14d")
@dataset
class Transactions:
    user_id: int
    amount: float
    timestamp: datetime = field(timestamp=True)
    execution_timestamp: datetime


@dataset(index=True)
class TransactionsDerived:
    user_id: int = field(key=True)
    median: float
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(Transactions)
    def pipeline(cls, event: Dataset):
        return event.groupby("user_id").aggregate(
            median=Quantile(
                p=0.5,
                default=-1,
                approx=True,
                of="amount",
                window=Continuous("forever"),
            ),
            along="execution_timestamp",
        )


@featureset
class TransactionFeatures:
    user_id: int
    median: Optional[float] = F(TransactionsDerived.median)


@mock
def test_quantile_with_default_value(client):

    client.commit(
        message="msg",
        datasets=[Transactions, TransactionsDerived],
        featuresets=[TransactionFeatures],
    )

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4],
            "amount": [10, 20, 30, 40, 50, 60, 70, 8, 90, 100],
            "timestamp": [now, now, now, now, now, now, now, now, now, now],
            "execution_timestamp": [
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
                now,
            ],
        }
    )

    client.log("fennel_webhook", "Transactions", df)
    client.sleep()

    # Extract the data
    df = client.query(
        outputs=[TransactionFeatures.median],
        inputs=[TransactionFeatures.user_id],
        input_dataframe=pd.DataFrame(
            {
                "TransactionFeatures.user_id": [1, 2, 3, 4, 5],
            }
        ),
    )
    assert df["TransactionFeatures.median"].tolist() == [
        20.0,
        50.0,
        70.0,
        100.0,
        -1.0,
    ]
