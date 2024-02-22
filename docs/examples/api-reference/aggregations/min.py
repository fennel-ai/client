from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Min
from fennel.lib.schema import inputs
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="webhook")
__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    @source(webhook.endpoint("Transaction"))
    @dataset
    class Transaction:
        uid: int
        amt: float
        timestamp: datetime

    @dataset
    class Aggregated:
        uid: int = field(key=True)
        min_1d: float
        min_1w: float
        timestamp: datetime

        @pipeline
        @inputs(Transaction)
        def pipeline(cls, ds: Dataset):
            return ds.groupby("uid").aggregate(
                Min(of="amt", window="1d", default=-1.0, into_field="min_1d"),
                Min(of="amt", window="1w", default=-1.0, into_field="min_1w"),
            )

    # /docsnip
    client.commit(datasets=[Transaction, Aggregated])
    # log some rows to the transaction dataset
    client.log(
        "webhook",
        "Transaction",
        pd.DataFrame(
            [
                {
                    "uid": 1,
                    "vendor": "A",
                    "amt": 10,
                    "timestamp": "2021-01-01T00:00:00",
                },
                {
                    "uid": 1,
                    "vendor": "B",
                    "amt": 20,
                    "timestamp": "2021-01-02T00:00:00",
                },
                {
                    "uid": 2,
                    "vendor": "A",
                    "amt": 30,
                    "timestamp": "2021-01-03T00:00:00",
                },
                {
                    "uid": 2,
                    "vendor": "B",
                    "amt": 40,
                    "timestamp": "2021-01-04T00:00:00",
                },
                {
                    "uid": 3,
                    "vendor": "A",
                    "amt": 50,
                    "timestamp": "2021-01-05T00:00:00",
                },
                {
                    "uid": 3,
                    "vendor": "B",
                    "amt": 60,
                    "timestamp": "2021-01-06T00:00:00",
                },
            ]
        ),
    )

    # do lookup on the Aggregated dataset
    ts = pd.Series(
        [
            datetime(2021, 1, 6, 0, 0, 0),
            datetime(2021, 1, 6, 0, 0, 0),
            datetime(2021, 1, 6, 0, 0, 0),
        ]
    )
    df, found = Aggregated.lookup(ts, uid=pd.Series([1, 2, 3]))
    assert found.tolist() == [True, True, True]
    assert df["uid"].tolist() == [1, 2, 3]
    assert df["min_1d"].tolist() == [None, None, 50]
    assert df["min_1w"].tolist() == [10, 30, 50]


@mock
def test_invalid_type(client):
    with pytest.raises(Exception):
        # docsnip incorrect_type
        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int
            zip: str
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            min_1d: str
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    Min(of="zip", window="1d", default="min", into_field="min"),
                )

        # /docsnip


@mock
def test_non_matching_types(client):
    with pytest.raises(Exception):
        # docsnip non_matching_types
        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int
            amt: float
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            min_1d: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    Min(of="amt", window="1d", default=1, into_field="min_1d"),
                )

        # /docsnip
