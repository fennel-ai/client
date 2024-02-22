from datetime import datetime
from typing import List

import pandas as pd
import pytest

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Distinct
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
        amount: int
        timestamp: datetime

    @dataset
    class Aggregated:
        uid: int = field(key=True)
        amounts: List[int]
        timestamp: datetime

        @pipeline
        @inputs(Transaction)
        def pipeline(cls, ds: Dataset):
            return ds.groupby("uid").aggregate(
                Distinct(
                    of="amount",
                    window="1d",
                    into_field="amounts",
                    unordered=True,
                ),
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
                    "amount": 10,
                    "timestamp": "2021-01-01T00:00:00",
                },
                {
                    "uid": 1,
                    "vendor": "B",
                    "amount": 20,
                    "timestamp": "2021-01-02T00:00:00",
                },
                {
                    "uid": 2,
                    "vendor": "A",
                    "amount": 30,
                    "timestamp": "2021-01-03T00:00:00",
                },
                {
                    "uid": 2,
                    "vendor": "B",
                    "amount": 40,
                    "timestamp": "2021-01-04T00:00:00",
                },
                {
                    "uid": 3,
                    "vendor": "A",
                    "amount": 50,
                    "timestamp": "2021-01-05T00:00:00",
                },
                {
                    "uid": 3,
                    "vendor": "B",
                    "amount": 60,
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
    assert df["amounts"].tolist() == [[], [], [50, 60]]


@mock
def test_invalid_type(client):
    with pytest.raises(Exception):
        # docsnip incorrect_type
        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            amounts: int  # should be List[int]
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    Distinct(
                        of="amount",
                        limit=10,
                        into_field="amounts",
                        unordered=True,
                    ),
                )

        # /docsnip
