import pytest
from typing import List
from datetime import datetime

import pandas as pd

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field, pipeline, Dataset, LastK
    from fennel.lib import inputs
    from fennel.sources import source, Webhook

    webhook = Webhook(name="webhook")

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
        def lastk_pipeline(cls, ds: Dataset):
            return ds.groupby("uid").aggregate(
                # docsnip-highlight start
                LastK(
                    of="amount",
                    limit=10,
                    dedup=False,
                    window="1d",
                    into_field="amounts",
                ),
                # docsnip-highlight end
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
    assert df["amounts"].tolist() == [[], [], [60, 50]]


@mock
def test_invalid_type(client):
    with pytest.raises(Exception):
        # docsnip incorrect_type
        from fennel.datasets import dataset, field, pipeline, Dataset, LastK
        from fennel.lib import inputs
        from fennel.sources import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            # docsnip-highlight next-line
            amounts: int  # should be List[int]
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def bad_pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    # docsnip-highlight start
                    LastK(
                        of="amount",
                        limit=10,
                        dedup=False,
                        window="1d",
                        into_field="amounts",
                    ),
                    # docsnip-highlight end
                )

        # /docsnip
