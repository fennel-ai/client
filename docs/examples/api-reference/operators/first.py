import unittest
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.schema import inputs
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="webhook")
__owner__ = "aditya@fennel.ai"


class TestFirstSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class FirstOnly:
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                return ds.groupby("uid").first()

        # /docsnip

        client.commit(datasets=[Transaction, FirstOnly])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 1,
                        "amount": 20,
                        "timestamp": "2021-01-02T00:00:00",
                    },
                    {
                        "uid": 2,
                        "amount": 30,
                        "timestamp": "2021-01-02T00:00:00",
                    },
                    {
                        "uid": 2,
                        "amount": 40,
                        "timestamp": "2021-01-03T00:00:00",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df, found = FirstOnly.lookup(
            pd.Series(
                [datetime(2021, 1, 3, 0, 0, 0), datetime(2021, 1, 3, 0, 0, 0)]
            ),
            uid=pd.Series([1, 2]),
        )
        assert df["uid"].tolist() == [1, 2]
        assert df["amount"].tolist() == [10, 30]
