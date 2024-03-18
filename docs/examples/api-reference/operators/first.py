import unittest
from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestFirstSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset, index
        from fennel.lib import inputs
        from fennel.sources import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime

        @index
        @dataset
        class FirstOnly:
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def first_pipeline(cls, ds: Dataset):
                # docsnip-highlight next-line
                return ds.groupby("uid").first()

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, FirstOnly])
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
