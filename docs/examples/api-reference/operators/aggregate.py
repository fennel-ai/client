import unittest
from datetime import datetime

import pandas as pd

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestAssignSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import (
            dataset,
            field,
            pipeline,
            Dataset,
            Count,
            Sum,
        )
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime = field(timestamp=True)
            transaction_time: datetime

        @dataset(index=True)
        class Aggregated:
            # docsnip-highlight start
            # groupby field becomes the key field
            uid: int = field(key=True)
            # new fields are added to the dataset by the aggregate operation
            total: int
            count_1d: int
            # docsnip-highlight end
            transaction_time: datetime = field(timestamp=True)

            @pipeline
            @inputs(Transaction)
            def aggregate_pipeline(cls, ds: Dataset):
                # docsnip-highlight start
                return ds.groupby("uid").aggregate(
                    count_1d=Count(window="1d"),
                    total=Sum(of="amount", window="forever"),
                    along="transaction_time"
                )
                # docsnip-highlight end

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, Aggregated])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "amount": 10,
                        "transaction_time": "2021-01-01T00:00:00",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 1,
                        "amount": 20,
                        "transaction_time": "2021-01-02T00:00:00",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "amount": 30,
                        "transaction_time": "2021-01-02T00:00:00",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "amount": 40,
                        "transaction_time": "2021-01-03T00:00:00",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df, found = Aggregated.lookup(
            pd.Series(
                [datetime(2021, 1, 2, 0, 0, 0), datetime(2021, 1, 2, 0, 0, 0)]
            ),
            uid=pd.Series([1, 2]),
        )
        assert df["uid"].tolist() == [1, 2]
        assert df["total"].tolist() == [30, 30]
        assert df["count_1d"].tolist() == [2, 1]
