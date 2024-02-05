import pytest
import unittest
from datetime import datetime

import pandas as pd
from fennel.test_lib import mock

__owner__ = "aditya@fennel.ai"

class TestAssignSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib.schema import inputs
        from fennel.sources import source, Webhook
        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

        @dataset
        class WithSquare:
            uid: int = field(key=True)
            amount: int
            amount_sq: int
            timestamp: datetime

            @pipeline(version=1)
            @inputs(Transaction)
            def my_pipeline(cls, ds: Dataset):
                return ds.assign("amount_sq", int, lambda df: df["amount"] ** 2)
        # /docsnip

        client.sync(datasets=[Transaction, WithSquare])
        # log some rows to the transaction dataset
        client.log("webhook", "Transaction", pd.DataFrame([{"uid": 1, "amount": 10, "timestamp": "2021-01-01T00:00:00"}]))
        # do lookup on the WithSquare dataset
        df, found = WithSquare.lookup(
            pd.Series([datetime(2021, 1, 1, 0, 0, 0)]),
            uid=pd.Series([1])
        )
        assert(df["uid"].tolist() == [1])
        assert(df["amount"].tolist() == [10])
        assert(df["amount_sq"].tolist() == [100])
        assert(df["timestamp"].tolist() == [datetime(2021, 1, 1, 0, 0, 0)])
        assert(found.tolist() == [True])

    @mock
    def test_invalid_type(self, client):
        # docsnip incorrect_type
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib.schema import inputs
        from fennel.sources import source, Webhook
        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

        @dataset
        class WithHalf:
            uid: int = field(key=True)
            amount: int
            amount_sq: int
            timestamp: datetime

            @pipeline(version=1)
            @inputs(Transaction)
            def my_pipeline(cls, ds: Dataset):
                return ds.assign("amount_sq", int, lambda df: df["amount"] * 0.5)
        # /docsnip
        client.sync(datasets=[Transaction, WithHalf])
        # log some rows to the transaction dataset
        with pytest.raises(Exception):
            client.log("webhook", "Transaction", pd.DataFrame([{"uid": 1, "amount": 10, "timestamp": "2021-01-01T00:00:00"}]))
        
