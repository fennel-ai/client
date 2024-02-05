import pytest
import unittest
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.aggregate import Count, Sum
from fennel.lib.schema import inputs
from fennel.sources import source, Webhook
from fennel.test_lib import mock

webhook = Webhook(name="webhook")
__owner__ = "aditya@fennel.ai"

class TestAssignSnips(unittest.TestCase):
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
        class Aggregated:
            uid: int = field(key=True)
            total_amount: int
            count_1d: int
            timestamp: datetime

            @pipeline(version=1)
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    Count(window="1d", into_field="count_1d"),
                    Sum(of="amount", window="forever", into_field="total_amount")
                )
        # /docsnip

        client.sync(datasets=[Transaction, Aggregated])
        # log some rows to the transaction dataset
        client.log("webhook", "Transaction", pd.DataFrame([
            {"uid": 1, "amount": 10, "timestamp": "2021-01-01T00:00:00"},
            {"uid": 1, "amount": 20, "timestamp": "2021-01-02T00:00:00"},
            {"uid": 2, "amount": 30, "timestamp": "2021-01-02T00:00:00"},
            {"uid": 2, "amount": 40, "timestamp": "2021-01-03T00:00:00"},
        ]))
        # do lookup on the WithSquare dataset
        df, found = Aggregated.lookup(
            pd.Series([datetime(2021, 1, 2, 0, 0, 0), datetime(2021, 1, 2, 0, 0, 0)]),
            uid=pd.Series([1, 2])
        )
        assert(df["uid"].tolist() == [1, 2])
        assert(df["total_amount"].tolist() == [30, 30])
        assert(df["count_1d"].tolist() == [2, 1])
