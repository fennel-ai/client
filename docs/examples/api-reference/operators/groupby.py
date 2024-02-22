import unittest
from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib import inputs
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="webhook")
__owner__ = "aditya@fennel.ai"


class TestGroupbySnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        @source(webhook.endpoint("Transaction"))
        @dataset
        class Transaction:
            uid: int
            category: str
            timestamp: datetime

        @dataset
        class FirstInCategory:
            category: str = field(key=True)
            uid: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, transactions: Dataset):
                return transactions.groupby("category").first()

        # /docsnip

        # log some rows to the transaction dataset
        client.commit(datasets=[Transaction, FirstInCategory])
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "category": "grocery",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "category": "electronics",
                        "timestamp": "2021-02-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "category": "grocery",
                        "timestamp": "2021-03-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "category": "electronics",
                        "timestamp": "2021-04-01T00:00:00",
                    },
                ]
            ),
        )
        df = client.get_dataset_df("FirstInCategory")
        assert df["category"].tolist() == ["grocery", "electronics"]
        assert df["uid"].tolist() == [1, 2]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 2, 1, 0, 0, 0),
        ]

    @mock
    def test_groupby_non_existent_column(self, client):
        with pytest.raises(Exception):
            # docsnip non_existent_column
            @source(webhook.endpoint("Transaction"))
            @dataset
            class Transaction:
                uid: int
                category: str
                timestamp: datetime

            @dataset
            class FirstInCategory:
                category: str = field(key=True)
                uid: int
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def pipeline(cls, transactions: Dataset):
                    return transactions.groupby("non_existent_column").first()

            # /docsnip
