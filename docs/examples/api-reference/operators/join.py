import unittest
from datetime import datetime, timezone

import pandas as pd

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestJoinSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            merchant: int
            amount: int
            timestamp: datetime

        @source(
            webhook.endpoint("MerchantCategory"), disorder="14d", cdc="upsert"
        )
        @dataset(index=True)
        class MerchantCategory:
            # docsnip-highlight start
            # right side of the join can only be on key fields
            merchant: int = field(key=True)
            # docsnip-highlight end
            category: str
            updated_at: datetime  # won't show up in joined dataset

        @dataset
        class WithCategory:
            uid: int
            merchant: int
            amount: int
            timestamp: datetime
            category: str

            @pipeline
            @inputs(Transaction, MerchantCategory)
            def join_pipeline(cls, tx: Dataset, merchant_category: Dataset):
                # docsnip-highlight next-line
                return tx.join(merchant_category, on=["merchant"], how="inner")

        # /docsnip

        # log some rows to both datasets
        client.commit(
            message="some msg",
            datasets=[Transaction, MerchantCategory, WithCategory],
        )
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "merchant": 4,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "merchant": 5,
                        "amount": 20,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "merchant": 4,
                        "amount": 30,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "merchant": 6,
                        "amount": 30,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        client.log(
            "webhook",
            "MerchantCategory",
            pd.DataFrame(
                [
                    {
                        "merchant": 4,
                        "category": "grocery",
                        "updated_at": "2021-01-01T00:00:00",
                    },
                    {
                        "merchant": 5,
                        "category": "electronics",
                        "updated_at": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        df = client.get_dataset_df("WithCategory")
        assert df["uid"].tolist() == [1, 2, 3]
        assert df["merchant"].tolist() == [4, 5, 4]
        assert df["amount"].tolist() == [10, 20, 30]
        assert df["category"].tolist() == ["grocery", "electronics", "grocery"]
        assert (
            df["timestamp"].tolist()
            == [datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)] * 3
        )
        
    @mock
    def test_optional(self, client):
        # docsnip optional_join
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook
        from typing import Optional
        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            merchant: Optional[int]
            amount: int
            timestamp: datetime

        @source(
            webhook.endpoint("MerchantCategory"), disorder="14d", cdc="upsert"
        )
        @dataset(index=True)
        class MerchantCategory:
            # docsnip-highlight start
            # right side of the join can only be on key fields
            merchant: int = field(key=True)
            # docsnip-highlight end
            category: str
            updated_at: datetime  # won't show up in joined dataset

        @dataset
        class WithCategory:
            uid: int
            merchant: Optional[int]
            amount: int
            timestamp: datetime
            category: Optional[str]

            @pipeline
            @inputs(Transaction, MerchantCategory)
            def join_pipeline(cls, tx: Dataset, merchant_category: Dataset):
                # docsnip-highlight next-line
                return tx.join(merchant_category, on=["merchant"], how="left")

        # /docsnip

        # log some rows to both datasets
        client.commit(
            message="some msg",
            datasets=[Transaction, MerchantCategory, WithCategory],
        )
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "merchant": 4,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 1,
                        "merchant": None,
                        "amount": 15,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "merchant": 5,
                        "amount": 20,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "merchant": 4,
                        "amount": 30,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "merchant": 6,
                        "amount": 30,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        client.log(
            "webhook",
            "MerchantCategory",
            pd.DataFrame(
                [
                    {
                        "merchant": 4,
                        "category": "grocery",
                        "updated_at": "2021-01-01T00:00:00",
                    },
                    {
                        "merchant": 5,
                        "category": "electronics",
                        "updated_at": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        import numpy as np
        df = client.get_dataset_df("WithCategory")
        df = df.replace({np.nan: None})
        assert df["uid"].tolist() == [1, 1, 2, 3, 3]
        assert df["merchant"].tolist() == [4, None, 5, 4, 6]
        assert df["amount"].tolist() == [10, 15, 20, 30, 30]
        assert df["category"].tolist() == ["grocery", None, "electronics", "grocery", None]
        assert (
            df["timestamp"].tolist()
            == [datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)] * 5
        )
