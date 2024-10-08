import unittest
from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestAssignSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="upsert")
        @dataset
        class Transaction:
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

        @dataset(index=True)
        class WithSquare:
            uid: int = field(key=True)
            amount: int
            amount_sq: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def my_pipeline(cls, ds: Dataset):
                return ds.assign("amount_sq", int, lambda df: df["amount"] ** 2)

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, WithSquare])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [{"uid": 1, "amount": 10, "timestamp": "2021-01-01T00:00:00"}]
            ),
        )
        # do lookup on the WithSquare dataset
        df, found = WithSquare.lookup(
            pd.Series([datetime(2021, 1, 1, 0, 0, 0)]), uid=pd.Series([1])
        )
        assert df["uid"].tolist() == [1]
        assert df["amount"].tolist() == [10]
        assert df["amount_sq"].tolist() == [100]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ]
        assert found.tolist() == [True]

    @mock
    def test_expression(self, client):
        # docsnip expression
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        # docsnip-highlight next-line
        from fennel.expr import col

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="upsert")
        @dataset
        class Transaction:
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

        @dataset(index=True)
        class WithSquare:
            uid: int = field(key=True)
            amount: int
            amount_sq: int
            amount_half: float
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def my_pipeline(cls, ds: Dataset):
                # docsnip-highlight start
                return ds.assign(
                    amount_sq=(col("amount") * col("amount")).astype(int),
                    amount_half=(col("amount") / 2).astype(float),
                )
                # docsnip-highlight end

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, WithSquare])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [{"uid": 1, "amount": 10, "timestamp": "2021-01-01T00:00:00"}]
            ),
        )
        # do lookup on the WithSquare dataset
        df, found = WithSquare.lookup(
            pd.Series([datetime(2021, 1, 1, 0, 0, 0)]), uid=pd.Series([1])
        )
        assert df["uid"].tolist() == [1]
        assert df["amount"].tolist() == [10]
        assert df["amount_sq"].tolist() == [100]
        assert df["amount_half"].tolist() == [5.0]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        ]
        assert found.tolist() == [True]

    @mock
    def test_invalid_type_expr(self, client):
        # docsnip incorrect_type_expr
        with pytest.raises(Exception):

            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook
            from fennel.expr import col

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="upsert")
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
                amount_half: int
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def my_pipeline(cls, ds: Dataset):
                    return ds.assign(
                        amount_sq=(col("amount") * col("amount")).astype(int),
                        # docsnip-highlight next-line
                        amount_half=(col("amount") / 2).astype(int),
                    )

        # /docsnip

    @mock
    def test_invalid_type(self, client):
        # docsnip incorrect_type
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="upsert")
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

            @pipeline
            @inputs(Transaction)
            def my_pipeline(cls, ds: Dataset):
                return ds.assign(
                    "amount_sq", int, lambda df: df["amount"] * 0.5
                )

        # /docsnip
        client.commit(message="some msg", datasets=[Transaction, WithHalf])
        # log some rows to the transaction dataset
        with pytest.raises(Exception):
            client.log(
                "webhook",
                "Transaction",
                pd.DataFrame(
                    [
                        {
                            "uid": 1,
                            "amount": 10,
                            "timestamp": "2021-01-01T00:00:00",
                        }
                    ]
                ),
            )
