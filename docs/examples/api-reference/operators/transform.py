import unittest
from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field, pipeline, Dataset
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
            uid: int = field(key=True)
            amount: int
            timestamp: datetime

        @dataset
        class WithSquare:
            uid: int = field(key=True)
            amount: int
            amount_sq: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                schema = ds.schema()
                schema["amount_sq"] = int
                return ds.transform(
                    lambda df: df.assign(amount_sq=df["amount"] ** 2), schema
                )  # noqa

        # /docsnip

        client.commit(datasets=[Transaction, WithSquare])
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
        assert df["timestamp"].tolist() == [datetime(2021, 1, 1, 0, 0, 0)]
        assert found.tolist() == [True]

    @mock
    def test_changing_keys(self, client):
        with pytest.raises(Exception):
            # docsnip modifying_keys
            @source(webhook.endpoint("Transaction"))
            @dataset
            class Transaction:
                uid: int = field(key=True)
                amount: int
                timestamp: datetime

            def transform(df: pd.DataFrame) -> pd.DataFrame:
                df["user"] = df["uid"]
                df.drop(columns=["uid"], inplace=True)
                return df

            @dataset
            class Derived:
                user: int = field(key=True)
                amount: int
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def pipeline(cls, ds: Dataset):
                    schema = {"user": int, "amount": int, "timestamp": datetime}
                    return ds.transform(transform, schema)

            # /docsnip

    @mock
    def test_invalid_type(self, client):
        # docsnip incorrect_type
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

            @pipeline
            @inputs(Transaction)
            def pipeline(cls, ds: Dataset):
                schema = ds.schema()
                schema["amount_sq"] = int
                return ds.transform(
                    lambda df: df.assign(amount_sq=str(df["amount"])), schema
                )  # noqa

        # /docsnip
        client.commit(datasets=[Transaction, WithHalf])
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
