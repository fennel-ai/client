import unittest
from datetime import datetime

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
            # docsnip-highlight next-line
            amount_sq: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def transform_pipeline(cls, ds: Dataset):
                schema = ds.schema()
                schema["amount_sq"] = int
                # docsnip-highlight start
                return ds.transform(
                    lambda df: df.assign(amount_sq=df["amount"] ** 2), schema
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
        assert df["timestamp"].tolist() == [datetime(2021, 1, 1, 0, 0, 0)]
        assert found.tolist() == [True]

    @mock
    def test_changing_keys(self, client):
        with pytest.raises(Exception):
            # docsnip modifying_keys
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

            webhook = Webhook(name="webhook")

            @source(
                webhook.endpoint("Transaction"), disorder="14d", cdc="upsert"
            )
            @dataset
            class Transaction:
                # docsnip-highlight next-line
                uid: int = field(key=True)
                amount: int
                timestamp: datetime

            def transform(df: pd.DataFrame) -> pd.DataFrame:
                # docsnip-highlight start
                df["user"] = df["uid"]
                df.drop(columns=["uid"], inplace=True)
                # docsnip-highlight end
                return df

            @dataset
            class Derived:
                user: int = field(key=True)
                amount: int
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def bad_pipeline(cls, ds: Dataset):
                    schema = {"user": int, "amount": int, "timestamp": datetime}
                    # docsnip-highlight next-line
                    return ds.transform(transform, schema)

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
            # docsnip-highlight next-line
            amount_sq: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def invalid_pipeline(cls, ds: Dataset):
                schema = ds.schema()
                schema["amount_sq"] = int
                # docsnip-highlight start
                return ds.transform(
                    lambda df: df.assign(amount_sq=str(df["amount"])), schema
                )  # noqa
                # docsnip-highlight end

        # /docsnip
        client.commit(message="msg", datasets=[Transaction, WithHalf])
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
