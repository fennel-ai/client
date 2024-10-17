import unittest
from datetime import datetime, timezone

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestDedupSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Deduped:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def dedup_pipeline(cls, ds: Dataset):
                # docsnip-highlight next-line
                return ds.dedup(by="txid")

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, Deduped])
        # log some rows to the transaction dataset, with some duplicates
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "txid": 1,
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "txid": 1,
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "txid": 2,
                        "uid": 2,
                        "amount": 20,
                        "timestamp": "2021-01-02T00:00:00",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df = client.get_dataset_df("Deduped")
        assert df["txid"].tolist() == [1, 2]
        assert df["uid"].tolist() == [1, 2]
        assert df["amount"].tolist() == [10, 20]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2021, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        ]

    @mock
    def test_dedup_by_all(self, client):
        # docsnip dedup_by_all
        from fennel.datasets import dataset, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Deduped:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def dedup_by_all_pipeline(cls, ds: Dataset):
                # docsnip-highlight next-line
                return ds.dedup()

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, Deduped])
        # log some rows to the transaction dataset, with some duplicates
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "txid": 1,
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "txid": 1,
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "txid": 2,
                        "uid": 2,
                        "amount": 20,
                        "timestamp": "2021-01-02T00:00:00",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df = client.get_dataset_df("Deduped")
        assert df["txid"].tolist() == [1, 2]
        assert df["uid"].tolist() == [1, 2]
        assert df["amount"].tolist() == [10, 20]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2021, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
        ]

    @mock
    def test_dedup_with_session_window(self, client):
        # docsnip dedup_with_session_window
        from fennel.datasets import dataset, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook
        from fennel.dtypes import Session

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Deduped:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def dedup_by_all_pipeline(cls, ds: Dataset):
                # docsnip-highlight next-line
                return ds.dedup(by="txid", window=Session(gap="10s"))

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, Deduped])
        # log some rows to the transaction dataset, with some duplicates
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "txid": 1,
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "txid": 1,
                        "uid": 3,
                        "amount": 30,
                        "timestamp": "2021-01-01T00:00:11",
                    },
                    {
                        "txid": 1,
                        "uid": 30,
                        "amount": 40,
                        "timestamp": "2021-01-01T00:00:11",
                    },
                    {
                        "txid": 1,
                        "uid": 20,
                        "amount": 20,
                        "timestamp": "2021-01-01T00:00:02",
                    },
                    {
                        "txid": 1,
                        "uid": 4,
                        "amount": 40,
                        "timestamp": "2021-01-01T00:00:21",
                    },
                    {
                        "txid": 2,
                        "uid": 4,
                        "amount": 40,
                        "timestamp": "2021-01-01T00:00:21",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df = client.get_dataset_df("Deduped")
        assert df["txid"].tolist() == [1, 1, 2]
        assert df["uid"].tolist() == [30, 4, 4]
        assert df["amount"].tolist() == [40, 40, 40]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 11, tzinfo=timezone.utc),
            datetime(2021, 1, 1, 0, 0, 21, tzinfo=timezone.utc),
            datetime(2021, 1, 1, 0, 0, 21, tzinfo=timezone.utc),
        ]

    @mock
    def test_dedup_with_tumbling_window(self, client):
        # docsnip dedup_with_tumbling_window
        from fennel.datasets import dataset, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook
        from fennel.dtypes import Tumbling

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Deduped:
            txid: int
            uid: int
            amount: int
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def dedup_by_all_pipeline(cls, ds: Dataset):
                # docsnip-highlight next-line
                return ds.dedup(by="txid", window=Tumbling(duration="10s"))

        # /docsnip

        client.commit(message="some msg", datasets=[Transaction, Deduped])
        # log some rows to the transaction dataset, with some duplicates
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "txid": 1,
                        "uid": 1,
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "txid": 1,
                        "uid": 3,
                        "amount": 30,
                        "timestamp": "2021-01-01T00:00:09",
                    },
                    {
                        "txid": 1,
                        "uid": 4,
                        "amount": 40,
                        "timestamp": "2021-01-01T00:00:09",
                    },
                    {
                        "txid": 1,
                        "uid": 2,
                        "amount": 20,
                        "timestamp": "2021-01-01T00:00:19",
                    },
                    {
                        "txid": 2,
                        "uid": 2,
                        "amount": 20,
                        "timestamp": "2021-01-01T00:00:05",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df = client.get_dataset_df("Deduped")
        assert df["txid"].tolist() == [2, 1, 1]
        assert df["uid"].tolist() == [2, 4, 2]
        assert df["amount"].tolist() == [20, 40, 20]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 5, tzinfo=timezone.utc),
            datetime(2021, 1, 1, 0, 0, 9, tzinfo=timezone.utc),
            datetime(2021, 1, 1, 0, 0, 19, tzinfo=timezone.utc),
        ]
