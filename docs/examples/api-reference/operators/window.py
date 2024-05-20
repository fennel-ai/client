import unittest
from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel.testing import mock

__owner__ = "hoang@fennel.ai"


class TestAssignSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook
        from fennel.dtypes import Window, Session

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime = field(timestamp=True)

        @dataset(index=True)
        class SessionDS:
            # docsnip-highlight start
            # groupby field becomes the key field
            uid: int = field(key=True)
            # window also becomes a key field
            window: Window = field(key=True)
            # docsnip-highlight end
            timestamp: datetime = field(timestamp=True)

            @pipeline
            @inputs(Transaction)
            def window_pipeline(cls, ds: Dataset):
                return ds.groupby("uid", window=Session("15m")).aggregate()

        # /docsnip

        client.commit(datasets=[Transaction, SessionDS], message="msg")
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
                        "timestamp": "2021-01-01T00:10:00",
                    },
                    {
                        "uid": 2,
                        "amount": 30,
                        "timestamp": "2021-01-02T00:10:00",
                    },
                    {
                        "uid": 2,
                        "amount": 40,
                        "timestamp": "2021-01-02T00:15:00",
                    },
                ]
            ),
        )
        # do lookup on the window dataset
        df, found = client.lookup(
            "SessionDS",
            timestamps=pd.Series(
                [
                    datetime(2021, 1, 1, 0, 10, 0, microsecond=1),
                    datetime(2021, 1, 2, 0, 15, 0, microsecond=1),
                ]
            ),
            keys=pd.DataFrame(
                {
                    "uid": [1, 2],
                    "window": [
                        {
                            "begin": datetime(
                                2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc
                            ),
                            "end": datetime(
                                2021,
                                1,
                                1,
                                0,
                                10,
                                0,
                                microsecond=1,
                                tzinfo=timezone.utc,
                            ),
                        },
                        {
                            "begin": datetime(
                                2021, 1, 2, 0, 10, 0, tzinfo=timezone.utc
                            ),
                            "end": datetime(
                                2021,
                                1,
                                2,
                                0,
                                15,
                                0,
                                microsecond=1,
                                tzinfo=timezone.utc,
                            ),
                        },
                    ],
                }
            ),
        )
        assert found.tolist() == [True, True]
        assert df["uid"].tolist() == [1, 2]

    @mock
    def test_miss_window_key(self, client):
        with pytest.raises(Exception):
            # docsnip miss_window_key
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.dtypes import Session
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                amount: int
                timestamp: datetime = field(timestamp=True)

            @dataset
            class SessionDS:
                # docsnip-highlight start
                # schema doesn't contain a key field of type Window
                uid: int = field(key=True)
                # docsnip-highlight end
                timestamp: datetime = field(timestamp=True)

                @pipeline
                @inputs(Transaction)
                def window_pipeline(cls, ds: Dataset):
                    # docsnip-highlight start
                    return ds.groupby("uid", window=Session("15m")).aggregate()
                    # docsnip-highlight end

            # /docsnip
            client.commit(datasets=[Transaction, SessionDS], message="msg")

    @mock
    def test_invalid_window(self, client):
        with pytest.raises(
            ValueError,
            match="'forever' is not a valid duration value for Tumbling window.",
        ):
            # docsnip invalid_window
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook
            from fennel.dtypes import Window, Tumbling

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                amount: int
                timestamp: datetime = field(timestamp=True)

            @dataset
            class SessionDS:
                uid: int = field(key=True)
                window: Window = field(key=True)
                timestamp: datetime = field(timestamp=True)

                @pipeline
                @inputs(Transaction)
                def window_pipeline(cls, ds: Dataset):
                    return ds.groupby(
                        "uid", window=Tumbling("forever")
                    ).aggregate()

            # /docsnip
            client.commit(datasets=[Transaction, SessionDS], message="msg")
