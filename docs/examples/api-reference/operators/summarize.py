import unittest
from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel.testing import mock

__owner__ = "hoang@fennel.ai"


class TestSummarizeSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook
        from fennel.dtypes import Window

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime = field(timestamp=True)

        @dataset(index=True)
        class Session:
            uid: int = field(key=True)
            window: Window = field(key=True)
            # docsnip-highlight start
            # new field added by the summarize operator
            total_amount: int
            # docsnip-highlight end
            timestamp: datetime = field(timestamp=True)

            @pipeline
            @inputs(Transaction)
            def window_pipeline(cls, ds: Dataset):
                return (
                    ds.groupby("uid").window(
                        type="session", gap="15m", into_field="window"
                    )
                    # docsnip-highlight start
                    .summarize(
                        "total_amount",
                        int,
                        lambda df: df["amount"].sum(),
                    )
                    # docsnip-highlight end
                )

        # /docsnip

        client.commit(datasets=[Transaction, Session], message="msg")
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
        df, found = Session.lookup(
            pd.Series(
                [
                    datetime(2021, 1, 1, 0, 10, 0, microsecond=1),
                    datetime(2021, 1, 2, 0, 15, 0, microsecond=1),
                ]
            ),
            uid=pd.Series([1, 2]),
            window=pd.Series(
                [
                    {
                        "begin": pd.Timestamp(
                            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                        ),
                        "end": pd.Timestamp(
                            datetime(
                                2021,
                                1,
                                1,
                                0,
                                10,
                                0,
                                microsecond=1,
                                tzinfo=timezone.utc,
                            )
                        ),
                    },
                    {
                        "begin": pd.Timestamp(
                            datetime(2021, 1, 2, 0, 10, 0, tzinfo=timezone.utc)
                        ),
                        "end": pd.Timestamp(
                            datetime(
                                2021,
                                1,
                                2,
                                0,
                                15,
                                0,
                                microsecond=1,
                                tzinfo=timezone.utc,
                            )
                        ),
                    },
                ]
            ),
        )
        assert found.tolist() == [True, True]
        assert df["uid"].tolist() == [1, 2]
        assert df["total_amount"].tolist() == [30, 70]

    @mock
    def test_type_error(self, client):
        with pytest.raises(Exception):
            # docsnip wrong_type
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook
            from fennel.dtypes import Window

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                amount: int
                timestamp: datetime = field(timestamp=True)

            @dataset
            class Session:
                uid: int = field(key=True)
                window: Window = field(key=True)
                # docsnip-highlight start
                # type 'float' doesn't match the type in summarize below
                total_amount: float
                # docsnip-highlight end
                timestamp: datetime = field(timestamp=True)

                @pipeline
                @inputs(Transaction)
                def window_pipeline(cls, ds: Dataset):
                    return (
                        ds.groupby("uid")
                        .window(type="session", gap="15m", into_field="window")
                        .summarize(
                            "total_amount",
                            # docsnip-highlight start
                            int,
                            # docsnip-highlight end
                            lambda df: df["amount"].sum(),
                        )
                    )

            # /docsnip

            client.commit(datasets=[Transaction, Session], message="msg")

    @mock
    def test_runtime_error(self, client):
        with pytest.raises(Exception):
            # docsnip runtime_error
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook
            from fennel.dtypes import Window

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                amount: int
                timestamp: datetime = field(timestamp=True)

            @dataset
            class Session:
                uid: int = field(key=True)
                window: Window = field(key=True)
                total_amount: str
                timestamp: datetime = field(timestamp=True)

                @pipeline
                @inputs(Transaction)
                def window_pipeline(cls, ds: Dataset):
                    return (
                        ds.groupby("uid")
                        .window(type="session", gap="15m", into_field="window")
                        .summarize(
                            "total_amount",
                            str,
                            # docsnip-highlight start
                            # lambda returns int, not str; hence runtime error
                            lambda df: df["amount"].sum(),
                            # docsnip-highlight end
                        )
                    )

            # /docsnip

            client.commit(datasets=[Transaction, Session], message="msg")
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
            df, found = Session.lookup(
                pd.Series(
                    [
                        datetime(2021, 1, 1, 0, 10, 0, microsecond=1),
                        datetime(2021, 1, 2, 0, 15, 0, microsecond=1),
                    ]
                ),
                uid=pd.Series([1, 2]),
                window=pd.Series(
                    [
                        {
                            "begin": pd.Timestamp(
                                datetime(
                                    2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc
                                )
                            ),
                            "end": pd.Timestamp(
                                datetime(
                                    2021,
                                    1,
                                    1,
                                    0,
                                    10,
                                    0,
                                    microsecond=1,
                                    tzinfo=timezone.utc,
                                )
                            ),
                        },
                        {
                            "begin": pd.Timestamp(
                                datetime(
                                    2021, 1, 2, 0, 10, 0, tzinfo=timezone.utc
                                )
                            ),
                            "end": pd.Timestamp(
                                datetime(
                                    2021,
                                    1,
                                    2,
                                    0,
                                    15,
                                    0,
                                    microsecond=1,
                                    tzinfo=timezone.utc,
                                )
                            ),
                        },
                    ]
                ),
            )
            assert found.tolist() == [True, True]
            assert df["uid"].tolist() == [1, 2]
            assert df["total_amount"].tolist() == [30, 70]
