import unittest
from datetime import datetime

import pandas as pd
from fennel.testing import mock
import pytest

__owner__ = "hoang@fennel.ai"


class TestAssignSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import (
            dataset,
            field,
            pipeline,
            Dataset,
        )
        from fennel.lib import inputs
        from fennel.sources import source, Webhook
        from fennel.dtypes import Window

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime = field(timestamp=True)

        @dataset
        class Session:
            # docsnip-highlight start
            # groupby field becomes the key field
            uid: int = field(key=True)
            # new key fields are added to the dataset by the window operation
            # Window now become a key after operator
            window: Window = field(key=True)
            # docsnip-highlight end
            timestamp: datetime = field(timestamp=True)

            @pipeline
            @inputs(Transaction)
            def window_pipeline(cls, ds: Dataset):
                # docsnip-highlight start
                return ds.groupby("uid").window(
                    type="session", gap="15m", into_field="window"
                )
                # docsnip-highlight end

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
                        "begin": pd.Timestamp(datetime(2021, 1, 1, 0, 0, 0)),
                        "end": pd.Timestamp(
                            datetime(2021, 1, 1, 0, 10, 0, microsecond=1)
                        ),
                    },
                    {
                        "begin": pd.Timestamp(datetime(2021, 1, 2, 0, 10, 0)),
                        "end": pd.Timestamp(
                            datetime(2021, 1, 2, 0, 15, 0, microsecond=1)
                        ),
                    },
                ]
            ),
        )
        assert found.tolist() == [True, True]
        assert df["uid"].tolist() == [1, 2]

    @mock
    def test_miss_window_key(self, client):
        with pytest.raises(Exception):
            # docsnip miss_window_key
            from fennel.datasets import (
                dataset,
                field,
                pipeline,
                Dataset,
            )
            from fennel.lib import inputs
            from fennel.sources import source, Webhook
            from fennel.dtypes import Window

            webhook = Webhook(name="webhook")

            @source(
                webhook.endpoint("Transaction"), disorder="14d", cdc="append"
            )
            @dataset
            class Transaction:
                uid: int
                amount: int
                timestamp: datetime = field(timestamp=True)

            @dataset
            class Session:
                # docsnip-highlight start
                # groupby field becomes the key field
                uid: int = field(key=True)
                # docsnip-highlight end
                timestamp: datetime = field(timestamp=True)

                @pipeline
                @inputs(Transaction)
                def window_pipeline(cls, ds: Dataset):
                    # docsnip-highlight start
                    return ds.groupby("uid").window(
                        type="session", gap="15m", into_field="window"
                    )
                    # docsnip-highlight end

            # /docsnip
            client.commit(datasets=[Transaction, Session], message="msg")

    @mock
    def test_invalid_window(self, client):
        with pytest.raises(
            Exception, match="'forever' is not a valid duration value."
        ):
            # docsnip invalid_window
            from fennel.datasets import (
                dataset,
                field,
                pipeline,
                Dataset,
            )
            from fennel.lib import inputs
            from fennel.sources import source, Webhook
            from fennel.dtypes import Window

            webhook = Webhook(name="webhook")

            @source(
                webhook.endpoint("Transaction"), disorder="14d", cdc="append"
            )
            @dataset
            class Transaction:
                uid: int
                amount: int
                timestamp: datetime = field(timestamp=True)

            @dataset
            class Session:
                # groupby field becomes the key field
                uid: int = field(key=True)
                # new key fields are added to the dataset by the window operation
                # Window now become a key after operator
                window: Window = field(key=True)
                timestamp: datetime = field(timestamp=True)

                @pipeline
                @inputs(Transaction)
                def window_pipeline(cls, ds: Dataset):
                    return ds.groupby("uid").window(
                        # docsnip-highlight start
                        type="tumbling",
                        duration="forever",
                        into_field="window",
                        # docsnip-highlight end
                    )

            # /docsnip
            client.commit(datasets=[Transaction, Session], message="msg")
