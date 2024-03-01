import pytest
import unittest
from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestSumSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset, Sum
        from fennel.lib import inputs
        from fennel.sources import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            # docsnip-highlight start
            # new int fields added to the dataset by the count aggregation
            amount_1w: int
            total: int
            # docsnip-highlight end
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def sum_pipeline(cls, ds: Dataset):
                # docsnip-highlight start
                return ds.groupby("uid").aggregate(
                    Sum(of="amount", window="1w", into_field="amount_1w"),
                    Sum(of="amount", window="forever", into_field="total"),
                )
                # docsnip-highlight end

        # /docsnip
        client.commit(datasets=[Transaction, Aggregated])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "Transaction",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "vendor": "A",
                        "amount": 10,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 1,
                        "vendor": "B",
                        "amount": 20,
                        "timestamp": "2021-01-02T00:00:00",
                    },
                    {
                        "uid": 2,
                        "vendor": "A",
                        "amount": 30,
                        "timestamp": "2021-01-03T00:00:00",
                    },
                    {
                        "uid": 2,
                        "vendor": "B",
                        "amount": 40,
                        "timestamp": "2021-01-04T00:00:00",
                    },
                    {
                        "uid": 3,
                        "vendor": "A",
                        "amount": 50,
                        "timestamp": "2021-01-05T00:00:00",
                    },
                    {
                        "uid": 3,
                        "vendor": "B",
                        "amount": 60,
                        "timestamp": "2021-01-06T00:00:00",
                    },
                ]
            ),
        )

        # do lookup on the Aggregated dataset
        ts = pd.Series(
            [
                datetime(2021, 1, 6, 0, 0, 0),
                datetime(2021, 1, 6, 0, 0, 0),
                datetime(2021, 1, 6, 0, 0, 0),
            ]
        )
        df, found = Aggregated.lookup(ts, uid=pd.Series([1, 2, 3]))
        assert found.tolist() == [True, True, True]
        assert df["uid"].tolist() == [1, 2, 3]
        assert df["amount_1w"].tolist() == [30, 70, 110]
        assert df["total"].tolist() == [30, 70, 110]

    @mock
    def test_invalid_type(self, client):
        with pytest.raises(Exception):
            # docsnip incorrect_type
            from fennel.datasets import dataset, field, pipeline, Dataset, Sum
            from fennel.lib import inputs
            from fennel.sources import source, Webhook

            webhook = Webhook(name="webhook")

            @source(
                webhook.endpoint("Transaction"), disorder="14d", cdc="append"
            )
            @dataset
            class Transaction:
                uid: int
                amount: str
                # docsnip-highlight next-line
                vendor: str
                timestamp: datetime

            @dataset
            class Aggregated:
                uid: int = field(key=True)
                total: int
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def bad_pipeline(cls, ds: Dataset):
                    return ds.groupby("uid").aggregate(
                        # docsnip-highlight next-line
                        Sum(of="vendor", window="forever", into_field="total"),
                    )

            # /docsnip
