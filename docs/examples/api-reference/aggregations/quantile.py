import pytest
import unittest
from datetime import datetime
from typing import Optional

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestQuantileSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset, Quantile
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
            # new float fields added to the dataset by the quantile aggregation
            median_amount_1w: float
            # docsnip-highlight end
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def quantil_pipeline(cls, ds: Dataset):
                # docsnip-highlight start
                return ds.groupby("uid").aggregate(
                    Quantile(
                        of="amount",
                        window="1w",
                        into_field="median_amount_1w",
                        p=0.5,
                        approx=True,
                        default=0.0,
                    ),
                )
                # docsnip-highlight end

        # /docsnip
        client.commit(message="msg", datasets=[Transaction, Aggregated])
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
        assert df["median_amount_1w"].tolist() == [20, 40, 60]

    @mock
    def test_invalid_type(self, client):
        with pytest.raises(
            Exception, match="Cannot get quantile of field amount of type str"
        ):
            # docsnip incorrect_type
            from fennel.datasets import dataset, field, pipeline
            from fennel.datasets import Dataset, Quantile
            from fennel.lib import inputs
            from fennel.sources import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                # docsnip-highlight next-line
                amount: str
                timestamp: datetime

            @dataset
            class Aggregated:
                uid: int = field(key=True)
                median_amount_1w: float
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def bad_pipeline(cls, ds: Dataset):
                    return ds.groupby("uid").aggregate(
                        Quantile(
                            of="amount",
                            window="1w",
                            into_field="median_amount_1w",
                            p=0.5,
                            approx=True,
                            default=0.0,
                        ),
                    )

            # /docsnip
            client.commit(message="msg", datasets=[Transaction, Aggregated])

    @mock
    def test_invalid_default(self, client):
        with pytest.raises(Exception):
            # docsnip invalid_default
            from fennel.datasets import dataset, field, pipeline
            from fennel.datasets import Dataset, Quantile
            from fennel.lib import inputs
            from fennel.sources import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                amount: int
                vendor: str
                timestamp: datetime

            @dataset
            class Aggregated:
                uid: int = field(key=True)
                # docsnip-highlight next-line
                median_amount_1w: float
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def bad_pipeline(cls, ds: Dataset):
                    return ds.groupby("uid").aggregate(
                        Quantile(
                            of="amount",
                            window="1w",
                            into_field="median_amount_1w",
                            p=0.5,
                            approx=True,
                        ),
                    )

            # /docsnip
            client.commit(message="msg", datasets=[Transaction, Aggregated])

    @mock
    def test_invalid_p(self, client):
        with pytest.raises(
            Exception, match="Expect p value between 0 and 1, found 10.0"
        ):
            # docsnip incorrect_p
            from fennel.datasets import dataset, field, pipeline
            from fennel.datasets import Dataset, Quantile
            from fennel.lib import inputs
            from fennel.sources import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("txn"), disorder="14d", cdc="append")
            @dataset
            class Transaction:
                uid: int
                amount: int
                vendor: str
                timestamp: datetime

            @dataset
            class Aggregated:
                uid: int = field(key=True)
                median_amount_1w: Optional[float]
                timestamp: datetime

                @pipeline
                @inputs(Transaction)
                def bad_pipeline(cls, ds: Dataset):
                    return ds.groupby("uid").aggregate(
                        Quantile(
                            of="amount",
                            window="1w",
                            into_field="median_amount_1w",
                            # docsnip-highlight next-line
                            p=10.0,
                            approx=True,
                        ),
                    )

            # /docsnip
            client.commit(message="msg", datasets=[Transaction, Aggregated])
