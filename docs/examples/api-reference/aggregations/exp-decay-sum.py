import unittest
from datetime import datetime

import pandas as pd
import pytest

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestExpDecaySumSnips(unittest.TestCase):
    @mock
    def test_exponential_decay(self, client):
        # docsnip basic
        from fennel.datasets import (
            dataset,
            field,
            pipeline,
            Dataset,
            ExpDecaySum,
        )
        from fennel.dtypes import Continuous
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amount: int
            timestamp: datetime

        @dataset(index=True)
        class Aggregated:
            uid: int = field(key=True)
            # docsnip-highlight start
            # new int fields added to the dataset by the aggregation
            amount_1w: float
            total: float
            # docsnip-highlight end
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def exp_decay_sum(cls, ds: Dataset):
                # docsnip-highlight start
                return ds.groupby("uid").aggregate(
                    amount_1w=ExpDecaySum(
                        of="amount", window=Continuous("1w"), half_life="1d"
                    ),
                    total=ExpDecaySum(
                        of="amount",
                        window=Continuous("forever"),
                        half_life="1w",
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
        expected = [1.5625, 13.75, 85.0]
        # Approximate values due to floating point errors
        assert df["amount_1w"].tolist() == pytest.approx(expected, abs=1e-3)
        expected = [19.554, 55.1033, 105.286]
        assert df["total"].tolist() == pytest.approx(expected, abs=1e-3)

    @mock
    def test_invalid_type(self, client):
        with pytest.raises(Exception):
            # docsnip incorrect_type_exp_decay
            from fennel.datasets import (
                dataset,
                field,
                pipeline,
                Dataset,
                ExpDecaySum,
            )
            from fennel.dtypes import Continuous
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

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
                        total=ExpDecaySum(
                            of="amount",
                            window=Continuous("forever"),
                            half_life="1w",
                        ),
                    )

            # /docsnip
