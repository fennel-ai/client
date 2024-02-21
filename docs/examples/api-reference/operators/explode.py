import pytest
from typing import List, Optional
import unittest
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.schema import inputs
from fennel.sources import source, Webhook
from fennel.test_lib import mock

webhook = Webhook(name="webhook")
__owner__ = "nikhil@fennel.ai"


class TestExplodeSnips(unittest.TestCase):
    @pytest.mark.skip("Unskip after fixing explode on mock client")
    @mock
    def test_basic(self, client):
        # docsnip basic
        @source(webhook.endpoint("Orders"))
        @dataset
        class Orders:
            uid: int
            skus: List[int]
            prices: List[float]
            timestamp: datetime

        @dataset
        class Derived:
            uid: int
            sku: Optional[int]
            price: Optional[float]
            timestamp: datetime

            @pipeline
            @inputs(Orders)
            def pipeline(cls, ds: Dataset):
                return ds.explode("skus", "prices").rename(
                    {"skus": "sku", "prices": "price"}
                )

        # /docsnip

        client.commit(datasets=[Orders, Derived])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "Orders",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "skus": [1, 2],
                        "prices": [10.1, 20.0],
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "skus": [],
                        "prices": [],
                        "timestamp": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        df = client.get_dataset_df("Derived")
        assert df["uid"].tolist() == [1, 1, 2]
        assert df["sku"].tolist() == [1, 2, None]
        assert df["price"].tolist() == [10.1, 20.0, None]
        assert df["timestamp"].tolist() == [
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 1, 1, 0, 0, 0),
        ]

    @mock
    def test_invalid_type(self, client):
        with pytest.raises(Exception):
            # docsnip exploding_non_list
            @source(webhook.endpoint("Orders"))
            @dataset
            class Orders:
                uid: int
                price: float
                timestamp: datetime

            @dataset
            class Derived:
                uid: int
                price: float
                timestamp: datetime

                @pipeline
                @inputs(Orders)
                def pipeline(cls, ds: Dataset):
                    return ds.explode("price")

            # /docsnip

    @mock
    def test_explode_missing_column(self, client):
        with pytest.raises(Exception):
            # docsnip exploding_missing
            @source(webhook.endpoint("Orders"))
            @dataset
            class Orders:
                uid: int
                price: List[float]
                timestamp: datetime

            @dataset
            class Derived:
                uid: int
                price: float
                timestamp: datetime

                @pipeline
                @inputs(Orders)
                def pipeline(cls, ds: Dataset):
                    return ds.explode("price", "random")

            # /docsnip
