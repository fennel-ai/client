import numpy as np
import pytest
from typing import List, Optional
import unittest
from datetime import datetime

import pandas as pd

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


class TestExplodeSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Orders"), disorder="14d", cdc="append")
        @dataset
        class Orders:
            uid: int
            # docsnip-highlight start
            skus: List[int]
            prices: List[float]
            # docsnip-highlight end
            timestamp: datetime

        @dataset
        class Derived:
            uid: int
            # docsnip-highlight start
            sku: Optional[int]
            price: Optional[float]
            # docsnip-highlight end
            timestamp: datetime

            @pipeline
            @inputs(Orders)
            def explode_pipeline(cls, ds: Dataset):
                return (
                    ds
                    # docsnip-highlight next-line
                    .explode("skus", "prices").rename(
                        {"skus": "sku", "prices": "price"}
                    )
                )

        # /docsnip

        client.commit(message="some msg", datasets=[Orders, Derived])
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
        df = df.fillna(np.nan).replace([np.nan], [None])
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
            from fennel.datasets import dataset, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("Orders"), disorder="14d", cdc="append")
            @dataset
            class Orders:
                uid: int
                # docsnip-highlight next-line
                price: float
                timestamp: datetime

            @dataset
            class Derived:
                uid: int
                price: float
                timestamp: datetime

                @pipeline
                @inputs(Orders)
                def bad_pipeline(cls, ds: Dataset):
                    # docsnip-highlight next-line
                    return ds.explode("price")

            # /docsnip

    @mock
    def test_explode_missing_column(self, client):
        with pytest.raises(Exception):
            # docsnip exploding_missing
            from fennel.datasets import dataset, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("Orders"), disorder="14d", cdc="append")
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
                def bad_pipeline(cls, ds: Dataset):
                    # docsnip-highlight next-line
                    return ds.explode("price", "random")

            # /docsnip
