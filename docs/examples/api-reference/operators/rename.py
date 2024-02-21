import pytest
import unittest
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.schema import inputs
from fennel.sources import source, Webhook
from fennel.test_lib import mock

webhook = Webhook(name="webhook")
__owner__ = "aditya@fennel.ai"


class TestRenameSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        @source(webhook.endpoint("User"))
        @dataset
        class User:
            uid: int = field(key=True)
            weight: float
            height: float
            timestamp: datetime

        @dataset
        class Derived:
            uid: int = field(key=True)
            weight_lb: float
            height_in: float
            timestamp: datetime

            @pipeline
            @inputs(User)
            def pipeline(cls, user: Dataset):
                return user.rename(
                    {"weight": "weight_lb", "height": "height_in"}
                )

        # /docsnip

        client.commit(datasets=[User, Derived])
        # log some rows
        client.log(
            "webhook",
            "User",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "weight": 150,
                        "height": 63,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "weight": 140,
                        "height": 60,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "weight": 160,
                        "height": 58,
                        "timestamp": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )

        # do lookup on the output dataset
        ts = pd.Series(
            [
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
            ]
        )
        df, found = Derived.lookup(ts, uid=pd.Series([1, 2, 3]))
        assert found.tolist() == [True, True, True]
        assert df["uid"].tolist() == [1, 2, 3]
        assert df["weight_lb"].tolist() == [150, 140, 160]
        assert df["height_in"].tolist() == [63, 60, 58]
        assert df["timestamp"].tolist()[1:] == [
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 1, 1, 0, 0, 0),
        ]
