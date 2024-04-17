import unittest
from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestFilterSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
        @dataset
        class User:
            uid: int = field(key=True)
            city: str
            signup_time: datetime

        @dataset(index=True)
        class Filtered:
            uid: int = field(key=True)
            city: str
            signup_time: datetime

            @pipeline
            @inputs(User)
            def my_pipeline(cls, user: Dataset):
                return user.filter(lambda df: df["city"] != "London")

        # /docsnip

        client.commit(message="some msg", datasets=[User, Filtered])
        # log some rows to the transaction dataset
        client.log(
            "webhook",
            "User",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "city": "London",
                        "signup_time": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "city": "San Francisco",
                        "signup_time": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "city": "New York",
                        "signup_time": "2021-01-01T00:00:00",
                    },
                ]
            ),
        )
        # do lookup on the WithSquare dataset
        ts = pd.Series(
            [
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
                datetime(2021, 1, 1, 0, 0, 0),
            ]
        )
        df, found = Filtered.lookup(ts, uid=pd.Series([1, 2, 3]))
        assert found.tolist() == [False, True, True]
        assert df["uid"].tolist()[1:] == [2, 3]
        assert df["city"].tolist()[1:] == ["San Francisco", "New York"]
        assert df["signup_time"].tolist()[1:] == [
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ]

    @mock
    def test_invalid_type(self, client):
        # docsnip incorrect_type
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
        @dataset
        class User:
            uid: int = field(key=True)
            city: str
            signup_time: datetime

        @dataset
        class Filtered:
            uid: int = field(key=True)
            city: str
            signup_time: datetime

            @pipeline
            @inputs(User)
            def my_pipeline(cls, user: Dataset):
                return user.filter(lambda df: df["city"] + "London")

        # /docsnip

        client.commit(message="msg", datasets=[User, Filtered])
        with pytest.raises(Exception):
            client.log(
                "webhook",
                "User",
                pd.DataFrame(
                    [
                        {
                            "uid": 1,
                            "city": "London",
                            "signup_time": "2021-01-01T00:00:00",
                        },
                    ]
                ),
            )
            Filtered.lookup(
                pd.Series([datetime(2021, 1, 1, 0, 0, 0)]), uid=pd.Series([1])
            )
