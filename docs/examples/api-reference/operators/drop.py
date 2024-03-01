import unittest
from datetime import datetime

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
        from fennel.sources import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("User"), disorder="14d", cdc="append")
        @dataset
        class User:
            uid: int = field(key=True)
            # docsnip-highlight start
            city: str
            country: str
            weight: float
            height: float
            # docsnip-highlight end
            gender: str
            timestamp: datetime

        @dataset
        class Dropped:
            uid: int = field(key=True)
            gender: str
            timestamp: datetime

            @pipeline
            @inputs(User)
            def drop_pipeline(cls, user: Dataset):
                # docsnip-highlight start
                return user.drop("height", "weight").drop(
                    columns=["city", "country"]
                )
                # docsnip-highlight end

        # /docsnip

        client.commit(datasets=[User, Dropped])
        # log some rows
        client.log(
            "webhook",
            "User",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "city": "London",
                        "country": "UK",
                        "weight": 150,
                        "height": 63,
                        "gender": "M",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "city": "San Francisco",
                        "country": "US",
                        "weight": 140,
                        "height": 60,
                        "gender": "F",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "city": "New York",
                        "country": "US",
                        "weight": 160,
                        "height": 58,
                        "gender": "M",
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
        df, found = Dropped.lookup(ts, uid=pd.Series([1, 2, 3]))
        assert found.tolist() == [True, True, True]
        assert df["uid"].tolist() == [1, 2, 3]
        assert df["gender"].tolist() == ["M", "F", "M"]
        assert df["timestamp"].tolist()[1:] == [
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 1, 1, 0, 0, 0),
        ]

    @mock
    def test_invalid_drop_key_or_timestamp(self, client):
        with pytest.raises(Exception):
            # docsnip incorrect_type
            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                city: str
                timestamp: datetime

            @dataset
            class Dropped:
                city: str
                timestamp: datetime

                @pipeline
                @inputs(User)
                def pipeline(cls, user: Dataset):
                    return user.drop("uid")

            # /docsnip

    @mock
    def test_missing_column(self, client):
        with pytest.raises(Exception):
            # docsnip missing_column
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.sources import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                city: str
                timestamp: datetime

            @dataset
            class Dropped:
                uid: int = field(key=True)
                city: str
                timestamp: datetime

                @pipeline
                @inputs(User)
                def bad_pipeline(cls, user: Dataset):
                    # docsnip-highlight next-line
                    return user.drop("random")

            # /docsnip
