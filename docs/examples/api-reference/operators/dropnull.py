import pytest
from typing import Optional
import unittest
from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib import inputs
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="webhook")
__owner__ = "aditya@fennel.ai"


class TestDropnullSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        @source(webhook.endpoint("User"))
        @dataset
        class User:
            uid: int = field(key=True)
            dob: str
            city: Optional[str]
            country: Optional[str]
            gender: Optional[str]
            timestamp: datetime

        @dataset
        class Derived:
            uid: int = field(key=True)
            dob: str
            city: str
            country: str
            gender: Optional[str]
            timestamp: datetime

            @pipeline
            @inputs(User)
            def pipeline(cls, user: Dataset):
                return user.dropnull("city", "country")

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
                        "dob": "1990-01-01",
                        "city": "London",
                        "gender": "M",
                        "country": "UK",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "dob": "1990-01-01",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "dob": "1990-01-01",
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
        assert found.tolist() == [True, False, False]
        assert df["uid"].tolist()[0] == 1
        assert df["dob"].tolist()[0] == "1990-01-01"
        assert df["gender"].tolist()[0] == "M"
        assert df["timestamp"].tolist()[0] == datetime(2021, 1, 1, 0, 0, 0)

    @mock
    def test_dropnull_all(self, client):
        # docsnip dropnull_all
        @source(webhook.endpoint("User"))
        @dataset
        class User:
            uid: int = field(key=True)
            dob: str
            city: Optional[str]
            country: Optional[str]
            gender: Optional[str]
            timestamp: datetime

        @dataset
        class Derived:
            uid: int = field(key=True)
            city: str
            country: str
            gender: str
            dob: str
            timestamp: datetime

            @pipeline
            @inputs(User)
            def pipeline(cls, user: Dataset):
                return user.dropnull()

        # /docsnip

        client.commit(datasets=[User, Derived])
        # log some rows
        # log some rows
        client.log(
            "webhook",
            "User",
            pd.DataFrame(
                [
                    {
                        "uid": 1,
                        "dob": "1990-01-01",
                        "city": "London",
                        "gender": "M",
                        "country": "UK",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 2,
                        "dob": "1990-01-01",
                        "timestamp": "2021-01-01T00:00:00",
                    },
                    {
                        "uid": 3,
                        "dob": "1990-01-01",
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
        assert found.tolist() == [True, False, False]
        assert df["uid"].tolist()[0] == 1
        assert df["dob"].tolist()[0] == "1990-01-01"
        assert df["gender"].tolist()[0] == "M"
        assert df["timestamp"].tolist()[0] == datetime(2021, 1, 1, 0, 0, 0)

    @mock
    def test_missing_column(self, client):
        with pytest.raises(Exception):
            # docsnip missing_column
            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                city: Optional[str]
                timestamp: datetime

            @dataset
            class Derived:
                uid: int = field(key=True)
                city: str
                timestamp: datetime

                @pipeline
                @inputs(User)
                def pipeline(cls, user: Dataset):
                    return user.select("random")

            # /docsnip

    @mock
    def test_non_optional_column(self, client):
        with pytest.raises(Exception):
            # docsnip non_optional_column
            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                city: str
                timestamp: datetime

            @dataset
            class Derived:
                uid: int = field(key=True)
                timestamp: datetime

                @pipeline
                @inputs(User)
                def pipeline(cls, user: Dataset):
                    return user.select("city")

            # /docsnip
