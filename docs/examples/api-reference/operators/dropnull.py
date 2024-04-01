import pytest
from typing import Optional
import unittest
from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


class TestDropnullSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        # docsnip basic
        from fennel.datasets import dataset, field, pipeline, Dataset, index
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("User"), disorder="14d", cdc="append")
        @dataset
        class User:
            uid: int = field(key=True)
            dob: str
            # docsnip-highlight start
            city: Optional[str]
            country: Optional[str]
            # docsnip-highlight end
            gender: Optional[str]
            timestamp: datetime

        @index
        @dataset
        class Derived:
            uid: int = field(key=True)
            dob: str
            # docsnip-highlight start
            # dropnull changes the type of the columns to non-optional
            city: str
            country: str
            # docsnip-highlight end
            gender: Optional[str]
            timestamp: datetime

            @pipeline
            @inputs(User)
            def dropnull_pipeline(cls, user: Dataset):
                # docsnip-highlight next-line
                return user.dropnull("city", "country")

        # /docsnip

        client.commit(message="msg", datasets=[User, Derived])
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
        from fennel.datasets import dataset, field, pipeline, Dataset, index
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("User"), disorder="14d", cdc="append")
        @dataset
        class User:
            uid: int = field(key=True)
            dob: str
            # docsnip-highlight start
            city: Optional[str]
            country: Optional[str]
            gender: Optional[str]
            # docsnip-highlight end
            timestamp: datetime

        @index
        @dataset
        class Derived:
            uid: int = field(key=True)
            dob: str
            # docsnip-highlight start
            # dropnull changes the type of all optional columns to non-optional
            city: str
            country: str
            gender: str
            # docsnip-highlight end
            timestamp: datetime

            @pipeline
            @inputs(User)
            def dropnull_pipeline(cls, user: Dataset):
                # docsnip-highlight next-line
                return user.dropnull()

        # /docsnip

        client.commit(message="msg", datasets=[User, Derived])
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
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

            webhook = Webhook(name="webhook")

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
                def bad_pipeline(cls, user: Dataset):
                    # docsnip-highlight next-line
                    return user.select("random")

            # /docsnip

    @mock
    def test_non_optional_column(self, client):
        with pytest.raises(Exception):
            # docsnip non_optional_column
            from fennel.datasets import dataset, field, pipeline, Dataset
            from fennel.lib import inputs
            from fennel.connectors import source, Webhook

            webhook = Webhook(name="webhook")

            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                # docsnip-highlight start
                # dropnull can only be used on optional columns
                city: str
                # docsnip-highlight end
                timestamp: datetime

            @dataset
            class Derived:
                uid: int = field(key=True)
                timestamp: datetime

                @pipeline
                @inputs(User)
                def bad_pipeline(cls, user: Dataset):
                    # docsnip-highlight next-line
                    return user.select("city")

            # /docsnip
