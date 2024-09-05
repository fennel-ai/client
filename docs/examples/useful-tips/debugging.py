import unittest
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from fennel.testing import mock
from fennel.expr import lit

__owner__ = "aditya@fennel.ai"


class TestDebugSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib.schema import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        # docsnip basic
        @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
        @dataset
        class User:
            uid: int = field(key=True)
            city: str
            signup_time: datetime

        @dataset(index=True)
        class Processed:
            uid: int = field(key=True)
            city: str
            country: str
            signup_time: datetime

            @pipeline
            @inputs(User)
            def my_pipeline(cls, user: Dataset):
                ds = user.filter(lambda df: df["city"] != "London")
                # docsnip-highlight start
                schema = ds.schema()
                print(schema)
                # docsnip-highlight end
                return ds.assign(country=lit("US").astype(str))

        # /docsnip

        client.commit(message="msg", datasets=[User, Processed])
        # log some rows to the dataset
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
        df, found = Processed.lookup(ts, uid=pd.Series([1, 2, 3]))
        assert found.tolist() == [False, True, True]
        assert df["uid"].tolist()[1:] == [2, 3]
        assert df["city"].tolist()[1:] == ["San Francisco", "New York"]
        assert df["signup_time"].tolist()[1:] == [
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            datetime(2021, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        ]
        assert df["country"].tolist()[1:] == ["US", "US"]

    @mock
    def test_print_dataset(self, client):

        # docsnip print_dataset
        from fennel.datasets import dataset, field, pipeline, Dataset
        from fennel.lib.schema import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
        @dataset
        class User:
            uid: int = field(key=True)
            country: str
            signup_time: datetime

        @dataset
        class USUsers:
            uid: int = field(key=True)
            country: str
            signup_time: datetime

            @pipeline
            @inputs(User)
            def my_pipeline(cls, user: Dataset):
                return user.filter(lambda df: df["country"] == "US")

        client.commit(message="msg", datasets=[User, USUsers])
        # log some rows to the dataset
        client.log(
            "webhook",
            "User",
            pd.DataFrame(
                columns=["uid", "country", "signup_time"],
                data=[
                    [1, "UK", "2021-01-01T00:00:00"],
                    [2, "US", "2021-02-01T00:00:00"],
                    [3, "US", "2021-03-01T00:00:00"],
                ],
            ),
        )
        df = client.get_dataset_df("USUsers")
        print(df)
        # /docsnip
        assert df["uid"].tolist() == [2, 3]
        assert df["country"].tolist() == ["US", "US"]
        assert df["signup_time"].tolist() == [
            pd.Timestamp("2021-02-01T00:00:00", tzinfo=timezone.utc),
            pd.Timestamp("2021-03-01T00:00:00", tzinfo=timezone.utc),
        ]
        assert df.shape == (2, 3)

    @mock
    def test_astype(self, client):
        from fennel.datasets import dataset, field
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        # docsnip astype
        @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
        @dataset
        class User:
            uid: int = field(key=True)
            height_cm: Optional[float]
            signup_time: datetime

        client.commit(message="msg", datasets=[User])
        # log some rows to the dataset
        df = pd.DataFrame(
            columns=["uid", "height_cm", "signup_time"],
            data=[
                [1, 180, "2021-01-01T00:00:00"],
                [2, 175, "2021-01-01T00:00:00"],
                [3, None, "2021-01-01T00:00:00"],
            ],
        )
        # docsnip-highlight next-line
        df["height_cm"] = df["height_cm"].astype("Int64")
        client.log("webhook", "User", df)
        # /docsnip
