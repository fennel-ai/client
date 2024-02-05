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

class TestSelectSnips(unittest.TestCase):

    @mock
    def test_basic(self, client):
        # docsnip basic
        @source(webhook.endpoint("User"))
        @dataset
        class User:
            uid: int = field(key=True)
            weight: float
            height: float
            city: str
            country: str
            gender: str
            timestamp: datetime

        @dataset
        class Selected:
            uid: int = field(key=True)
            weight: float
            height: float
            timestamp: datetime

            @pipeline(version=1)
            @inputs(User)
            def pipeline(cls, user: Dataset):
                return user.select("uid", "height", "weight")
        # /docsnip

        client.sync(datasets=[User, Selected])
        # log some rows
        client.log("webhook", "User", pd.DataFrame([
            {"uid": 1, "city": "London", "country": "UK", "weight": 150, 
             "height": 63, "gender": "M", "timestamp": "2021-01-01T00:00:00"},
            {"uid": 2, "city": "San Francisco", "country": "US", "weight": 140,
                "height": 60, "gender": "F", "timestamp": "2021-01-01T00:00:00"},
            {"uid": 3, "city": "New York", "country": "US", "weight": 160, 
             "height": 58, "gender": "M", "timestamp": "2021-01-01T00:00:00"}
        ]))
              
        # do lookup on the output dataset
        ts = pd.Series([
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 1, 1, 0, 0, 0),
            datetime(2021, 1, 1, 0, 0, 0),
        ])
        df, found = Selected.lookup(ts, uid=pd.Series([1, 2, 3]))
        assert(found.tolist() == [True, True, True])
        assert(df["uid"].tolist() == [1, 2, 3])
        assert(df["weight"].tolist() == [150, 140, 160])
        assert(df["height"].tolist() == [63, 60, 58])
        assert(df["timestamp"].tolist()[1:] == [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 1, 1, 0, 0, 0)])

    @mock
    def test_invalid_drop_key_or_timestamp(self, client):
        with pytest.raises(Exception):
            # docsnip missing_key
            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                city: str
                timestamp: datetime
            
            @dataset
            class Selected:
                city: str
                timestamp: datetime

                @pipeline(version=1)
                @inputs(User)
                def pipeline(cls, user: Dataset):
                    return user.select("height", "weight")
            # /docsnip

    @mock
    def test_missing_column(self, client):
        with pytest.raises(Exception):
            # docsnip missing_column
            @source(webhook.endpoint("User"))
            @dataset
            class User:
                uid: int = field(key=True)
                city: str
                timestamp: datetime
            
            @dataset
            class Selected:
                uid: int = field(key=True)
                city: str
                timestamp: datetime

                @pipeline(version=1)
                @inputs(User)
                def pipeline(cls, user: Dataset):
                    return user.select("uid", "random")
            # /docsnip