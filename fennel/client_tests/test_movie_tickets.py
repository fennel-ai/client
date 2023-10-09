import unittest
from datetime import datetime, timedelta

import pandas as pd

import requests

from fennel import featureset, extractor, feature
from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.sources import source
from fennel.datasets import pipeline, Dataset
from fennel.lib.aggregate import Sum
from fennel.lib.window import Window
from fennel.sources import Webhook
from fennel.test_lib import mock, MockClient

from typing import List


client = MockClient()

webhook = Webhook(name="fennel_webhook")


@meta(owner="abhay@fennel.ai")
@source(webhook.endpoint("MovieInfo"))
@dataset
class MovieInfo:
    title: str = field(key=True)
    actors: List[str]  # can be an empty list
    release: datetime


@meta(owner="abhay@fennel.ai")
@source(webhook.endpoint("TicketSale"))
@dataset
class TicketSale:
    ticket_id: str
    title: str
    price: int
    at: datetime


@meta(owner="abhay@fennel.ai")
@dataset
class ActorStats:
    name: str = field(key=True)
    revenue: int
    at: datetime

    @pipeline(version=1)
    @inputs(MovieInfo, TicketSale)
    def pipeline_join(cls, info: Dataset, sale: Dataset):
        uniq = sale.groupby("ticket_id").first()
        c = (
            uniq.join(info, how="inner", on=["title"])
            .explode(columns=["actors"])
            .rename(columns={"actors": "name"})
        )
        # name -> Option[str]
        schema = c.schema()
        schema["name"] = str
        c = c.transform(lambda x: x, schema)
        return c.groupby("name").aggregate(
            [
                Sum(
                    window=Window("forever"),
                    of="price",
                    into_field="revenue",
                ),
            ]
        )

    @pipeline(version=2, active=True)
    @inputs(MovieInfo, TicketSale)
    def pipeline_join_v2(cls, info: Dataset, sale: Dataset):
        def foo(df):
            df["price"] = df["price"] * 2
            return df

        uniq = sale.groupby("ticket_id").first()
        c = (
            uniq.join(info, how="inner", on=["title"])
            .explode(columns=["actors"])
            .rename(columns={"actors": "name"})
        )
        # name -> Option[str]
        schema = c.schema()
        schema["name"] = str
        c = c.transform(foo, schema)
        return c.groupby("name").aggregate(
            [
                Sum(
                    window=Window("forever"),
                    of="price",
                    into_field="revenue",
                ),
            ]
        )


@meta(owner="zaki@fennel.ai")
@featureset
class RequestFeatures:
    name: str = feature(id=1)


@meta(owner="abhay@fennel.ai")
@featureset
class ActorFeatures:
    revenue: int = feature(id=1)

    @extractor(depends_on=[ActorStats])
    @inputs(RequestFeatures.name)
    @outputs(revenue)
    def extract_revenue(cls, ts: pd.Series, name: pd.Series):
        import sys

        print(name, file=sys.stderr)
        print("##", name.name, file=sys.stderr)
        df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
        df = df.fillna(0)
        return df["revenue"]


class TestMovieTicketSale(unittest.TestCase):
    @mock
    def test_movie_ticket_sale(self, client):
        datasets = [MovieInfo, TicketSale, ActorStats]  # type: ignore
        featuresets = [ActorFeatures, RequestFeatures]
        client.sync(datasets=datasets, featuresets=featuresets)  # type: ignore
        client.sleep()
        data = [
            [
                "Titanic",
                ["Leonardo DiCaprio", "Kate Winslet"],
                datetime.strptime("1997-12-19", "%Y-%m-%d"),
            ],
            [
                "Jumanji",
                ["Robin Williams", "Kirsten Dunst"],
                datetime.strptime("1995-12-15", "%Y-%m-%d"),
            ],
            [
                "Great Gatbsy",
                ["Leonardo DiCaprio", "Carey Mulligan"],
                datetime.strptime("2013-05-10", "%Y-%m-%d"),
            ],
        ]
        columns = ["title", "actors", "release"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "MovieInfo", df)
        assert (
            response.status_code == requests.codes.OK
        ), response.json()  # noqa

        now = datetime.now()
        one_hour_ago = now - timedelta(hours=1)
        one_day_ago = now - timedelta(days=1)
        two_hours_ago = now - timedelta(hours=2)
        columns = ["ticket_id", "title", "price", "at"]
        data = [
            ["1", "Titanic", 50, one_hour_ago],
            ["2", "Titanic", 100, one_day_ago],
            ["3", "Jumanji", 25, one_hour_ago],
            ["4", "The Matrix", 50, two_hours_ago],  # no match
            ["5", "Great Gatbsy", 49, one_hour_ago],
        ]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("fennel_webhook", "TicketSale", df)

        client.sleep()
        assert (
            response.status_code == requests.codes.OK
        ), response.json()  # noqa

        features = client.extract_features(
            input_feature_list=[RequestFeatures.name],  # type: ignore
            output_feature_list=[ActorFeatures.revenue],  # type: ignore
            input_dataframe=pd.DataFrame(
                {
                    "RequestFeatures.name": [
                        "Robin Williams",
                        "Leonardo DiCaprio",
                    ],
                }
            ),
        )
        assert features.shape == (2, 1)
        assert features["ActorFeatures.revenue"][0] == 50
        assert features["ActorFeatures.revenue"][1] == 398
