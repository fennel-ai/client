import unittest
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import pandas as pd
import pytest
import requests

from fennel import featureset, extractor
from fennel.connectors import Webhook
from fennel.connectors import source
from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Dataset,
    Sum,
    LastK,
    Distinct,
)
from fennel.dtypes import Continuous
from fennel.lib import meta, inputs, outputs
from fennel.testing import mock, MockClient

client = MockClient()

webhook = Webhook(name="fennel_webhook")


@meta(owner="abhay@fennel.ai")
@source(webhook.endpoint("MovieInfo"), cdc="upsert", disorder="14d")
@dataset(index=True)
class MovieInfo:
    title: str = field(key=True)
    actors: List[Optional[str]]  # can be an empty list
    release: datetime


@meta(owner="abhay@fennel.ai")
@source(webhook.endpoint("TicketSale"), disorder="14d", cdc="append")
@dataset
class TicketSale:
    ticket_id: str
    title: str
    price: int  # type: ignore
    at: datetime


@meta(owner="abhay@fennel.ai")
@dataset(index=True)
class ActorStats:
    name: str = field(key=True)
    revenue: int  # type: ignore
    at: datetime

    @pipeline(env="staging")
    @inputs(MovieInfo, TicketSale)
    def pipeline_join(cls, info: Dataset, sale: Dataset):
        c = (
            sale.join(info, how="inner", on=["title"])
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
                    window=Continuous("forever"),
                    of="price",
                    into_field="revenue",
                ),
            ]
        )

    @pipeline(env="prod")
    @inputs(MovieInfo, TicketSale)
    def pipeline_join_v2(cls, info: Dataset, sale: Dataset):
        def foo(df):
            df["price"] = df["price"] * 2
            return df

        c = (
            sale.join(info, how="inner", on=["title"])
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
                    window=Continuous("forever"),
                    of="price",
                    into_field="revenue",
                ),
            ]
        )


@meta(owner="abhay@fennel.ai")
@dataset
class ActorStatsList:
    name: str = field(key=True)
    revenue: List[int]  # type: ignore
    revenue_distinct: List[int]  # type: ignore
    at: datetime

    @pipeline(env="prod")
    @inputs(MovieInfo, TicketSale)
    def pipeline_join(cls, info: Dataset, sale: Dataset):
        c = (
            sale.join(info, how="inner", on=["title"])
            .explode(columns=["actors"])
            .rename(columns={"actors": "name"})
        )
        # name -> Option[str]
        schema = c.schema()
        schema["name"] = str
        c = c.transform(lambda x: x, schema)
        return c.groupby("name").aggregate(
            [
                LastK(
                    window=Continuous("forever"),
                    of="price",
                    into_field="revenue",
                    limit=10,
                    dedup=False,
                ),
                Distinct(
                    window=Continuous("forever"),
                    of="price",
                    into_field="revenue_distinct",
                    unordered=True,
                ),
            ]
        )


@meta(owner="zaki@fennel.ai")
@featureset
class RequestFeatures:
    name: str


@meta(owner="abhay@fennel.ai")
@featureset
class ActorFeatures:
    revenue: int

    @extractor(deps=[ActorStats], env="prod")  # type: ignore
    @inputs(RequestFeatures.name)
    @outputs("revenue")
    def extract_revenue(cls, ts: pd.Series, name: pd.Series):
        df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
        df = df.fillna(0)
        return df["revenue"]

    @extractor(deps=[ActorStats], env="staging")  # type: ignore
    @inputs(RequestFeatures.name)
    @outputs("revenue")
    def extract_revenue2(cls, ts: pd.Series, name: pd.Series):
        df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
        df = df.fillna(0)
        return df["revenue"] * 2


def _get_changed_featureset_valid():
    @meta(owner="abhay@fennel.ai")
    @featureset
    class ActorFeatures:
        revenue: int
        twice_revenue: int

        @extractor(deps=[ActorStats], env="prod")  # type: ignore
        @inputs(RequestFeatures.name)
        @outputs("revenue")
        def extract_revenue(cls, ts: pd.Series, name: pd.Series):
            df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
            df = df.fillna(0)
            return df["revenue"]

        @extractor(deps=[ActorStats], env="prod")  # type: ignore
        @inputs(RequestFeatures.name)
        @outputs("twice_revenue")
        def extract_revenue2(cls, ts: pd.Series, name: pd.Series):
            df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
            df = df.fillna(0)
            df["twice_revenue"] = df["revenue"] * 2
            return df["twice_revenue"]

    return ActorFeatures


def _get_changed_featureset_invalid():
    @meta(owner="abhay@fennel.ai")
    @featureset
    class ActorFeatures:
        twice_revenue: int

        @extractor(deps=[ActorStats], env="prod")  # type: ignore
        @inputs(RequestFeatures.name)
        @outputs("twice_revenue")
        def extract_revenue2(cls, ts: pd.Series, name: pd.Series):
            df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
            df = df.fillna(0)
            df["twice_revenue"] = df["revenue"] * 2
            return df["twice_revenue"]

    return ActorFeatures


class TestMovieTicketSale(unittest.TestCase):
    @mock
    def test_movie_ticket_sale(self, client):
        datasets = [MovieInfo, TicketSale, ActorStats, ActorStatsList]  # type: ignore
        featuresets = [ActorFeatures, RequestFeatures]
        client.commit(
            message="initial commit",
            datasets=datasets,
            featuresets=featuresets,
            env="prod",
        )  # type: ignore
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

        now = datetime.now(timezone.utc)
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

        features = client.query(
            inputs=[RequestFeatures.name],  # type: ignore
            outputs=[ActorFeatures.revenue],  # type: ignore
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

        client.commit(
            message="new featureset",
            featuresets=[_get_changed_featureset_valid()],
            incremental=True,
            env="prod",
        )  # type: ignore

        features = client.query(
            inputs=[RequestFeatures.name],  # type: ignore
            outputs=[_get_changed_featureset_valid()],  # type: ignore
            input_dataframe=pd.DataFrame(
                {
                    "RequestFeatures.name": [
                        "Robin Williams",
                        "Leonardo DiCaprio",
                    ],
                }
            ),
        )
        assert features.shape == (2, 2)
        assert features["ActorFeatures.revenue"][0] == 50
        assert features["ActorFeatures.revenue"][1] == 398
        assert features["ActorFeatures.twice_revenue"][0] == 100
        assert features["ActorFeatures.twice_revenue"][1] == 796

        with pytest.raises(Exception) as e:
            client.commit(
                message="new featureset",
                featuresets=[_get_changed_featureset_invalid()],
                incremental=True,
                env="prod",
            )  # type: ignore
        assert (
            str(e.value)
            == "Featureset ActorFeatures is not a superset of the existing featureset"
        )
