from fennel import Sum
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.schema import outputs
from datetime import datetime

import pandas as pd
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import List

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs
from fennel.lib.window import Window
from fennel.sources import source, Webhook
from fennel.test_lib import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="abhay@fennel.ai")
@source(webhook.endpoint("MovieInfo"), tier="prod")
@source(webhook.endpoint("MovieInfo2"), tier="staging")
@dataset
class MovieInfo:
    title: str = field(key=True)
    actors: List[str]  # can be an empty list
    release: datetime


@meta(owner="abhay@fennel.ai")
@source(webhook.endpoint("TicketSale"), tier="prod")
@source(webhook.endpoint("TicketSale2"), tier="staging")
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

    @pipeline(version=1, tier="prod")
    @inputs(MovieInfo, TicketSale)
    def pipeline_join(cls, info: Dataset, sale: Dataset):
        uniq = sale.groupby("ticket_id").first()
        c = (
            uniq.join(info, how="inner", on=["title"])
            .explode(columns=["actors"])
            .rename(columns={"actors": "name"})
        )
        c = c.dropnull()
        return c.groupby("name").aggregate(
            [
                Sum(
                    window=Window("forever"),
                    of="price",
                    into_field="revenue",
                ),
            ]
        )

    @pipeline(version=2, active=True, tier="staging")
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
        c = c.dropnull()
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

    @extractor(depends_on=[ActorStats], tier="prod")
    @inputs(RequestFeatures.name)
    @outputs(revenue)
    def extract_revenue(cls, ts: pd.Series, name: pd.Series):
        df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
        df = df.fillna(0)
        return df["revenue"]

    @extractor(depends_on=[ActorStats], tier="staging")
    @inputs(RequestFeatures.name)
    @outputs(revenue)
    def extract_revenue2(cls, ts: pd.Series, name: pd.Series):
        df, _ = ActorStats.lookup(ts, name=name)  # type: ignore
        df = df.fillna(0)
        return df["revenue"] * 2


def test_tier_selector():
    view = InternalTestClient()
    view.add(MovieInfo)
    view.add(TicketSale)
    view.add(ActorStats)
    view.add(RequestFeatures)
    view.add(ActorFeatures)

    sync_request = view._get_sync_request_proto("dev")
    assert len(sync_request.feature_sets) == 2
    assert len(sync_request.features) == 2
    assert len(sync_request.datasets) == 3
    assert len(sync_request.sources) == 0
    assert len(sync_request.pipelines) == 0
    assert len(sync_request.extractors) == 0
