from datetime import datetime
from typing import List

import pandas as pd
from google.protobuf.json_format import ParseDict  # type: ignore

from fennel.datasets import dataset, pipeline, field, Dataset, Sum
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import meta, inputs, outputs
from fennel.sources import source, Webhook
from fennel.testing import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="abhay@fennel.ai")
@source(
    webhook.endpoint("MovieInfo"), cdc="append", disorder="14d", tier="prod"
)
@source(
    webhook.endpoint("MovieInfo2"), cdc="append", disorder="14d", tier="staging"
)
@dataset
class MovieInfo:
    title: str = field(key=True)
    actors: List[str]  # can be an empty list
    release: datetime


@meta(owner="abhay@fennel.ai")
@source(
    webhook.endpoint("TicketSale"), disorder="14d", cdc="append", tier="prod"
)
@source(
    webhook.endpoint("TicketSale2"),
    disorder="14d",
    cdc="append",
    tier="staging",
)
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

    @pipeline(tier="prod")
    @inputs(MovieInfo, TicketSale)
    def pipeline_join(cls, info: Dataset, sale: Dataset):
        c = (
            sale.join(info, how="inner", on=["title"])
            .explode(columns=["actors"])
            .rename(columns={"actors": "name"})
        )
        c = c.dropnull()
        return c.groupby("name").aggregate(
            [
                Sum(
                    window="forever",
                    of="price",
                    into_field="revenue",
                ),
            ]
        )

    @pipeline(tier="staging")
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
        c = c.dropnull()
        return c.groupby("name").aggregate(
            [
                Sum(
                    window="forever",
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

    sync_request = view._get_sync_request_proto(tier="dev")
    assert len(sync_request.feature_sets) == 2
    assert len(sync_request.features) == 2
    assert len(sync_request.datasets) == 3
    assert len(sync_request.sources) == 0
    assert len(sync_request.pipelines) == 0
    assert len(sync_request.extractors) == 0
