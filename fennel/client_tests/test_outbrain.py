from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field, pipeline, Dataset, Count
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import meta, inputs, outputs
from fennel.sources import S3, Webhook
from fennel.sources import source
from fennel.testing import mock

s3 = S3(
    name="outbrain_source",
    aws_access_key_id="AKIA3EK5WO5OPRXWZE5V",
    aws_secret_access_key="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",
)

webhook = Webhook(name="outbrain_webhook")


@source(
    s3.bucket("fennel-demo-data", prefix="outbrain/page_views_filter.csv"),
    every="1d",
    cdc="append",
    disorder="14d",
    tier="prod",
)
@source(webhook.endpoint("PageViews"), disorder="14d", cdc="append", tier="dev")
@meta(owner="xiao@fennel.ai")
@dataset
class PageViews:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: str
    traffic_source: int


@meta(owner="xiao@fennel.ai")
@dataset
class PageViewsByUser:
    uuid: str = field(key=True)
    page_views: int
    page_views_1d: int
    page_views_3d: int
    page_views_9d: int
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(PageViews)
    def group_by_user(cls, page_views: Dataset):
        return (
            page_views.dedup(by=["uuid", "document_id"])
            .groupby("uuid")
            .aggregate(
                [
                    Count(window="28d", into_field="page_views"),
                    Count(window="1d", into_field="page_views_1d"),
                    Count(window="3d", into_field="page_views_3d"),
                    Count(window="9d", into_field="page_views_9d"),
                ]
            )
        )


@meta(owner="aditya@fennel.ai")
@featureset
class Request:
    uuid: str = feature(id=1)
    document_id: int = feature(id=2)


@meta(owner="aditya@fennel.ai")
@featureset
class UserPageViewFeatures:
    page_views: int = feature(id=1)
    page_views_1d: int = feature(id=2)
    page_views_3d: int = feature(id=3)
    page_views_9d: int = feature(id=4)

    @extractor(depends_on=[PageViewsByUser], version=2)
    @inputs(Request.uuid)
    @outputs(page_views, page_views_1d, page_views_3d, page_views_9d)
    def extract(cls, ts: pd.Series, uuids: pd.Series):
        page_views, found = PageViewsByUser.lookup(  # type: ignore
            ts, uuid=uuids
        )
        ret = page_views[
            ["page_views", "page_views_1d", "page_views_3d", "page_views_9d"]
        ]
        ret = ret.fillna(0)
        ret["page_views"] = ret["page_views"].astype(int)
        ret["page_views_1d"] = ret["page_views_1d"].astype(int)
        ret["page_views_3d"] = ret["page_views_3d"].astype(int)
        ret["page_views_9d"] = ret["page_views_9d"].astype(int)
        return ret


@pytest.mark.integration
@mock
def test_outbrain(client):
    client.commit(
        message="initial commit",
        datasets=[
            PageViews,
            PageViewsByUser,
        ],
        featuresets=[
            Request,
            UserPageViewFeatures,
        ],
        tier="dev",
    )
    df = pd.read_csv("fennel/client_tests/data/page_views_sample.csv")
    # Current time in ms
    cur_time_ms = datetime.now().timestamp() * 1000
    max_ts = max(df["timestamp"].tolist())
    # Shift all the data such that the most recent data point has timestamp = cur_time_ms
    df["timestamp"] = df["timestamp"] + cur_time_ms - max_ts
    # Filter out data that is more than 12 days old.
    # TODO: Remove this once watermarks are correctly implemented
    twelve_days = 12 * 24 * 60 * 60 * 1000
    df = df[df["timestamp"] > cur_time_ms - twelve_days]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

    client.log("outbrain_webhook", "PageViews", df)
    if client.is_integration_client():
        client.sleep(120)

    input_df = df.copy()
    input_df = input_df[["uuid", "document_id"]]
    input_df.rename(
        columns={
            "uuid": "Request.uuid",
            "document_id": "Request.document_id",
        },
        inplace=True,
    )
    input_df = input_df.reset_index(drop=True)
    feature_df = client.query(
        outputs=[
            Request,
            UserPageViewFeatures,
        ],
        inputs=[Request.uuid, Request.document_id],
        input_dataframe=input_df,
    )
    assert feature_df.shape[0] == 347
    # NOTE: It is possible that based on the time of execution of the test, the integration mode
    # could fail due to undercounting at the beginning of the configured window.
    assert (
        feature_df["UserPageViewFeatures.page_views"].sum(),
        feature_df["UserPageViewFeatures.page_views_1d"].sum(),
        feature_df["UserPageViewFeatures.page_views_3d"].sum(),
        feature_df["UserPageViewFeatures.page_views_9d"].sum(),
    ) == (11975, 1140, 3840, 9544)
