from datetime import datetime
import sys

sys.path.insert(0, "/Users/adityanambiar/fennel-ai/client")

from fennel.sources import source, S3
from fennel.datasets import dataset, field, Dataset, pipeline
from fennel.sources import sources, Snowflake, Kinesis
from fennel.lib.aggregate import Count, Sum
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.lib.window import Window
from fennel.featuresets import featureset, extractor, feature
import pandas as pd
from fennel.test_lib import MockClient

snowflake = sources.Snowflake(
    name="snowflake_src",
    account="nhb38793.us-west-2.snowflakecomputing.com",
    warehouse="TEST",
    schema="PUBLIC",
    db_name="TEST_DB",
    src_schema="PUBLIC",
    role="ACCOUNTADMIN",
    username="<username>",
    password="<password>",
)

kinesis = Kinesis(
    name="kinesis_src",
    role_arn="arn:aws:iam::852537873043:user/users/camelia.hssaine",
)
stream = kinesis.stream(
    stream_arn="arn:aws:kinesis:us-east-1:852537873043:stream/json-pipeline-page_view",
    init_position="latest",
    format="json",
)

__owner__ = "blackcrow@fennel.ai"


@source(
    snowflake.table(table_name="user_dataset", cursor="update_time"),
    every="24h",
)
@dataset
class PageViewsSnowFlake:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: str
    traffic_source: int


@source(
    snowflake.table(table_name="user_dataset", cursor="update_time"),
    every="24h",
)
@dataset
class PageViewsKinesis:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: str
    traffic_source: int


@dataset
class PageViewsUnion:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: str
    traffic_source: int

    @pipeline(version=1)
    @inputs(PageViewsSnowFlake, PageViewsKinesis)
    def union(cls, page_views_snow_flake: Dataset, page_views_kinesis: Dataset):
        a = page_views_snow_flake.filter(
            lambda df: df["timestamp"] > "2021-01-01"
        )
        b = page_views_kinesis.filter(lambda df: df["timestamp"] > "2021-01-01")
        return a + b


@meta(owner="xiao@fennel.ai")
@dataset
class PageViewsByDocument:
    document_id: int = field(key=True)
    page_views: int
    page_views_1d: int
    page_views_3d: int
    page_views_7d: int
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(PageViewsUnion)
    def group_by_document(cls, page_views: Dataset):
        return page_views.groupby("document_id").aggregate(
            [
                Count(window="28d", into_field="page_views"),
                Count(window="1d", into_field="page_views_1d"),
                Count(window="3d", into_field="page_views_3d"),
                Count(window="7d", into_field="page_views_7d"),
            ]
        )


@dataset
class PageViewsByUser:
    uuid: str = field(key=True)
    page_views: int
    page_views_1d: int
    page_views_3d: int
    page_views_7d: int
    timestamp: datetime = field(timestamp=True)

    @pipeline(version=1)
    @inputs(PageViewsUnion)
    def group_by_user(cls, page_views: Dataset):
        return page_views.groupby("uuid").aggregate(
            [
                Count(window="28d", into_field="page_views"),
                Count(window="1d", into_field="page_views_1d"),
                Count(window="3d", into_field="page_views_3d"),
                Count(window="7d", into_field="page_views_7d"),
            ]
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
    page_views_7d: int = feature(id=4)

    @extractor(depends_on=[PageViewsByUser], version=2)
    @inputs(Request.uuid)
    @outputs(page_views, page_views_1d, page_views_3d, page_views_7d)
    def extract(cls, ts: pd.Series, uuids: pd.Series):
        page_views, _ = PageViewsByUser.lookup(ts, uuid=uuids)
        ret = page_views[
            ["page_views", "page_views_1d", "page_views_3d", "page_views_7d"]
        ]
        ret = ret.fillna(0)
        ret["page_views"] = ret["page_views"].astype(int)
        ret["page_views_1d"] = ret["page_views_1d"].astype(int)
        ret["page_views_3d"] = ret["page_views_3d"].astype(int)
        ret["page_views_7d"] = ret["page_views_7d"].astype(int)
        return ret


client = MockClient()
client.sync(
    datasets=[
        PageViewsSnowFlake,
        PageViewsKinesis,
        PageViewsUnion,
        PageViewsByDocument,
        PageViewsByUser,
    ],
    featuresets=[Request, UserPageViewFeatures],
)
