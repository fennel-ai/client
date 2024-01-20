import argparse
from datetime import datetime
import sys
from typing import Optional
from fennel.client.client import Client

sys.path.insert(0, "/Users/adityanambiar/fennel-ai/client")

from fennel.sources import source, S3
from fennel.datasets import dataset, field, Dataset, pipeline
from fennel.sources import sources, Snowflake, Kinesis
from fennel.lib.aggregate import Count, Sum
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.featuresets import featureset, extractor, feature
import pandas as pd
from fennel.test_lib import MockClient

snowflake = sources.Snowflake(
    name="snowflake_src",
    account="VPECCVJ-MUB03765",
    warehouse="FENNEL_TEST_WH",
    db_name="BLACKCROW_POC",
    src_schema="ORDERS",
    role="ACCOUNTADMIN",
    username="LOCAL_DEV_UT_USER",
    password="FennelLocalDev1",
)

kinesis = Kinesis(
    name="kinesis_src",
    role_arn="arn:aws:iam::824489454832:role/admin",
)
stream = kinesis.stream(
    stream_arn="arn:aws:kinesis:us-west-2:824489454832:stream/outbrain_test",
    init_position="trim_horizon",
    format="json",
)

__owner__ = "blackcrow@fennel.ai"


@source(
    snowflake.table(table_name="PAGE_VIEWS", cursor="TIMESTAMP"),
    every="24h",
)
@meta(deleted=True)
@dataset
class PageViewsSnowFlake:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: Optional[str]
    traffic_source: int


@source(stream)
@dataset
class PageViewsKinesis:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: Optional[str]
    traffic_source: int


@meta(deleted=True)
@dataset
class PageViewsUnion:
    uuid: str
    document_id: int
    timestamp: datetime
    platform: int
    geo_location: Optional[str]
    traffic_source: int

    @pipeline(version=1)
    @inputs(PageViewsSnowFlake, PageViewsKinesis)
    def union(cls, page_views_snow_flake: Dataset, page_views_kinesis: Dataset):
        a = page_views_snow_flake.filter(
            lambda df: df["timestamp"] > "2021-01-01"
        )
        b = page_views_kinesis.filter(lambda df: df["timestamp"] > "2021-01-01")
        return a + b


@meta(deleted=True)
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
    #@inputs(PageViewsUnion)
    @inputs(PageViewsKinesis)
    def group_by_user(cls, page_views: Dataset):
        return page_views.groupby("uuid").aggregate(
            [
                Count(window="28d", into_field="page_views"),
                Count(window="1d", into_field="page_views_1d"),
                Count(window="3d", into_field="page_views_3d"),
                Count(window="7d", into_field="page_views_7d"),
            ]
        )


@featureset
class Request:
    uuid: str = feature(id=1)
    document_id: int = feature(id=2)


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


client = Client("https://main.fe-moh5jg8f2v.aws.fennel.ai")

parser = argparse.ArgumentParser(description="A minimal example for fennel")
parser.add_argument("-s", action="store_true")
parser.add_argument("-e")
parser.add_argument("-k", action="store_true") # kinesis only
args = parser.parse_args()

if args.s:
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
elif args.k:
    client.sync(
        datasets=[
            PageViewsKinesis,
            PageViewsByUser,
        ],
        featuresets=[Request, UserPageViewFeatures],
    )
elif args.e:
    df = client.extract_features(
        input_feature_list=[Request.uuid], 
        input_dataframe=pd.DataFrame({"Request.uuid": [str(args.e)]}),
        output_feature_list=[UserPageViewFeatures.page_views, UserPageViewFeatures.page_views_1d]
    )
    print(df)
else:
    print("Please specify -s or -e or -k")
