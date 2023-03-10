from collections import Counter
import re
import pandas as pd
import unittest
import requests
import json
import time
from fennel.lib.schema import DataFrame, Series
from typing import Tuple, Optional
from fennel.datasets import dataset, Dataset, pipeline, field
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.metadata import meta
from datetime import datetime
from fennel.sources import source
from fennel.lib.aggregate import Count, Sum, Average, TopK
from fennel.lib.window import Window
import numpy as np
from fennel.client import Client


def log_to_client(dataset, client, dataset_df):
    response = client.log(dataset._name, dataset_df, batch_size=200)
    assert response.status_code == requests.codes.OK, response.json()
    return response.status_code


def extract_coarse_url(url):
    url = url.lower()
    dot_parts = url.split('.')
    if len(dot_parts) >= 2 and dot_parts[-2] in ['google']:
        return '.'.join(dot_parts[-3:-1])
    return '.'.join(dot_parts[-2:-1])


def get_real_domain(domain_df, referrer_df, object_type_df):
    # iterate through each row in domain, referrer, object_type
    real_domain = pd.Series(np.zeros(len(domain_df)))
    for i in range(len(domain_df)):
        domain = domain_df[i]
        referrer = referrer_df[i]
        object_type = object_type_df[i]
        if object_type == 'LINK':
            # assign row i of real_domain to domain
            real_domain[i] = domain
        else:  # if object_type == 'SNIPPET':
            real_domain[i] = referrer
    return real_domain


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class ShortcutDataset:
    pk: str
    sk: str
    shortcut_id: str = field(key=True)
    shortlink: str
    created_at: datetime = field(timestamp=True)
    creator: str
    kind: str
    org: str
    url: str
    creator_id: str


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberDataset:
    org: str
    sk: str
    created_at: datetime = field(timestamp=True)
    email: str
    member_id: str = field(key=True)


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class ShortcutActivitySourceDataset:
    country: str
    platform: str
    object_type: str  # SNIPPET then use referrer, if it is LINK, then use domain
    browser: str
    shortcut_id: str
    os: str
    org: str
    domain: str
    location: str
    time: datetime = field(timestamp=True)
    object_category: str
    referrer: str
    member_id: str


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class ShortcutActivityDataset:
    country: str
    platform: str
    object_type: str
    browser: str
    shortcut_id: str
    os: str
    org: str
    domain: str
    coarse_domain: str
    location: str
    time: datetime = field(timestamp=True)
    object_category: str
    referrer: str
    member_id: str
    real_domain: str

    @classmethod
    @pipeline(id=1)
    def pipeline_transform(cls, m: Dataset[ShortcutActivitySourceDataset]):
        def t(df: pd.DataFrame) -> pd.DataFrame:
            # assign domain to real_domain if object_type is LINK
            df["real_domain"] = df["domain"]
            # assign referrer to real_domain if object_type is SNIPPET
            df.loc[df["object_type"] == "SNIPPET", "real_domain"] = df["referrer"]
            df["coarse_domain"] = df["real_domain"].apply(extract_coarse_url)
            return df

        return m.transform(
            t,
            schema={
                "country": str,
                "platform": str,
                "object_type": str,
                "browser": str,
                "shortcut_id": str,
                "os": str,
                "org": str,
                "domain": str,
                "coarse_domain": str,
                "location": str,
                "time": datetime,
                "object_category": str,
                "referrer": str,
                "member_id": str,
                "real_domain": str,
            },
        )


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberActivityDataset:
    url: str
    member_id: str
    transitionType: str
    time: datetime = field(timestamp=True)
    domain: str
    hasShortcut: bool
    country: str


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class ShortcutUsageAggByShortcutDataset:
    shortcut_id: str = field(key=True)
    SHORTCUT_TOTAL: int
    time: datetime = field(timestamp=True)

    @classmethod
    @pipeline(id=2)
    def aggregation(cls, shortcut_activity: Dataset[ShortcutActivityDataset]):
        ds = shortcut_activity.groupby("shortcut_id").aggregate(
            [Count(window=Window("forever"), into_field=str(cls.SHORTCUT_TOTAL))]
        )
        return ds


@featureset
@meta(owner="dhruv.anand@oslash.com")
class Query:
    shortcut_id: str = feature(id=1)
    member_id: str = feature(id=2)
    domain: str = feature(id=3)
    creator_id: str = feature(id=4)
    referrer: str = feature(id=5)
    object_type: str = feature(id=6)


# featureset for ShortcutUsageAggByShortcutDataset
@meta(owner="dhruv.anand@oslash.com")
@featureset
class ShortcutUsageAggByShortcutFeatureset:
    shortcut_id: str = feature(id=1)
    SHORTCUT_TOTAL: int = feature(id=2)

    @classmethod
    @extractor
    @depends_on(ShortcutUsageAggByShortcutDataset)
    def get_features(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
    ) -> DataFrame[shortcut_id, SHORTCUT_TOTAL]:
        df, found = ShortcutUsageAggByShortcutDataset.lookup(
            ts, shortcut_id=shortcut_id
        )
        return df[
            [
                str(cls.shortcut_id),
                str(cls.SHORTCUT_TOTAL),
            ]
        ]


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberShortcutGranularUsageDataset:
    shortcut_id: str = field(key=True)
    member_id: str = field(key=True)
    SHORTCUT_MEMBER_TOTAL: int
    SHORTCUT_MEMBER_TOTAL_1W: int
    time: datetime = field(timestamp=True)

    @staticmethod
    @pipeline(id=3)
    def aggregation(cls, shortcut_activity: Dataset[ShortcutActivityDataset]):
        # -> DataFrame["member_id", "shortcut_id", "SHORTCUT_MEMBER_TOTAL", "SHORTCUT_MEMBER_TOTAL_1W"]\

        ds = shortcut_activity.groupby(
            str(ShortcutActivityDataset.member_id),
            str(ShortcutActivityDataset.shortcut_id),
        ).aggregate(
            [
                Count(
                    window=Window("forever"), into_field=str(cls.SHORTCUT_MEMBER_TOTAL)
                ),
                # one week
                Count(
                    window=Window("1w"), into_field=str(cls.SHORTCUT_MEMBER_TOTAL_1W)
                ),
            ]
        )
        return ds


# dataset that aggregates ShortuctActivityDataset on member_id, real_domain and shortcut_id
@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberShortcutDomainGranularUsageDataset:
    shortcut_id: str = field(key=True)
    member_id: str = field(key=True)
    real_domain: str = field(key=True)
    SHORTCUT_MEMBER_DOMAIN_TOTAL: int
    SHORTCUT_MEMBER_DOMAIN_TOTAL_1W: int
    time: datetime = field(timestamp=True)

    @staticmethod
    @pipeline(id=4)
    def aggregation(
        cls, shortcut_activity: Dataset[ShortcutActivityDataset]
    ) -> DataFrame[
        "member_id",
        "shortcut_id",
        "real_domain",
        "SHORTCUT_MEMBER_DOMAIN_TOTAL",
        "SHORTCUT_MEMBER_DOMAIN_TOTAL_1W",
        "time",
    ]:
        # check object_type
        # if object_type == "SNIPPET": then group by referrer
        # else if object_type is 'LINK' group by domain
        ds = shortcut_activity.groupby(
            str(ShortcutActivityDataset.member_id),
            str(ShortcutActivityDataset.shortcut_id),
            str(ShortcutActivityDataset.real_domain),
        ).aggregate(
            [
                Count(
                    window=Window("forever"),
                    into_field=str(cls.SHORTCUT_MEMBER_DOMAIN_TOTAL),
                ),
                # one week
                Count(
                    window=Window("1w"),
                    into_field=str(cls.SHORTCUT_MEMBER_DOMAIN_TOTAL_1W),
                ),
            ]
        )
        return ds


# dataset that aggregates ShortuctActivityDataset on member_id, coarse_domain and shortcut_id
@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberShortcutCoarseDomainGranularUsageDataset:
    shortcut_id: str = field(key=True)
    member_id: str = field(key=True)
    coarse_domain: str = field(key=True)
    SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL: int
    SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL_1W: int
    time: datetime = field(timestamp=True)

    @staticmethod
    @pipeline(id=6)
    def aggregation(
        cls, shortcut_activity: Dataset[ShortcutActivityDataset]
    ) -> DataFrame[
        "member_id",
        "shortcut_id",
        "coarse_domain",
        "SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL",
        "SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL_1W",
        "time",
    ]:
        ds = shortcut_activity.groupby(
            str(ShortcutActivityDataset.member_id),
            str(ShortcutActivityDataset.shortcut_id),
            str(ShortcutActivityDataset.coarse_domain),
        ).aggregate(
            [
                Count(
                    window=Window("forever"),
                    into_field=str(cls.SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL),
                ),
                # one week
                Count(
                    window=Window("1w"),
                    into_field=str(cls.SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL_1W),
                ),
            ]
        )
        return ds


@meta(owner="dhruv.anand@oslash.com")
@featureset
class MemberShortcutGranularUsageFeatureset:
    shortcut_id: str = feature(id=1)
    member_id: str = feature(id=2)
    SHORTCUT_MEMBER_TOTAL: int = feature(id=3)
    SHORTCUT_MEMBER_TOTAL_1W: int = feature(id=4)

    @classmethod
    @extractor
    @depends_on(MemberShortcutGranularUsageDataset)
    def get_features(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
        member_id: Series[Query.member_id],
    ) -> DataFrame[
        shortcut_id, member_id, SHORTCUT_MEMBER_TOTAL, SHORTCUT_MEMBER_TOTAL_1W
    ]:
        df, found = MemberShortcutGranularUsageDataset.lookup(
            ts, shortcut_id=shortcut_id, member_id=member_id
        )
        return df[
            [
                str(cls.shortcut_id),
                str(cls.member_id),
                str(cls.SHORTCUT_MEMBER_TOTAL),
                str(cls.SHORTCUT_MEMBER_TOTAL_1W),
            ]
        ]


# featureset for MemberShortcutDomainGranularUsageDataset
@meta(owner="dhruv.anand@oslash.com")
@featureset
class MemberShortcutDomainGranularUsageFeatureset:
    shortcut_id: str = feature(id=1)
    member_id: str = feature(id=2)
    real_domain: str = feature(id=3)
    SHORTCUT_MEMBER_DOMAIN_TOTAL: int = feature(id=4)
    SHORTCUT_MEMBER_DOMAIN_TOTAL_1W: int = feature(id=5)

    @classmethod
    @extractor
    @depends_on(MemberShortcutDomainGranularUsageDataset)
    def extract(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
        member_id: Series[Query.member_id],
        domain: Series[Query.domain],
        referrer: Series[Query.referrer],
        object_type: Series[Query.object_type],
    ) -> DataFrame[
        shortcut_id,
        member_id,
        real_domain,
        SHORTCUT_MEMBER_DOMAIN_TOTAL,
        SHORTCUT_MEMBER_DOMAIN_TOTAL_1W,
    ]:
        # real_domain df from domain,referrer and object_type dfs using get_real_domain on each row
        real_domain_series = get_real_domain(domain, referrer, object_type)
        df, found = MemberShortcutDomainGranularUsageDataset.lookup(
            ts,
            member_id=member_id,
            shortcut_id=shortcut_id,
            real_domain=real_domain_series,
        )
        return df[
            [
                str(cls.shortcut_id),
                str(cls.member_id),
                str(cls.real_domain),
                str(cls.SHORTCUT_MEMBER_DOMAIN_TOTAL),
                str(cls.SHORTCUT_MEMBER_DOMAIN_TOTAL_1W),
            ]
        ]


# featureset for MemberShortcutCoarseDomainGranularUsageDataset
@meta(owner="dhruv.anand@oslash.com")
@featureset
class MemberShortcutCoarseDomainGranularUsageFeatureset:
    shortcut_id: str = feature(id=1)
    member_id: str = feature(id=2)
    coarse_domain: str = feature(id=3)
    SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL: int = feature(id=4)
    SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL_1W: int = feature(id=5)

    @classmethod
    @extractor
    @depends_on(MemberShortcutCoarseDomainGranularUsageDataset)
    def extract(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
        member_id: Series[Query.member_id],
        domain: Series[Query.domain],
        referrer: Series[Query.referrer],
        object_type: Series[Query.object_type],
    ) -> DataFrame[
        shortcut_id,
        member_id,
        coarse_domain,
        SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL,
        SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL_1W,
    ]:
        real_domain_series = get_real_domain(domain, referrer, object_type)
        coarse_domain_series = real_domain_series.apply(lambda x: extract_coarse_url(x))
        df, found = MemberShortcutCoarseDomainGranularUsageDataset.lookup(
            ts,
            member_id=member_id,
            shortcut_id=shortcut_id,
            coarse_domain=coarse_domain_series,
        )
        return df[
            [
                str(cls.shortcut_id),
                str(cls.member_id),
                str(cls.coarse_domain),
                str(cls.SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL),
                str(cls.SHORTCUT_MEMBER_COARSE_DOMAIN_TOTAL_1W),
            ]
        ]


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class DomainShortcutGranularUsageDataset:
    shortcut_id: str = field(key=True)
    real_domain: str = field(key=True)
    SHORTCUT_DOMAIN_TOTAL: int
    SHORTCUT_DOMAIN_TOTAL_1W: int
    time: datetime = field(timestamp=True)

    @classmethod
    @pipeline(id=5)
    def aggregation(
        cls, shortcut_activity: Dataset[ShortcutActivityDataset]
    ) -> DataFrame[
        "domain", "shortcut_id", "SHORTCUT_DOMAIN_TOTAL", "SHORTCUT_DOMAIN_TOTAL_1W"
    ]:
        ds = shortcut_activity.groupby("real_domain", "shortcut_id").aggregate(
            [
                Count(
                    window=Window("forever"), into_field=str(cls.SHORTCUT_DOMAIN_TOTAL)
                ),
                # one week
                Count(
                    window=Window("1w"), into_field=str(cls.SHORTCUT_DOMAIN_TOTAL_1W)
                ),
            ]
        )
        return ds


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class CoarseDomainShortcutGranularUsageDataset:
    shortcut_id: str = field(key=True)
    coarse_domain: str = field(key=True)
    SHORTCUT_COARSE_DOMAIN_TOTAL: int
    SHORTCUT_COARSE_DOMAIN_TOTAL_1W: int
    time: datetime = field(timestamp=True)

    @classmethod
    @pipeline(id=6)
    def aggregation(
        cls, shortcut_activity: Dataset[ShortcutActivityDataset]
    ) -> DataFrame[
        "coarse_domain",
        "shortcut_id",
        "SHORTCUT_COARSE_DOMAIN_TOTAL",
        "SHORTCUT_COARSE_DOMAIN_TOTAL_1W",
    ]:
        ds = shortcut_activity.groupby("coarse_domain", "shortcut_id").aggregate(
            [
                Count(
                    window=Window("forever"),
                    into_field=str(cls.SHORTCUT_COARSE_DOMAIN_TOTAL),
                ),
                # one week
                Count(
                    window=Window("1w"),
                    into_field=str(cls.SHORTCUT_COARSE_DOMAIN_TOTAL_1W),
                ),
            ]
        )
        return ds


@meta(owner="dhruv.anand@oslash.com")
@featureset
class DomainShortcutGranularUsageFeatureset:
    shortcut_id: str = feature(id=1)
    real_domain: str = feature(id=2)
    SHORTCUT_DOMAIN_TOTAL: int = feature(id=3)
    SHORTCUT_DOMAIN_TOTAL_1W: int = feature(id=4)

    @classmethod
    @extractor
    @depends_on(DomainShortcutGranularUsageDataset)
    def get_features(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
        domain: Series[Query.domain],
        referrer: Series[Query.referrer],
        object_type: Series[Query.object_type],
    ) -> DataFrame[
        shortcut_id, real_domain, SHORTCUT_DOMAIN_TOTAL, SHORTCUT_DOMAIN_TOTAL_1W
    ]:
        real_domain_series = get_real_domain(domain, referrer, object_type)
        df, found = DomainShortcutGranularUsageDataset.lookup(
            ts, shortcut_id=shortcut_id, real_domain=real_domain_series
        )
        return df[
            [
                str(cls.shortcut_id),
                str(cls.real_domain),
                str(cls.SHORTCUT_DOMAIN_TOTAL),
                str(cls.SHORTCUT_DOMAIN_TOTAL_1W),
            ]
        ]


@meta(owner="dhruv.anand@oslash.com")
@featureset
class CoarseDomainShortcutGranularUsageFeatureset:
    shortcut_id: str = feature(id=1)
    coarse_domain: str = feature(id=2)
    SHORTCUT_COARSE_DOMAIN_TOTAL: int = feature(id=3)
    SHORTCUT_COARSE_DOMAIN_TOTAL_1W: int = feature(id=4)

    @classmethod
    @extractor
    @depends_on(CoarseDomainShortcutGranularUsageDataset)
    def get_features(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
        domain: Series[Query.domain],
        referrer: Series[Query.referrer],
        object_type: Series[Query.object_type],
    ) -> DataFrame[
        shortcut_id,
        coarse_domain,
        SHORTCUT_COARSE_DOMAIN_TOTAL,
        SHORTCUT_COARSE_DOMAIN_TOTAL_1W,
    ]:
        real_domain_series = get_real_domain(domain, referrer, object_type)
        coarse_domain = real_domain_series.apply(lambda x: extract_coarse_url(x))
        df, found = CoarseDomainShortcutGranularUsageDataset.lookup(
            ts, shortcut_id=shortcut_id, coarse_domain=coarse_domain
        )
        return df[
            [
                str(cls.shortcut_id),
                str(cls.coarse_domain),
                str(cls.SHORTCUT_COARSE_DOMAIN_TOTAL),
                str(cls.SHORTCUT_COARSE_DOMAIN_TOTAL_1W),
            ]
        ]


# featureset for ShortcutOwnedByMemberDataset
@meta(owner="dhruv.anand@oslash.com")
@featureset
class ShortcutDetailsFeatureset:
    shortcut_id: str = feature(id=1)
    creator_id: str = feature(id=2)
    SHORTCUT_OWNED_BY_MEMBER: int = feature(id=3)
    created_at: datetime = feature(id=4)
    SHORTCUT_AGE_DECAY: float = feature(id=5)
    SHORTCUT_AGE_RECIPROCAL: int = feature(id=6)

    @classmethod
    @extractor
    @depends_on(ShortcutDataset)
    def get_features(
        cls,
        ts: Series[datetime],
        shortcut_id: Series[Query.shortcut_id],
        creator_id: Series[Query.creator_id],
    ) -> DataFrame[
        shortcut_id,
        creator_id,
        created_at,
        SHORTCUT_OWNED_BY_MEMBER,
        SHORTCUT_AGE_DECAY,
        SHORTCUT_AGE_RECIPROCAL,
    ]:
        df, found = ShortcutDataset.lookup(ts, shortcut_id=shortcut_id)
        # print("df[created_at] = ", df["created_at"])
        # convert None to datetime start of time in df['created_at'] when found is False
        df.loc[~found, "created_at"] = datetime.fromtimestamp(0)
        df[str(cls.SHORTCUT_OWNED_BY_MEMBER)] = (
            df[str(cls.creator_id)] == creator_id
        )
        # print("df[]", df[str(cls.SHORTCUT_OWNED_BY_MEMBER)])
        timeDiff = ts - df["created_at"].astype("datetime64[ns]")
        numOfSecondsInAYear = 3600 * 24 * 365

        df.loc[~found, str(cls.SHORTCUT_AGE_DECAY)] = 0.0
        df[str(cls.SHORTCUT_AGE_DECAY)] = timeDiff.apply(
            lambda x: np.exp(-x.total_seconds() / numOfSecondsInAYear)
        )
        df.loc[~found, str(cls.SHORTCUT_AGE_RECIPROCAL)] = 0
        df[str(cls.SHORTCUT_AGE_RECIPROCAL)] = timeDiff.apply(
            lambda x: 1 / (x.days + (x.seconds / (3600.0 * 24)) + 0.01)
        )
        df[str(cls.SHORTCUT_AGE_RECIPROCAL)] = df[str(cls.SHORTCUT_AGE_RECIPROCAL)].astype(int)
        return df[
            [
                str(cls.shortcut_id),
                str(cls.creator_id),
                str(cls.created_at),
                str(cls.SHORTCUT_OWNED_BY_MEMBER),
                str(cls.SHORTCUT_AGE_DECAY),
                str(cls.SHORTCUT_AGE_RECIPROCAL),
            ]
        ]


class TestOslash(unittest.TestCase):

    def test_reset(self):
        client = Client(url="localhost:50051", rest_url="http://localhost:3000")
        client.sync([])


    def test_oslash_extract_features_blocked(self):
        client = Client(url="k8s-dev-devaesen-b6c8760480-cbbef87fba57325e.elb.us-west-2.amazonaws.com",
                        rest_url="http://k8s-dev-devaesen-b6c8760480-cbbef87fba57325e.elb.us-west-2.amazonaws.com")
        # client.sync([])
        client.sync(
            datasets=[
                ShortcutDataset,  # 1
                MemberDataset,  # 2
                ShortcutActivitySourceDataset,  # 3
                MemberActivityDataset,  # 4
                ShortcutActivityDataset,  # 5
                MemberShortcutGranularUsageDataset,  # 6
                MemberShortcutDomainGranularUsageDataset,  # 7
                DomainShortcutGranularUsageDataset,  # 8
                ShortcutUsageAggByShortcutDataset,  # 9
                MemberShortcutCoarseDomainGranularUsageDataset,  # 10
                CoarseDomainShortcutGranularUsageDataset,  # 11
            ],
            featuresets=[
                Query,  # 1
                MemberShortcutGranularUsageFeatureset,  # 2
                MemberShortcutDomainGranularUsageFeatureset,  # 3
                DomainShortcutGranularUsageFeatureset,  # 4
                ShortcutUsageAggByShortcutFeatureset,  # 5
                ShortcutDetailsFeatureset,  # 6
                MemberShortcutCoarseDomainGranularUsageFeatureset,  # 7
                CoarseDomainShortcutGranularUsageFeatureset,  # 8
            ],
        )

        shortcutDf = pd.read_csv("/Users/mohitreddy/fennel-ai/client/fennel/client_tests/CsvData/v2/shortcutsData.csv", on_bad_lines="skip")
        shortcutDf["created_at"] = pd.to_datetime(shortcutDf["created_at"]).dt.tz_localize(None)
        shortcutDf = shortcutDf.fillna("NotAvailable")
        # shortcutDf = shortcutDf.head(1000)

        memberDf = pd.read_csv("/Users/mohitreddy/fennel-ai/client/fennel/client_tests/CsvData/v2/membersData.csv", on_bad_lines="skip")
        memberDf["created_at"] = pd.to_datetime(memberDf["created_at"]).dt.tz_localize(None)
        memberDf = memberDf.fillna("NotAvailable")
        # memberDf = memberDf.head(1000)
        # memberDf.head(2)

        shortcutActivityDf = pd.read_csv("/Users/mohitreddy/fennel-ai/client/fennel/client_tests/CsvData/v2/shortcutActivity.csv")
        shortcutActivityDf["time"] = pd.to_datetime(shortcutActivityDf["time"])
        shortcutActivityDf = shortcutActivityDf.dropna()
        shortcutActivityDf = shortcutActivityDf.merge(
            memberDf, on="member_id", how="inner"
        )
        # # drop column org_y and rename org_x to org
        shortcutActivityDf = shortcutActivityDf.drop(columns=["org_y"])
        shortcutActivityDf = shortcutActivityDf.rename(columns={"org_x": "org"})
        # shortcutActivityDf = shortcutActivityDf.head(1000)

        memberActivityDf = pd.read_csv("/Users/mohitreddy/fennel-ai/client/fennel/client_tests/CsvData/v2/memberActivity.csv")
        memberActivityDf["time"] = pd.to_datetime(memberActivityDf["time"])
        # memberActivityDf = memberActivityDf.head(1000)
        # # memberActivityDf.head(2)

        # log_to_client(ShortcutDataset, client, shortcutDf)
        # log_to_client(MemberDataset, client, memberDf)
        # log_to_client(ShortcutActivitySourceDataset, client, shortcutActivityDf)

        now = datetime.now()
        ts = pd.Series([now])
        sid2 = "LINK#888a5c75-51a8-4ba4-bad8-525fdcaf763e"
        sid3 = "LINK#bd60465c-2605-4206-b6e1-b5d2d29de85c"
        mid2 = "fd8cab27-4c30-4edf-ad98-d69adebdb386"
        mid3 = "ec849f7b-0d2c-4a1b-a1c1-2145562062c8"
        mid4 = '9b5d0228-8731-45d8-b148-7f62e427b762'
        sid4 = 'LINK#9c041949-331c-4496-8ec6-57e556ecedca'
        did4 = 'app.datadoghq.com'
        sid5 = 'LINK#1e2ebae3-2604-497a-a804-5013a34ebc40'
        mid5 = 'aa5c6473-4e09-4087-84a0-91de1fa9efe2'
        did5 = 'vercel.com'
        firstSAdfRow = shortcutActivityDf.head(1)
        sid6, mid6, did6 = firstSAdfRow['shortcut_id'], firstSAdfRow['member_id'], firstSAdfRow['domain']
        sid6, mid6, did6 = sid6[0], mid6[0], did6[0]
        mid7 = "google-oauth2|117616787569718433094"
        sid7 = "81ab9522-b846-4945-ba2a-4da071b4ee17"
        mid8 = 'aa5c6473-4e09-4087-84a0-91de1fa9efe2'
        sid8 = '1e2ebae3-2604-497a-a804-5013a34ebc40'
        rid8 = ''
        ot8 = 'LINK'
        print("calling extract features")
        time.sleep(10)

        headSAD = shortcutActivityDf.head(10)
        sidL = headSAD["shortcut_id"]
        midL = headSAD["member_id"]
        didL = headSAD["domain"]
        ridL = headSAD["referrer"]
        otL = headSAD["object_type"]

        sid9, mid9, did9, rid9, ot9 = (
            headSAD.iloc[0]["shortcut_id"],
            headSAD.iloc[0]["member_id"],
            headSAD.iloc[0]["domain"],
            headSAD.iloc[0]["referrer"],
            headSAD.iloc[0]["object_type"],
        )

        res = client.extract_features(
            output_feature_list=[ShortcutDetailsFeatureset],
            input_feature_list=[Query.shortcut_id, Query.creator_id],
            input_dataframe=pd.DataFrame({"Query.shortcut_id": [sid7], "Query.creator_id": ['MEMBER#' + mid7]}),
        )
        print(res)
        result = client.extract_features(
            output_feature_list=[
                MemberShortcutGranularUsageFeatureset,
                MemberShortcutDomainGranularUsageFeatureset,
                DomainShortcutGranularUsageFeatureset,
                ShortcutUsageAggByShortcutFeatureset,
                ShortcutDetailsFeatureset,
                MemberShortcutCoarseDomainGranularUsageFeatureset,
                CoarseDomainShortcutGranularUsageFeatureset,
            ],
            input_feature_list=[
                Query.shortcut_id,
                Query.member_id,
                Query.domain,
                Query.creator_id,
                Query.referrer,
                Query.object_type,
            ],
            input_dataframe=pd.DataFrame(
                {
                    "Query.shortcut_id": ["LINK#888a5c75-51a8-4ba4-bad8-525fdcaf763e"],
                    "Query.member_id": ["fd8cab27-4c30-4edf-ad98-d69adebdb386"],
                    "Query.domain": ["app.datadoghq.com"],
                    "Query.creator_id": ["fd8cab27-4c30-4edf-ad98-d69adebdb386"],
                    "Query.referrer": [""],
                    "Query.object_type": ["LINK"],
                }
            ),
        )
        print(result)
        res = client.extract_features(
            output_feature_list=[ShortcutDetailsFeatureset],
            input_feature_list=[Query.shortcut_id, Query.creator_id],
            input_dataframe=pd.DataFrame({"Query.shortcut_id": [sid7], "Query.creator_id": ['MEMBER#' + mid7]}),
        )
        print(res.T)

        r0 = client.extract_features(
            output_feature_list=[ShortcutUsageAggByShortcutFeatureset],
            input_feature_list=[Query.shortcut_id],
            input_dataframe=pd.DataFrame({"Query.shortcut_id": [sid6]}),
        )
        print(r0.T)

        r1 = client.extract_features(
            output_feature_list=[MemberShortcutGranularUsageFeatureset],
            input_feature_list=[Query.member_id, Query.shortcut_id],
            input_dataframe=pd.DataFrame({"Query.member_id": [mid5], "Query.shortcut_id": [sid5]}),
        )
        print(r1.T)
        mid4 = '9b5d0228-8731-45d8-b148-7f62e427b762'
        sid4 = 'LINK#9c041949-331c-4496-8ec6-57e556ecedca'
        did4 = 'app.datadoghq.com'
        result = client.extract_features(
            output_feature_list=[MemberShortcutDomainGranularUsageFeatureset],
            input_feature_list=[Query.member_id, Query.shortcut_id, Query.domain],
            input_dataframe=pd.DataFrame({"Query.member_id": [mid6], "Query.shortcut_id": [sid6], "Query.domain": [did6]}),
        )
        print(result.T)

        cdid6 = extract_coarse_url(did6)
        result = client.extract_features(
            output_feature_list=[MemberShortcutCoarseDomainGranularUsageFeatureset],
            input_feature_list=[Query.member_id, Query.shortcut_id, Query.coarse_domain],
            input_dataframe=pd.DataFrame({"Query.member_id": [mid6], "Query.shortcut_id": [sid6], "Query.domain": [did6]}),
        )

        print(result.T)

