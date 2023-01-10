import unittest
from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.featuresets import featureset, extractor, feature, depends_on
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
# noinspection PyUnresolvedReferences
from fennel.test_lib import *


@featureset
class Query:
    shortcut_id: int = feature(id=1)
    member_id: int = feature(id=2)
    domain: int = feature(id=3)


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberActivityDataset:
    url: str
    uid: str
    transitionType: str
    time: datetime = field(timestamp=True)
    domain: str
    hasShortcut: bool
    country: str


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class MemberDataset:
    pk: str
    sk: str
    uid: str = field(key=True)
    email: str
    displayName: str
    createdAt: datetime = field(timestamp=True)


@meta(owner="sagar.chudamani@oslash.com")
@dataset
class DomainUsageAggregatedByMemberDataset:
    domain: str = field(key=True)
    DOMAIN_USED_COUNT: int
    time: datetime = field(timestamp=True)

    @classmethod
    @pipeline(MemberActivityDataset)
    def aggregation(cls, ds: Dataset):
        return ds


@meta(owner="sagar.chudamani@oslash.com")
@featureset
class DomainFeatures:
    domain: str = feature(id=1)
    DOMAIN_USED_COUNT: int = feature(id=2)

    @classmethod
    @extractor
    @depends_on(DomainUsageAggregatedByMemberDataset)
    def get_domain_feature(
            cls, ts: Series[datetime], domain: Series[Query.domain]
    ) -> DataFrame[domain, DOMAIN_USED_COUNT]:
        df, found = DomainUsageAggregatedByMemberDataset.lookup(  # type: ignore
            ts, domain=domain
        )
        return df


class TestInvalidSync(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_invalid_sync(self, client):
        with pytest.raises(ValueError) as e:
            client.sync(featuresets=[DomainFeatures])

        if client.is_integration_client():
            assert (
                    str(e.value)
                    == "error: extractor DomainFeatures.get_domain_feature "
                       "of featureset DomainFeatures depends on unknown feature domain"
            )
        else:
            assert (
                    str(e.value) == "Dataset DomainUsageAggregatedByMemberDataset "
                                    "not found in sync call"
            )


@meta(owner="sagar.chudamani@oslash.com")
@featureset
class DomainFeatures2:
    domain: str = feature(id=1)
    DOMAIN_USED_COUNT: int = feature(id=2)

    @classmethod
    @extractor
    @depends_on(MemberDataset)
    def get_domain_feature(
            cls, ts: Series[datetime], domain: Series[Query.domain]
    ) -> DataFrame[domain, DOMAIN_USED_COUNT]:
        df, found = DomainUsageAggregatedByMemberDataset.lookup(  # type: ignore
            ts, domain=domain
        )
        return df


class TestInvalidExtractorDependsOn(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_missing_features(self, client):
        with pytest.raises(Exception) as e:
            client.sync(datasets=[MemberDataset], featuresets=[DomainFeatures2])
            client.extract_features(
                output_feature_list=[DomainFeatures2],
                input_feature_list=[Query],
                input_df=pd.DataFrame(
                    {
                        "Query.domain": [
                            "www.google.com",
                            "www.google.com",
                        ],
                    }
                ),
            )

        if client.is_integration_client():
            assert (
                    "error: extractor DomainFeatures2.get_domain_feature of featureset "
                    "DomainFeatures2 depends on unknown feature domain"
                    == str(e.value)
            )
        else:
            assert (
                    "Input dataframe does not contain all the required features"
                    in str(e.value)
            )

    @pytest.mark.integration
    @mock_client
    def test_missing_dataset(self, client):
        with pytest.raises(Exception) as e:
            client.sync(datasets=[MemberDataset], featuresets=[DomainFeatures2])
            client.extract_features(
                output_feature_list=[DomainFeatures2],
                input_feature_list=[Query.domain],
                input_df=pd.DataFrame(
                    {
                        "Query.domain": [
                            "www.google.com",
                            "www.google.com",
                        ],
                    }
                ),
            )

        if client.is_integration_client():
            assert (
                    "error: extractor DomainFeatures2.get_domain_feature of featureset DomainFeatures2 "
                    "depends on unknown feature domain" == str(e.value)
            )
        else:
            assert (
                    "Dataset DomainUsageAggregatedByMemberDataset not found, please ensure it is synced."
                    == str(e.value)
            )

    @pytest.mark.integration
    @mock_client
    def test_no_access(self, client):
        with pytest.raises(Exception) as e:
            client.sync(
                datasets=[MemberDataset, DomainUsageAggregatedByMemberDataset],
                featuresets=[DomainFeatures2],
            )
            client.extract_features(
                output_feature_list=[DomainFeatures2],
                input_feature_list=[Query.domain],
                input_df=pd.DataFrame(
                    {
                        "Query.domain": [
                            "www.google.com",
                            "www.google.com",
                        ],
                    }
                ),
            )

        if client.is_integration_client():
            assert (
                    "error: extractor DomainFeatures2.get_domain_feature of featureset DomainFeatures2 "
                    "depends on unknown feature domain" == str(e.value)
            )
        else:
            assert (
                    "Extractor is not allowed to access dataset DomainUsageAggregatedByMemberDataset, enabled datasets are ['MemberDataset']"
                    == str(e.value)
            )
