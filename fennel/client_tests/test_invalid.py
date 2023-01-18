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


@meta(owner="test@fennel.ai")
@dataset
class MemberActivityDataset:
    url: str
    uid: str
    transitionType: str
    time: datetime = field(timestamp=True)
    domain: str = field(key=True)
    hasShortcut: bool
    country: str
    DOMAIN_USED_COUNT: int


@meta(owner="test@fennel.ai")
@dataset
class MemberDataset:
    pk: str
    sk: str
    uid: str = field(key=True)
    email: str
    displayName: str
    createdAt: datetime = field(timestamp=True)


@meta(owner="test@fennel.ai")
@dataset
class MemberActivityDatasetCopy:
    domain: str = field(key=True)
    DOMAIN_USED_COUNT: int
    time: datetime = field(timestamp=True)
    url: str
    uid: str
    transitionType: str
    hasShortcut: bool
    country: str

    @pipeline(id=1)
    def copy(cls, ds: Dataset[MemberActivityDataset]):
        return ds


@meta(owner="test@fennel.ai")
@featureset
class DomainFeatures:
    domain: str = feature(id=1)
    DOMAIN_USED_COUNT: int = feature(id=2)

    @extractor
    @depends_on(MemberActivityDatasetCopy)
    def get_domain_feature(
        cls, ts: Series[datetime], domain: Series[Query.domain]
    ) -> DataFrame[domain, DOMAIN_USED_COUNT]:
        df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
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
                str(e.value) == "Dataset MemberActivityDatasetCopy "
                "not found in sync call"
            )


@meta(owner="test@fennel.ai")
@featureset
class DomainFeatures2:
    domain: str = feature(id=1)
    DOMAIN_USED_COUNT: int = feature(id=2)

    @extractor
    @depends_on(MemberDataset)
    def get_domain_feature(
        cls, ts: Series[datetime], domain: Series[Query.domain]
    ) -> DataFrame[domain, DOMAIN_USED_COUNT]:
        df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
            ts, domain=domain
        )
        return df


class TestInvalidExtractorDependsOn(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_missing_features(self, client):
        @meta(owner="test@fennel.ai")
        @dataset
        class MemberActivityDataset:
            url: str
            uid: str
            transitionType: str
            time: datetime = field(timestamp=True)
            domain: str = field(key=True)
            hasShortcut: bool
            country: str
            DOMAIN_USED_COUNT: int

        @meta(owner="test@fennel.ai")
        @dataset
        class MemberDataset:
            pk: str
            sk: str
            uid: str = field(key=True)
            email: str
            displayName: str
            createdAt: datetime = field(timestamp=True)

        @meta(owner="test@fennel.ai")
        @dataset
        class MemberActivityDatasetCopy:
            domain: str = field(key=True)
            DOMAIN_USED_COUNT: int
            time: datetime = field(timestamp=True)
            url: str
            uid: str
            transitionType: str
            hasShortcut: bool
            country: str

            @pipeline(id=1)
            def copy(cls, ds: Dataset[MemberActivityDataset]):
                return ds

        @meta(owner="test@fennel.ai")
        @featureset
        class DomainFeatures:
            domain: str = feature(id=1)
            DOMAIN_USED_COUNT: int = feature(id=2)

            @extractor
            @depends_on(MemberActivityDatasetCopy)
            def get_domain_feature(
                cls, ts: Series[datetime], domain: Series[Query.domain]
            ) -> DataFrame[domain, DOMAIN_USED_COUNT]:
                df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
                    ts, domain=domain
                )
                return df

        with pytest.raises(Exception) as e:
            client.sync(datasets=[MemberDataset], featuresets=[DomainFeatures2])
            client.extract_features(
                output_feature_list=[DomainFeatures2],
                input_feature_list=[Query],
                input_dataframe=pd.DataFrame(
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
                input_dataframe=pd.DataFrame(
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
                "Dataset MemberActivityDatasetCopy not found, please ensure it is synced."
                == str(e.value)
            )

    @pytest.mark.integration
    @mock_client
    def test_no_access(self, client):
        with pytest.raises(Exception) as e:
            client.sync(
                datasets=[MemberDataset, MemberActivityDatasetCopy],
                featuresets=[DomainFeatures2],
            )
            client.extract_features(
                output_feature_list=[DomainFeatures2],
                input_feature_list=[Query.domain],
                input_dataframe=pd.DataFrame(
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
                "Extractor is not allowed to access dataset MemberActivityDatasetCopy, enabled datasets are ['MemberDataset']"
                == str(e.value)
            )
