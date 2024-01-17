import unittest
from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.sources import source, Webhook
from fennel.test_lib import *

# noinspection PyUnresolvedReferences

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@fennel.ai")
@featureset
class Query:
    shortcut_id: int = feature(id=1)
    member_id: int = feature(id=2)
    domain: str = feature(id=3)


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
    domain_used_count: int


@meta(owner="test@fennel.ai")
@source(webhook.endpoint("MemberDataset"))
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
    domain_used_count: int
    time: datetime = field(timestamp=True)
    url: str
    uid: str
    transitionType: str
    hasShortcut: bool
    country: str

    @pipeline(version=1)
    @inputs(MemberActivityDataset)
    def copy(cls, ds: Dataset):
        return ds


@meta(owner="test@fennel.ai")
@featureset
class DomainFeatures:
    domain: str = feature(id=1)
    domain_used_count: int = feature(id=2)

    @extractor(depends_on=[MemberActivityDatasetCopy])
    @inputs(Query.domain)
    @outputs(domain, domain_used_count)
    def get_domain_feature(cls, ts: pd.Series, domain: pd.Series):
        df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
            ts, domain=domain
        )
        return df


class TestInvalidSync(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_invalid_sync(self, client):
        with pytest.raises(ValueError) as e:
            client.sync(featuresets=[DomainFeatures, Query])

        if client.is_integration_client():
            assert (
                str(e.value) == "Failed to sync: error: can not add edge "
                'to (Extractor, "DomainFeatures.get_domain_feature"): from vertex (Dataset, "MemberActivityDatasetCopy") not in graph'
            )
        else:
            assert (
                str(e.value) == "Dataset `MemberActivityDatasetCopy` "
                "not found in sync call"
            )


@meta(owner="test@fennel.ai")
@featureset
class DomainFeatures2:
    domain: str = feature(id=1)
    domain_used_count: int = feature(id=2)

    @extractor()
    @inputs(Query.domain)
    @outputs(domain, domain_used_count)
    def get_domain_feature(cls, ts: pd.Series, domain: pd.Series):
        df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
            ts, domain=domain
        )
        return df[[str(cls.domain), str(cls.domain_used_count)]]


class TestInvalidExtractorDependsOn(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_missing_features(self, client):
        @meta(owner="test@fennel.ai")
        @source(webhook.endpoint("MemberActivityDataset"))
        @dataset
        class MemberActivityDataset:
            url: str
            uid: str
            transitionType: str
            time: datetime = field(timestamp=True)
            domain: str = field(key=True)
            hasShortcut: bool
            country: str
            domain_used_count: int

        @meta(owner="test@fennel.ai")
        @source(webhook.endpoint("MemberDataset"))
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
            domain_used_count: int
            time: datetime = field(timestamp=True)
            url: str
            uid: str
            transitionType: str
            hasShortcut: bool
            country: str

            @pipeline(version=1)
            @inputs(MemberActivityDataset)
            def copy(cls, ds: Dataset):
                return ds

        @meta(owner="test@fennel.ai")
        @featureset
        class DomainFeatures:
            domain: str = feature(id=1)
            domain_used_count: int = feature(id=2)

            @extractor(depends_on=[MemberActivityDatasetCopy])
            @inputs(Query.domain)
            @outputs(domain, domain_used_count)
            def get_domain_feature(cls, ts: pd.Series, domain: pd.Series):
                df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
                    ts, domain=domain
                )
                return df

        with pytest.raises(Exception) as e:
            client.sync(
                datasets=[
                    MemberActivityDataset,
                    MemberActivityDatasetCopy,
                ],
                featuresets=[DomainFeatures, Query],
            )
            client.extract(
                outputs=[DomainFeatures2],
                inputs=[Query.member_id],
                input_dataframe=pd.DataFrame(
                    {
                        "Query.domain": [
                            "www.google.com",
                            "www.google.com",
                        ],
                    }
                ),
            )
        assert (
            "Input dataframe does not contain all the required features"
            in str(e.value)
        )

    @pytest.mark.integration
    @mock
    def test_missing_dataset(self, client):
        client.sync(
            datasets=[MemberDataset], featuresets=[DomainFeatures2, Query]
        )
        with pytest.raises(Exception) as e:
            client.extract(
                outputs=[DomainFeatures2],
                inputs=[Query.domain],
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
            assert "name 'MemberActivityDatasetCopy' is not defined" in str(
                e.value
            )
        else:
            assert (
                "Extractor `get_domain_feature` in `DomainFeatures2` "
                "failed to run with error: name 'MemberActivityDatasetCopy' is not defined. "
                == str(e.value)
            )

    @pytest.mark.integration
    @mock
    def test_no_access(self, client):
        with pytest.raises(Exception) as e:
            client.sync(
                datasets=[MemberDataset, MemberActivityDatasetCopy],
                featuresets=[DomainFeatures2, Query],
            )
            client.extract(
                outputs=[DomainFeatures2],
                inputs=[Query.domain],
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
                "Failed to sync: error: can not add edge to (Pipeline, "
                '"MemberActivityDatasetCopy-copy"): from vertex (Dataset, "MemberActivityDataset") not in graph'
                == str(e.value)
            )
        else:
            assert (
                str(e.value)
                == """Dataset `MemberActivityDataset` is an input to the pipelines: `['MemberActivityDatasetCopy.copy']` but is not synced. Please add it to the sync call."""
            )

    @mock
    def test_drop_timestamp_col(self, client):
        with pytest.raises(Exception) as e:

            @meta(owner="test@fennel.ai")
            @dataset
            class MemberDatasetDerived:
                pk: str
                sk: str
                uid: str = field(key=True)
                email: str
                displayName: str
                createdAt: datetime = field(timestamp=True)

                @pipeline(version=1)
                @inputs(MemberDataset)
                def del_timestamp(cls, d: Dataset):
                    return d.drop(columns=["createdAt"])

        assert (
            """Field `createdAt` is a key or timestamp field in schema of drop node input '[Dataset:MemberDataset]'. Value fields are: ['pk', 'sk', 'email', 'displayName']"""
        ) == str(e.value)
