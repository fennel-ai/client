import unittest
from datetime import datetime

import pandas as pd
import pytest

from fennel.connectors import source, Webhook
from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.featuresets import featureset, extractor
from fennel.lib import meta, inputs, outputs
from fennel.testing import *

# noinspection PyUnresolvedReferences

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@fennel.ai")
@featureset
class Query:
    shortcut_id: int
    member_id: int
    domain: str


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
@source(webhook.endpoint("MemberDataset"), cdc="upsert", disorder="14d")
@dataset
class MemberDataset:
    pk: str
    sk: str
    uid: str = field(key=True)
    email: str
    displayName: str
    createdAt: datetime = field(timestamp=True)


@meta(owner="test@fennel.ai")
@dataset(index=True)
class MemberActivityDatasetCopy:
    domain: str = field(key=True)
    domain_used_count: int
    time: datetime = field(timestamp=True)
    url: str
    uid: str
    transitionType: str
    hasShortcut: bool
    country: str

    @pipeline
    @inputs(MemberActivityDataset)
    def copy(cls, ds: Dataset):
        return ds


@meta(owner="test@fennel.ai")
@featureset
class DomainFeatures:
    domain: str
    domain_used_count: int

    @extractor(deps=[MemberActivityDatasetCopy])  # type: ignore
    @inputs(Query.domain)
    @outputs("domain", "domain_used_count")
    def get_domain_feature(cls, ts: pd.Series, domain: pd.Series):
        df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
            ts, domain=domain
        )
        return df


class TestInvalidSync(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_invalid_sync(self, client):
        with pytest.raises(Exception) as e:
            client.commit(message="msg", featuresets=[DomainFeatures, Query])

        if client.is_integration_client():
            assert (
                str(e.value)
                == "Server returned: 500, Dataset MemberActivityDatasetCopy not found in the graph while creating inedges for pipeline DomainFeatures.get_domain_feature"
            )
        else:
            assert (
                str(e.value) == "Dataset `MemberActivityDatasetCopy` "
                "not found in sync call"
            )


@meta(owner="test@fennel.ai")
@featureset
class DomainFeatures2:
    domain: str
    domain_used_count: int

    @extractor()
    @inputs(Query.domain)
    @outputs("domain", "domain_used_count")
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
        @source(
            webhook.endpoint("MemberActivityDataset"),
            disorder="14d",
            cdc="upsert",
        )
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
        @source(webhook.endpoint("MemberDataset"), disorder="14d", cdc="upsert")
        @dataset
        class MemberDataset:
            pk: str
            sk: str
            uid: str = field(key=True)
            email: str
            displayName: str
            createdAt: datetime = field(timestamp=True)

        @meta(owner="test@fennel.ai")
        @dataset(index=True)
        class MemberActivityDatasetCopy:
            domain: str = field(key=True)
            domain_used_count: int
            time: datetime = field(timestamp=True)
            url: str
            uid: str
            transitionType: str
            hasShortcut: bool
            country: str

            @pipeline
            @inputs(MemberActivityDataset)
            def copy(cls, ds: Dataset):
                return ds

        @meta(owner="test@fennel.ai")
        @featureset
        class DomainFeatures:
            domain: str
            domain_used_count: int

            @extractor(deps=[MemberActivityDatasetCopy])
            @inputs(Query.domain)
            @outputs("domain", "domain_used_count")
            def get_domain_feature(cls, ts: pd.Series, domain: pd.Series):
                df, found = MemberActivityDatasetCopy.lookup(  # type: ignore
                    ts, domain=domain
                )
                return df

        with pytest.raises(Exception) as e:
            client.commit(
                message="msg",
                datasets=[
                    MemberActivityDataset,
                    MemberActivityDatasetCopy,
                ],
                featuresets=[DomainFeatures, Query],
            )
            client.query(
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
        client.commit(
            message="msg",
            datasets=[MemberDataset],
            featuresets=[DomainFeatures2, Query],
        )
        with pytest.raises(Exception) as e:
            client.query(
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
            client.commit(
                message="msg",
                datasets=[MemberDataset, MemberActivityDatasetCopy],
                featuresets=[DomainFeatures2, Query],
            )
            client.query(
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
                "Server returned: 500, Dataset MemberActivityDataset not found in the graph while creating inedges for pipeline MemberActivityDatasetCopy@v1.copy"
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

                @pipeline
                @inputs(MemberDataset)
                def del_timestamp(cls, d: Dataset):
                    return d.drop(columns=["createdAt"])

        assert (
            """Field `createdAt` is a key or timestamp field in schema of drop node input '[Dataset:MemberDataset]'. Value fields are: ['pk', 'sk', 'email', 'displayName']"""
        ) == str(e.value)
