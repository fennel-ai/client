import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_branches_basic(client):
    from fennel.sources import Webhook, source
    from fennel.datasets import dataset
    from fennel.featuresets import featureset, feature

    webhook = Webhook(name="webhook")

    @source(webhook.endpoint("Dataset"), disorder="14d", cdc="append")
    @dataset
    class Dataset:
        uid: int
        timestamp: datetime

    @featureset
    class SomeFeatureSet:
        uid: int = feature(id=1)

    @featureset
    class OtherFeatureSet:
        uid: int = feature(id=1)

    # docsnip basic
    client.init_branch("branch1")
    client.checkout("branch1")
    client.commit(datasets=[Dataset], featuresets=[SomeFeatureSet])

    client.init_branch("branch2")
    client.checkout("branch2")
    client.commit(datasets=[Dataset], featuresets=[OtherFeatureSet])
    # /docsnip


@mock
def test_branches_clone(client):
    from fennel.sources import Webhook, source
    from fennel.datasets import dataset, field
    from fennel.featuresets import featureset, feature

    webhook = Webhook(name="webhook")

    @source(webhook.endpoint("Dataset"), disorder="14d", cdc="append")
    @dataset
    class SomeDataset:
        uid: int
        timestamp: datetime

    @featureset
    class SomeFeatureset:
        uid: int = feature(id=1)

    # docsnip clone
    client.init_branch("branch1")
    client.checkout("branch1")
    client.commit(datasets=[SomeDataset], featuresets=[SomeFeatureset])

    client.clone_branch("branch1", "branch2")
    client.checkout("branch2")
    # /docsnip
