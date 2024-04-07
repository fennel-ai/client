import os
from datetime import datetime

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_branches_basic(client):
    from fennel.connectors import Webhook, source
    from fennel.datasets import dataset
    from fennel.featuresets import featureset

    webhook = Webhook(name="webhook")

    @source(webhook.endpoint("Dataset"), disorder="14d", cdc="append")
    @dataset
    class Dataset:
        uid: int
        timestamp: datetime

    @featureset
    class SomeFeatureSet:
        uid: int

    @featureset
    class OtherFeatureSet:
        uid: int

    # docsnip basic
    client.init_branch("branch1")
    client.checkout("branch1")  # noop: init_branch checks out new branch
    client.commit(
        message="some module: some git like commit message",
        datasets=[Dataset],
        featuresets=[SomeFeatureSet],
    )

    client.init_branch("branch2")
    client.checkout("branch2")  # noop: init_branch checks out new branch
    client.commit(
        message="another module: some git like commit message",
        datasets=[Dataset],
        featuresets=[OtherFeatureSet],
    )
    # /docsnip


@mock
def test_branches_clone(client):
    from fennel.connectors import Webhook, source
    from fennel.datasets import dataset
    from fennel.featuresets import featureset, feature as F

    webhook = Webhook(name="webhook")

    @source(webhook.endpoint("Dataset"), disorder="14d", cdc="append")
    @dataset
    class SomeDataset:
        uid: int
        timestamp: datetime

    @featureset
    class SomeFeatureset:
        uid: int

    # docsnip clone
    client.init_branch("branch1")
    client.checkout("branch1")
    client.commit(
        message="branch1: initial commit",
        datasets=[SomeDataset],
        featuresets=[SomeFeatureset],
    )

    # docsnip-highlight next-line
    client.clone_branch("branch1", "branch2")
    client.checkout("branch2")
    # /docsnip
