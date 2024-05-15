from datetime import datetime
from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_tag_propagation(client):

    from fennel.datasets import dataset, field, Dataset, pipeline, Sum
    from fennel.featuresets import featureset, feature as F
    from fennel.lib import inputs, meta
    from fennel.connectors import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    # docsnip tag_propagation
    @source(webhook.endpoint("movie"), disorder="1d", cdc="upsert")
    @meta(tags=["PII"])  # docsnip-highlight
    @dataset(index=True)
    class User:
        uid: int = field(key=True)
        city: str
        updated_at: datetime

    @source(webhook.endpoint("txn"), disorder="1d", cdc="append")
    @meta(tags=["stripe"])  # docsnip-highlight
    @dataset
    class Transaction:
        uid: int
        amount: float
        at: datetime

    @meta(tags=["~PII"])  # docsnip-highlight
    @dataset(index=True)
    class TxnByCity:
        city: str = field(key=True)
        total: float
        at: datetime

        @pipeline
        @inputs(Transaction, User)  # docsnip-highlight
        def fn(cls, txn: Dataset, user: Dataset) -> Dataset:
            joined = txn.join(user, how="inner", on=["uid"])
            return joined.groupby("city").aggregate(
                total=Sum(of="amount", window="forever"),
            )

    @featureset
    class UserFeatures:
        uid: int
        # docsnip-highlight start
        city: str = F(User.city, default="unknown")
        total_in_hometown: float = F(TxnByCity.total, default=0.0)
        # docsnip-highlight end

    # /docsnip
    client.commit(
        message="some commit message",
        datasets=[User, Transaction, TxnByCity],
        featuresets=[UserFeatures],
    )
