from datetime import datetime

import pandas as pd

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field
    from fennel.connectors import source, Webhook
    from fennel.featuresets import feature as F, featureset, extractor

    webhook = Webhook(name="some_webhook")

    @source(
        webhook.endpoint("endpoint1"),
        disorder="14d",
        cdc="upsert",
        env="bronze",
    )
    @source(
        webhook.endpoint("endpoint2"),
        disorder="14d",
        cdc="upsert",
        env="silver",
    )
    @dataset(index=True)
    class Transaction:
        txid: int = field(key=True)
        amount: int
        timestamp: datetime

    @featureset
    class TransactionFeatures:
        txid: int
        amount: int = F(Transaction.amount, default=0)
        amount_is_high: bool

        @extractor(env="bronze")
        def some_fn(cls, ts, amount: pd.Series):
            return amount.apply(lambda x: x > 100)

    # docsnip-highlight start
    client.commit(
        message="transaction: add transaction dataset and featureset",
        datasets=[Transaction],
        featuresets=[TransactionFeatures],
        preview=False,  # default is False, so didn't need to include this
        env="silver",
    )
    # docsnip-highlight end
    # /docsnip


@mock
def test_incremental(client):
    # docsnip incremental
    from fennel.datasets import dataset, field
    from fennel.connectors import source, Webhook
    from fennel.featuresets import featureset, feature as F, extractor
    from fennel.lib import inputs, outputs

    webhook = Webhook(name="some_webhook")

    @source(webhook.endpoint("endpoint"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class Transaction:
        txid: int = field(key=True)
        amount: int
        timestamp: datetime

    client.commit(
        message="transaction: add transaction dataset",
        datasets=[Transaction],
        incremental=False,  # default is False, so didn't need to include this
    )

    @featureset
    class TransactionFeatures:
        txid: int
        amount: int = F(Transaction.amount, default=0)
        amount_is_high: bool

        @extractor(env="bronze")
        @inputs("amount")
        @outputs("amount_is_high")
        def some_fn(cls, ts, amount: pd.Series):
            return amount.apply(lambda x: x > 100)

    # docsnip-highlight start
    client.commit(
        message="transaction: add transaction featureset",
        datasets=[],  # note: transaction dataset is not included here
        featuresets=[TransactionFeatures],
        incremental=True,  # now we set incremental to True
    )
    # docsnip-highlight end
    # /docsnip
