from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field, index
    from fennel.connectors import source, Webhook
    from fennel.featuresets import feature as F, featureset, extractor

    webhook = Webhook(name="some_webhook")

    @source(
        webhook.endpoint("endpoint1"),
        disorder="14d",
        cdc="append",
        tier="bronze",
    )
    @source(
        webhook.endpoint("endpoint2"),
        disorder="14d",
        cdc="append",
        tier="silver",
    )
    @index
    @dataset
    class Transaction:
        txid: int = field(key=True)
        amount: int
        timestamp: datetime

    @featureset
    class TransactionFeatures:
        txid: int
        amount: int = F(Transaction.amount, default=0)
        amount_is_high: bool

        @extractor(tier="bronze")
        def some_fn(cls, ts, amount: pd.Series):
            return amount.apply(lambda x: x > 100)

    # docsnip-highlight start
    client.commit(
        message="transaction: add transaction dataset and featureset",
        datasets=[Transaction],
        featuresets=[TransactionFeatures],
        preview=False,  # default is False, so didn't need to include this
        tier="silver",
        incremental=False,  # default is False, so didn't need to include this
    )
    # docsnip-highlight end
    # /docsnip
