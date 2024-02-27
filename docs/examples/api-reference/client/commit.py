from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field
    from fennel.sources import source, Webhook
    from fennel.featuresets import featureset, feature, extractor

    webhook = Webhook(name="some_webhook")

    @source(webhook.endpoint("endpoint1"), tier="bronze")
    @source(webhook.endpoint("endpoint2"), tier="silver")
    @dataset
    class Transaction:
        txid: int = field(key=True)
        amount: int
        timestamp: datetime

    @featureset
    class TransactionFeatures:
        txid: int = feature(id=1)
        amount: int = feature(id=2).extract(field=Transaction.amount, default=0)
        amount_is_high: bool = feature(id=3)

        @extractor(tier="bronze")
        def some_fn(cls, ts, amount: pd.Series):
            return amount.apply(lambda x: x > 100)

    # docsnip-highlight start
    client.commit(
        datasets=[Transaction],
        featuresets=[TransactionFeatures],
        preview=False,  # default is False, so didn't need to include this
        tier="silver",
    )
    # docsnip-highlight end
    # /docsnip
