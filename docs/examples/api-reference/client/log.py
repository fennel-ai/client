from datetime import datetime

import pandas as pd

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field
    from fennel.sources import source, Webhook

    # first define & sync a dataset that sources from a webhook
    webhook = Webhook(name="some_webhook")

    @source(webhook.endpoint("some_endpoint"), disorder="14d", cdc="append")
    @dataset
    class Transaction:
        uid: int = field(key=True)
        amount: int
        timestamp: datetime

    client.commit(datasets=[Transaction])

    # log some rows to the webhook
    # docsnip-highlight start
    client.log(
        "some_webhook",
        "some_endpoint",
        df=pd.DataFrame(
            columns=["uid", "amount", "timestamp"],
            data=[
                [1, 10, "2021-01-01T00:00:00"],
                [2, 20, "2021-02-01T00:00:00"],
            ],
        ),
    )
    # docsnip-highlight end
    # /docsnip
    # do lookup to verify that the rows were logged
    ts = [datetime(2021, 1, 1, 0, 0, 0), datetime(2021, 2, 1, 0, 0, 0)]
    df, found = Transaction.lookup(pd.Series(ts), uid=pd.Series([1, 2]))
    assert found.tolist() == [True, True]
    assert df["uid"].tolist() == [1, 2]
    assert df["amount"].tolist() == [10, 20]
    assert df["timestamp"].tolist() == ts
