from datetime import datetime

import pandas as pd
from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field, index
    from fennel.connectors import source, Webhook

    # first define & sync a dataset that sources from a webhook
    webhook = Webhook(name="some_webhook")

    @source(webhook.endpoint("some_endpoint"), disorder="14d", cdc="upsert")
    @index
    @dataset
    class Transaction:
        uid: int = field(key=True)
        amount: int
        timestamp: datetime

    client.commit(message="some commit msg", datasets=[Transaction])

    # log some rows to the webhook
    client.log(
        "some_webhook",
        "some_endpoint",
        pd.DataFrame(
            data=[
                [1, 10, "2021-01-01T00:00:00"],
                [2, 20, "2021-02-01T00:00:00"],
            ],
            columns=["uid", "amount", "timestamp"],
        ),
    )

    # now do a lookup to verify that the rows were logged
    keys = pd.DataFrame({"uid": [1, 2, 3]})
    ts = [
        datetime(2021, 1, 1, 0, 0, 0),
        datetime(2021, 2, 1, 0, 0, 0),
        datetime(2021, 3, 1, 0, 0, 0),
    ]
    response, found = client.lookup(
        "Transaction",
        keys=keys,
        timestamps=pd.Series(ts),
    )
    # /docsnip
    assert found.tolist() == [True, True, False]
    # Response is columnar
    assert response["uid"].tolist() == [1, 2, 3]
    assert response["amount"].tolist() == [10, 20, None]
