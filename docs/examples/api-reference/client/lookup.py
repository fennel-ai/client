from datetime import datetime

import pandas as pd
from fennel.test_lib import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import dataset, field
    from fennel.sources import source, Webhook

    # first define & sync a dataset that sources from a webhook
    webhook = Webhook(name="some_webhook")

    @source(webhook.endpoint("some_endpoint"))
    @dataset
    class Transaction:
        uid: int = field(key=True)
        amount: int
        timestamp: datetime

    client.sync(datasets=[Transaction])

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
    keys = [{"uid": 1}, {"uid": 2}, {"uid": 3}]
    ts = [
        datetime(2021, 1, 1, 0, 0, 0),
        datetime(2021, 2, 1, 0, 0, 0),
        datetime(2021, 3, 1, 0, 0, 0),
    ]
    response, found = client.lookup(
        "Transaction",
        keys=keys,
        fields=["uid", "amount"],
        timestamps=ts,
    )
    # /docsnip
    assert found.tolist() == [True, True, False]
    assert response[0] == {"uid": 1, "amount": 10}
    assert response[1] == {"uid": 2, "amount": 20}
    assert response[2] == {"uid": 3, "amount": None}