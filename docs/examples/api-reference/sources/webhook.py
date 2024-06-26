import os
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_webhook_basic(client):
    # docsnip webhook_define
    from fennel.connectors import source, Webhook
    from fennel.datasets import dataset, field

    webhook = Webhook(name="prod_webhook", retention="14d")

    @source(webhook.endpoint("User"), disorder="14d", cdc="upsert")
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
    @dataset
    class Transaction:
        txid: int
        uid: int
        amount: float
        timestamp: datetime

    # /docsnip

    client.commit(message="some commit message", datasets=[User, Transaction])

    # docsnip log_data_sdk
    df = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "email": ["a@gmail.com", "b@gmail.com", "c@gmail.com"],
            "timestamp": [
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
                datetime.now(timezone.utc),
            ],
        }
    )
    client.log("prod_webhook", "User", df)
    # /docsnip

    @patch("requests.post")
    def mocked(mock_post):
        mock_post.return_value.status_code = 200
        os.environ["FENNEL_SERVER_URL"] = "some-url"
        # docsnip log_data_rest_api
        import requests

        url = "{}/api/v1/log".format(os.environ["FENNEL_SERVER_URL"])
        headers = {"Content-Type": "application/json"}
        data = [
            {
                "uid": 1,
                "email": "nikhil@gmail.com",
                "timestamp": 1614556800,
            },
            {
                "uid": 2,
                "email": "abhay@gmail.com",
                "timestamp": 1614556800,
            },
        ]
        req = {
            "webhook": "prod_webhook",
            "endpoint": "User",
            "data": data,
        }
        requests.post(url, headers=headers, data=req)
        # /docsnip

    mocked()
