from datetime import datetime
from unittest.mock import patch
import os
import pandas as pd

from fennel.test_lib import mock

__owner__ = "nikhil@fennel.ai"


@mock
def test_webhook_basic(client):
    # docsnip webhook_define
    from fennel.sources import source, Webhook
    from fennel.datasets import dataset, field

    webhook = Webhook(name="prod_webhook")

    @source(webhook.endpoint("User"))
    @dataset
    class User:
        uid: int = field(key=True)
        email: str
        timestamp: datetime

    @source(webhook.endpoint("Transaction"))
    @dataset
    class Transaction:
        txid: int
        uid: int
        amount: float
        timestamp: datetime

    # /docsnip

    client.commit(datasets=[User, Transaction])

    # docsnip log_data_sdk
    df = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "email": ["a@gmail.com", "b@gmail.com", "c@gmail.com"],
            "timestamp": [datetime.now(), datetime.now(), datetime.now()],
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
