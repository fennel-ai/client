from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.lib.schema import oneof, between, regex
from fennel.sources import source, Webhook
from fennel.test_lib import mock_client


# docsnip dataset_type_restrictions
@meta(owner="test@test.com")
@source(Webhook("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    email: regex(r"[^@]+@[^@]+\.[^@]+")
    timestamp: datetime


# /docsnip


@mock_client
def test_restrictions(client):
    client.sync(datasets=[UserInfoDataset])
    now = datetime.now()
    data = [
        {
            "user_id": 123,
            "name": "Rahul",
            "age": 28,
            "gender": "male",
            "email": "rahul@gmail.com",
            "timestamp": now,
        },
        {
            "user_id": 345,
            "name": "Norah",
            "age": 24,
            "gender": "female",
            "email": "norah@yahoo.com",
            "timestamp": now,
        },
    ]
    res = client.log("UserInfoDataset", pd.DataFrame(data))
    assert res.status_code == 200, res.json()

    data = [
        {
            "user_id": 123,
            "name": "Riya",
            "age": 128,
            "gender": "transgender",
            "email": "riya-gmail.com",
            "timestamp": now,
        },
    ]
    res = client.log("UserInfoDataset", pd.DataFrame(data))
    assert res.status_code == 400, res.json()
