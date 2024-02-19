from datetime import datetime

import pandas as pd
import pytest
from fennel.datasets import dataset, field
from fennel.lib import meta
from fennel.dtype import oneof, between, regex, struct
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")


# docsnip struct_type
@struct  # like dataclass but verifies that all fields are valid Fennel types
class Address:
    street: str
    city: str
    state: str
    zip_code: str


@meta(owner="test@test.com")
@dataset
class Student:
    id: int = field(key=True)
    name: str
    age: int
    address: Address  # Address is now a valid Fennel type for datasets/features
    signup_time: datetime


# /docsnip


# docsnip dataset_type_restrictions
@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: between(int, 0, 100)
    gender: oneof(str, ["male", "female", "non-binary"])
    email: regex(r"[^@]+@[^@]+\.[^@]+")
    timestamp: datetime


# /docsnip


@mock
def test_restrictions(client):
    client.commit(datasets=[UserInfoDataset])
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
    res = client.log("fennel_webhook", "UserInfoDataset", pd.DataFrame(data))
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
    with pytest.raises(Exception):
        client.log("fennel_webhook", "UserInfoDataset", pd.DataFrame(data))
