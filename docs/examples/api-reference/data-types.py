import pandas as pd
import pytest
from fennel.test_lib import mock


def test_func():
    # docsnip struct_type
    # imports for data types
    from typing import List, Optional
    from datetime import datetime
    from fennel.lib.schema import struct

    # imports for datasets
    from fennel.datasets import dataset, field
    from fennel.lib.metadata import meta

    @struct  # like dataclass but verifies that fields have valid Fennel types
    class Address:
        street: str
        city: str
        state: str
        zip_code: Optional[str]

    @meta(owner="test@test.com")
    @dataset
    class Student:
        id: int = field(key=True)
        name: str
        grades: dict[float] = field().meta(description="map class to grade")
        honors: bool
        classes: List[str]
        address: Address  # Address is now a valid Fennel type
        signup_time: datetime

    # /docsnip


@mock
def test_restrictions(client):
    # docsnip dataset_type_restrictions
    # imports for data types
    from datetime import datetime
    from fennel.lib.schema import oneof, between, regex

    # imports for datasets
    from fennel.datasets import dataset, field
    from fennel.lib.metadata import meta
    from fennel.sources import source, Webhook

    webhook = Webhook(name="fennel_webhook")

    @meta(owner="test@test.com")
    @source(webhook.endpoint("UserInfoDataset"))
    @dataset
    class UserInfoDataset:
        user_id: int = field(key=True)
        name: str
        age: between(int, 0, 100, strict_min=True)
        gender: oneof(str, ["male", "female", "non-binary"])
        email: regex(r"[^@]+@[^@]+\.[^@]+")
        timestamp: datetime

    # /docsnip

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
