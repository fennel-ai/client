from datetime import datetime, timezone, timedelta
from typing import Optional

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.expr import col
from fennel.featuresets import featureset, feature as F
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "eng@fennel.ai"


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    birthdate: datetime
    country: str
    ts: datetime = field(timestamp=True)


@pytest.mark.integration
@mock
def test_now(client):
    from fennel.expr import now

    @featureset
    class UserInfoFeatures:
        user_id: int
        name: Optional[str] = F(UserInfoDataset.name)
        birthdate: Optional[datetime] = F(UserInfoDataset.birthdate)
        age: Optional[int] = F(now().dt.since(col("birthdate"), unit="year"))
        country: Optional[str] = F(UserInfoDataset.country)

    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[UserInfoDataset],
        featuresets=[UserInfoFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()

    now = datetime.now(timezone.utc)
    now_1y = now - timedelta(days=365)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "name": ["Ross", "Monica", "Chandler", "Joey", "Rachel"],
            "birthdate": [
                datetime(1970, 1, 1, tzinfo=timezone.utc),
                datetime(1980, 3, 12, tzinfo=timezone.utc),
                datetime(1990, 5, 15, tzinfo=timezone.utc),
                datetime(1997, 12, 24, tzinfo=timezone.utc),
                datetime(2001, 1, 21, tzinfo=timezone.utc),
            ],
            "country": ["India", "USA", "Africa", "UK", "Chile"],
            "ts": [now_1y, now_1y, now_1y, now_1y, now_1y],
        }
    )
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep()

    # Querying UserInfoFeatures
    df = client.query(
        inputs=[UserInfoFeatures.user_id],
        outputs=[
            UserInfoFeatures.name,
            UserInfoFeatures.age,
            UserInfoFeatures.country,
        ],
        input_dataframe=pd.DataFrame(
            {"UserInfoFeatures.user_id": [1, 2, 3, 4, 5, 6]}
        ),
    )
    assert df.shape == (6, 3)
    assert df["UserInfoFeatures.name"].tolist() == [
        "Ross",
        "Monica",
        "Chandler",
        "Joey",
        "Rachel",
        pd.NA,
    ]
    assert df["UserInfoFeatures.age"].tolist() == [55, 44, 34, 27, 24, pd.NA]
    assert df["UserInfoFeatures.country"].tolist() == [
        "India",
        "USA",
        "Africa",
        "UK",
        "Chile",
        pd.NA,
    ]

    if not client.is_integration_client():
        df = client.query_offline(
            inputs=[UserInfoFeatures.user_id],
            outputs=[
                UserInfoFeatures.name,
                UserInfoFeatures.age,
                UserInfoFeatures.country,
            ],
            input_dataframe=pd.DataFrame(
                {
                    "UserInfoFeatures.user_id": [1, 2, 3, 4, 5, 6],
                    "timestamp": [
                        now_1y,
                        now_1y,
                        now_1y,
                        now_1y,
                        now_1y,
                        now_1y,
                    ],
                }
            ),
            timestamp_column="timestamp",
        )
        assert df.shape == (6, 4)
        assert df["UserInfoFeatures.name"].tolist() == [
            "Ross",
            "Monica",
            "Chandler",
            "Joey",
            "Rachel",
            pd.NA,
        ]
        assert df["UserInfoFeatures.age"].tolist() == [
            54,
            43,
            33,
            26,
            23,
            pd.NA,
        ]
        assert df["UserInfoFeatures.country"].tolist() == [
            "India",
            "USA",
            "Africa",
            "UK",
            "Chile",
            pd.NA,
        ]
