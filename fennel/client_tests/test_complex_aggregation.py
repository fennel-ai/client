from datetime import datetime, timezone, date
from decimal import Decimal as PythonDecimal
from typing import Optional

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import Webhook, source
from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Dataset,
    Max,
    Min,
    Average,
    Stddev,
)
from fennel.dtypes import Decimal, Continuous
from fennel.featuresets import featureset, feature as F
from fennel.lib import inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "eng@fennel.ai"


@pytest.mark.integration
@mock
def test_complex_min_max(client):
    @source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="append")
    @dataset
    class UserInfoDataset:
        user_id: int
        birthdate: date
        birthtime: datetime
        country: str
        income: Decimal[2]
        timestamp: datetime = field(timestamp=True)

    @dataset(index=True)
    class CountryDS:
        country: str = field(key=True)
        min_income: Decimal[2]
        max_income: Decimal[2]
        min_birthdate: date
        max_birthdate: date
        min_birthtime: datetime
        max_birthtime: datetime
        timestamp: datetime = field(timestamp=True)

        @pipeline
        @inputs(UserInfoDataset)
        def avg_income_pipeline(cls, event: Dataset):
            return event.groupby("country").aggregate(
                min_income=Min(
                    of="income",
                    window=Continuous("forever"),
                    default=PythonDecimal("1.20"),
                ),
                max_income=Max(
                    of="income",
                    window=Continuous("forever"),
                    default=PythonDecimal("2.20"),
                ),
                min_birthdate=Min(
                    of="birthdate",
                    window=Continuous("forever"),
                    default=date(1970, 1, 1),
                ),
                max_birthdate=Max(
                    of="birthdate",
                    window=Continuous("forever"),
                    default=date(1970, 1, 2),
                ),
                min_birthtime=Min(
                    of="birthtime",
                    window=Continuous("forever"),
                    default=datetime(1970, 1, 1, tzinfo=timezone.utc),
                ),
                max_birthtime=Max(
                    of="birthtime",
                    window=Continuous("forever"),
                    default=datetime(1970, 1, 2, tzinfo=timezone.utc),
                ),
            )

    @featureset
    class CountryFeatures:
        country: str
        min_income: Decimal[2] = F(
            CountryDS.min_income, default=PythonDecimal("1.20")
        )
        max_income: Decimal[2] = F(
            CountryDS.max_income, default=PythonDecimal("2.20")
        )
        min_birthdate: date = F(
            CountryDS.min_birthdate, default=date(1970, 1, 1)
        )
        max_birthdate: date = F(
            CountryDS.max_birthdate, default=date(1970, 1, 2)
        )
        min_birthtime: datetime = F(
            CountryDS.min_birthtime,
            default=datetime(1970, 1, 1, tzinfo=timezone.utc),
        )
        max_birthtime: datetime = F(
            CountryDS.max_birthtime,
            default=datetime(1970, 1, 2, tzinfo=timezone.utc),
        )

    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[UserInfoDataset, CountryDS],
        featuresets=[CountryFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep(30)

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "country": ["India", "USA", "India", "USA", "UK"],
            "birthdate": [
                date(1980, 1, 1),
                date(1990, 2, 11),
                date(2000, 3, 15),
                date(1997, 5, 22),
                date(1987, 6, 6),
            ],
            "birthtime": [
                datetime(1980, 1, 1, tzinfo=timezone.utc),
                datetime(1990, 2, 11, tzinfo=timezone.utc),
                datetime(2000, 3, 15, tzinfo=timezone.utc),
                datetime(1997, 5, 22, tzinfo=timezone.utc),
                datetime(1987, 6, 6, tzinfo=timezone.utc),
            ],
            "income": [
                PythonDecimal("1200.10"),
                PythonDecimal("1000.10"),
                PythonDecimal("1400.10"),
                PythonDecimal("90.10"),
                PythonDecimal("1100.10"),
            ],
            "timestamp": [now, now, now, now, now],
        }
    )
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep()

    # Querying UserInfoFeatures
    df = client.query(
        inputs=[CountryFeatures.country],
        outputs=[CountryFeatures],
        input_dataframe=pd.DataFrame(
            {"CountryFeatures.country": ["India", "USA", "UK", "China"]}
        ),
    )
    assert df.shape == (4, 7)
    assert df["CountryFeatures.country"].tolist() == [
        "India",
        "USA",
        "UK",
        "China",
    ]
    assert df["CountryFeatures.min_income"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("90.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("1.20"),
    ]
    assert df["CountryFeatures.max_income"].tolist() == [
        PythonDecimal("1400.10"),
        PythonDecimal("1000.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("2.20"),
    ]
    assert df["CountryFeatures.min_birthdate"].tolist() == [
        date(1980, 1, 1),
        date(1990, 2, 11),
        date(1987, 6, 6),
        date(1970, 1, 1),
    ]
    assert df["CountryFeatures.max_birthdate"].tolist() == [
        date(2000, 3, 15),
        date(1997, 5, 22),
        date(1987, 6, 6),
        date(1970, 1, 2),
    ]
    assert df["CountryFeatures.min_birthtime"].tolist() == [
        datetime(1980, 1, 1, tzinfo=timezone.utc),
        datetime(1990, 2, 11, tzinfo=timezone.utc),
        datetime(1987, 6, 6, tzinfo=timezone.utc),
        datetime(1970, 1, 1, tzinfo=timezone.utc),
    ]
    assert df["CountryFeatures.max_birthtime"].tolist() == [
        datetime(2000, 3, 15, tzinfo=timezone.utc),
        datetime(1997, 5, 22, tzinfo=timezone.utc),
        datetime(1987, 6, 6, tzinfo=timezone.utc),
        datetime(1970, 1, 2, tzinfo=timezone.utc),
    ]


@pytest.mark.integration
@mock
def test_none_default(client):
    @source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="append")
    @dataset
    class UserInfoDataset:
        user_id: int
        country: str
        income: float
        timestamp: datetime = field(timestamp=True)

    @dataset(index=True)
    class CountryDS:
        country: str = field(key=True)
        min_income: Optional[float]
        max_income: Optional[float]
        avg_income: Optional[float]
        stddev_income: Optional[float]
        timestamp: datetime = field(timestamp=True)

        @pipeline
        @inputs(UserInfoDataset)
        def avg_income_pipeline(cls, event: Dataset):
            return event.groupby("country").aggregate(
                min_income=Min(
                    of="income",
                    window=Continuous("forever"),
                    default=None,
                ),
                max_income=Max(
                    of="income",
                    window=Continuous("forever"),
                    default=None,
                ),
                avg_income=Average(
                    of="income",
                    window=Continuous("forever"),
                    default=None,
                ),
                stddev_income=Stddev(
                    of="income",
                    window=Continuous("forever"),
                    default=None,
                ),
            )

    @featureset
    class CountryFeatures:
        country: str
        min_income: float = F(CountryDS.min_income, default=1.20)
        max_income: float = F(CountryDS.max_income, default=2.20)
        avg_income: float = F(CountryDS.avg_income, default=1.20)
        stddev_income: Optional[float] = F(CountryDS.stddev_income)

    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[UserInfoDataset, CountryDS],
        featuresets=[CountryFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep(30)

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "country": ["India", "USA", "India", "USA", "UK"],
            "income": [
                1200.10,
                1000.10,
                1400.10,
                90.10,
                1100.10,
            ],
            "timestamp": [now, now, now, now, now],
        }
    )
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()

    client.sleep()

    df = client.query(
        inputs=[CountryFeatures.country],
        outputs=[CountryFeatures],
        input_dataframe=pd.DataFrame(
            {"CountryFeatures.country": ["India", "USA", "UK", "China"]}
        ),
    )
    assert df.shape == (4, 5)
    assert df["CountryFeatures.country"].tolist() == [
        "India",
        "USA",
        "UK",
        "China",
    ]
    assert df["CountryFeatures.min_income"].tolist() == [
        1200.10,
        90.10,
        1100.10,
        1.20,
    ]
    assert df["CountryFeatures.max_income"].tolist() == [
        1400.10,
        1000.10,
        1100.10,
        2.20,
    ]
    assert df["CountryFeatures.avg_income"].tolist() == [
        1300.1,
        545.1,
        1100.1,
        1.2,
    ]
    assert df["CountryFeatures.stddev_income"].tolist() == [
        100.0,
        455.0,
        0,
        pd.NA,
    ]
