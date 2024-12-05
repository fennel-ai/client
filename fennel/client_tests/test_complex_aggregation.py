from datetime import datetime, timezone, date
from decimal import Decimal as PythonDecimal

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
