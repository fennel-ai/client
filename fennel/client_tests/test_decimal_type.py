import sys
from datetime import datetime, timezone
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
    Average,
    Sum,
    Max,
    Min,
    Stddev,
    Quantile,
)
from fennel.dtypes import Decimal, Continuous
from fennel.featuresets import featureset, feature as F
from fennel.lib import inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "eng@fennel.ai"


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: int
    country: str
    net_worth: Decimal[2]
    income: Decimal[2]
    timestamp: datetime = field(timestamp=True)


@dataset(index=True)
class CountryIncomeDataset:
    country: str = field(key=True)
    avg: float
    sum: Decimal[2]
    min: Decimal[2]
    max: Decimal[2]
    stddev: float
    median: float
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(UserInfoDataset)
    def avg_income_pipeline(cls, event: Dataset):
        return event.groupby("country").aggregate(
            Average(
                of="income", into_field="avg", window=Continuous("forever")
            ),
            Sum(of="income", into_field="sum", window=Continuous("forever")),
            Min(
                of="income",
                into_field="min",
                window=Continuous("forever"),
                default=0.0,
            ),
            Max(
                of="income",
                into_field="max",
                window=Continuous("forever"),
                default=0.0,
            ),
            Stddev(
                of="income",
                into_field="stddev",
                window=Continuous("forever"),
                default=0.0,
            ),
            Quantile(
                of="income",
                into_field="median",
                window=Continuous("forever"),
                approx=True,
                default=0.0,
                p=0.5,
            ),
        )


@featureset
class UserInfoFeatures:
    user_id: int
    name: str = F(UserInfoDataset.name, default="None")
    age: int = F(UserInfoDataset.age, default=-1)
    country: str = F(UserInfoDataset.country, default="None")
    income: Decimal[2] = F(UserInfoDataset.income, default=0.0)
    net_worth: Decimal[2] = F(UserInfoDataset.net_worth, default=0.0)


@featureset
class CountryIncomeFeatures:
    country: str
    avg: float = F(CountryIncomeDataset.avg, default=0.0)
    sum: Decimal[2] = F(CountryIncomeDataset.sum, default=0.0)
    min: Decimal[2] = F(CountryIncomeDataset.min, default=0.0)
    max: Decimal[2] = F(CountryIncomeDataset.max, default=0.0)
    stddev: float = F(CountryIncomeDataset.stddev, default=0.0)
    median: float = F(CountryIncomeDataset.median, default=0.0)


@pytest.mark.integration
@mock
def test_sync_with_decimal_type(client):
    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[UserInfoDataset, CountryIncomeDataset],
        featuresets=[UserInfoFeatures, CountryIncomeFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_log_with_decimal_type(client):
    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[UserInfoDataset, CountryIncomeDataset],
        featuresets=[UserInfoFeatures, CountryIncomeFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "name": ["Ross", "Monica", "Chandler", "Joey", "Rachel"],
            "age": [31, 32, 33, 34, 35],
            "country": ["India", "USA", "Africa", "UK", "Chile"],
            "net_worth": [
                1200.10,
                1000.10,
                1400.10,
                90.10,
                1100.10,
            ],
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

    sample = client.inspect("UserInfoDataset", n=1)
    assert len(sample) == 1
    sample = client.inspect("UserInfoDataset")
    assert len(sample) == 5

    sample = client.inspect("CountryIncomeDataset", n=1)
    assert len(sample) == 1
    sample = client.inspect("CountryIncomeDataset")
    assert len(sample) == 5


@pytest.mark.integration
@mock
def test_query_with_decimal_type(client):
    # Sync the dataset
    response = client.commit(
        message="msg",
        datasets=[UserInfoDataset, CountryIncomeDataset],
        featuresets=[UserInfoFeatures, CountryIncomeFeatures],
    )
    assert response.status_code == requests.codes.OK, response.json()

    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "user_id": [1, 2, 3, 4, 5],
            "name": ["Ross", "Monica", "Chandler", "Joey", "Rachel"],
            "age": [31, 32, 33, 34, 35],
            "country": ["India", "USA", "Africa", "UK", "Chile"],
            "net_worth": [
                1200.10,
                1000.10,
                1400.10,
                90.10,
                1100.10,
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
        inputs=[UserInfoFeatures.user_id],
        outputs=[
            UserInfoFeatures.name,
            UserInfoFeatures.age,
            UserInfoFeatures.country,
            UserInfoFeatures.income,
            UserInfoFeatures.net_worth,
        ],
        input_dataframe=pd.DataFrame(
            {"UserInfoFeatures.user_id": [1, 2, 3, 4, 5, 6]}
        ),
    )
    assert df.shape == (6, 5)
    assert df["UserInfoFeatures.name"].tolist() == [
        "Ross",
        "Monica",
        "Chandler",
        "Joey",
        "Rachel",
        "None",
    ]
    assert df["UserInfoFeatures.age"].tolist() == [31, 32, 33, 34, 35, -1]
    assert df["UserInfoFeatures.country"].tolist() == [
        "India",
        "USA",
        "Africa",
        "UK",
        "Chile",
        "None",
    ]
    assert df["UserInfoFeatures.income"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("1000.10"),
        PythonDecimal("1400.10"),
        PythonDecimal("90.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("0.00"),
    ]
    assert df["UserInfoFeatures.net_worth"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("1000.10"),
        PythonDecimal("1400.10"),
        PythonDecimal("90.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("0.00"),
    ]

    # Querying CountryIncomeFeatures
    df = client.query(
        inputs=[CountryIncomeFeatures.country],
        outputs=[
            CountryIncomeFeatures.min,
            CountryIncomeFeatures.max,
            CountryIncomeFeatures.avg,
            CountryIncomeFeatures.sum,
            CountryIncomeFeatures.median,
            CountryIncomeFeatures.stddev,
        ],
        input_dataframe=pd.DataFrame(
            {
                "CountryIncomeFeatures.country": [
                    "India",
                    "USA",
                    "Africa",
                    "UK",
                    "Chile",
                    "None",
                ]
            }
        ),
    )
    assert df.shape == (6, 6)
    assert df["CountryIncomeFeatures.avg"].tolist() == [
        pytest.approx(1200.1),
        pytest.approx(1000.1),
        pytest.approx(1400.1),
        pytest.approx(90.1),
        pytest.approx(1100.1),
        pytest.approx(0),
    ]
    assert df["CountryIncomeFeatures.min"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("1000.10"),
        PythonDecimal("1400.10"),
        PythonDecimal("90.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("0.00"),
    ]
    assert df["CountryIncomeFeatures.max"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("1000.10"),
        PythonDecimal("1400.10"),
        PythonDecimal("90.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("0.00"),
    ]
    assert df["CountryIncomeFeatures.sum"].tolist() == [
        PythonDecimal("1200.10"),
        PythonDecimal("1000.10"),
        PythonDecimal("1400.10"),
        PythonDecimal("90.10"),
        PythonDecimal("1100.10"),
        PythonDecimal("0.00"),
    ]
    if not client.is_integration_client():
        assert [
            round(x, 1) for x in df["CountryIncomeFeatures.median"].tolist()
        ] == [
            1200.1,
            1000.1,
            1400.1,
            90.1,
            1100.1,
            0,
        ]
    assert [
        round(x, 1) for x in df["CountryIncomeFeatures.stddev"].tolist()
    ] == [
        0,
        0,
        0,
        0,
        0,
        0,
    ]


@pytest.mark.integration
@pytest.mark.skip
@mock
def test_optional_decimal_type(client):
    if sys.version_info >= (3, 11):

        @featureset
        class Features:
            user_id: int
            name: str = F(UserInfoDataset.name, default="None")
            age: int = F(UserInfoDataset.age, default=-1)
            country: str = F(UserInfoDataset.country, default="None")
            income: Optional[Decimal[2]] = F(UserInfoDataset.income)
            net_worth: Optional[Decimal[2]] = F(UserInfoDataset.net_worth)

        response = client.commit(
            datasets=[UserInfoDataset],
            featuresets=[Features],
            message="first-commit",
        )
        assert response.status_code == requests.codes.OK, response.json()

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "name": ["Ross", "Monica", "Chandler", "Joey", "Rachel"],
                "age": [31, 32, 33, 34, 35],
                "country": ["India", "USA", "Africa", "UK", "Chile"],
                "net_worth": [
                    1200.10,
                    1000.10,
                    1400.10,
                    90.10,
                    1100.10,
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
            inputs=[Features.user_id],
            outputs=[
                Features.income,
                Features.net_worth,
            ],
            input_dataframe=pd.DataFrame(
                {"Features.user_id": [1, 2, 3, 4, 5, 6]}
            ),
        )
        assert df.shape == (6, 2)
        assert df["Features.income"].tolist() == [
            PythonDecimal("1200.10"),
            PythonDecimal("1000.10"),
            PythonDecimal("1400.10"),
            PythonDecimal("90.10"),
            PythonDecimal("1100.10"),
            pd.NA,
        ]
        assert df["Features.net_worth"].tolist() == [
            PythonDecimal("1200.10"),
            PythonDecimal("1000.10"),
            PythonDecimal("1400.10"),
            PythonDecimal("90.10"),
            PythonDecimal("1100.10"),
            pd.NA,
        ]
