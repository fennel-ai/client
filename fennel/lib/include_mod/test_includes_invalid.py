import time
from datetime import datetime
from typing import Optional

import pandas as pd
import pytest
import requests

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.lib.include_mod import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import mock_client


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def square(x: int) -> int:
    return x**2


def cube(x: int) -> int:
    return x**3


@includes(square)
def power_4(x: int) -> int:
    return square(square(x))


@meta(owner="test@test.com")
@featureset
class UserInfoSingleExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_power_four: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    @depends_on(UserInfoDataset)
    def get_user_info(
        cls, ts: Series[datetime], user_id: Series[userid]
    ) -> DataFrame[age, age_power_four, age_cubed, is_name_common]:
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df[str(cls.userid)] = user_id
        df[str(cls.age_power_four)] = power_4(df["age"])
        df[str(cls.age_cubed)] = cube(df["age"])
        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                str(cls.age),
                str(cls.age_power_four),
                str(cls.age_cubed),
                str(cls.is_name_common),
            ]
        ]


@pytest.mark.integration
@mock_client
def test_simple_extractor(client):
    client.sync(
        datasets=[UserInfoDataset],
        featuresets=[UserInfoSingleExtractor],
    )
    now = datetime.utcnow()
    data = [
        [18232, "John", 32, "USA", now],
        [18234, "Monica", 24, "Chile", now],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    response = client.log("UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    if client.is_integration_client():
        time.sleep(5)
    with pytest.raises(Exception) as e:
        client.extract_features(
            output_feature_list=[UserInfoSingleExtractor],
            input_feature_list=[UserInfoSingleExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoSingleExtractor.userid": [18232, 18234]}
            ),
        )
    assert (
        str(e.value)
        == "Extractor `get_user_info` in `UserInfoSingleExtractor` "
        "failed to run with error: name 'power_4' is not defined. "
    )
