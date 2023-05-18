from datetime import datetime

import pandas as pd
import pytest
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional

import fennel._vendor.requests as requests
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.sources import source, Webhook
from fennel.test_lib import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def const() -> int:
    return 2


@includes(const)
def square(x: int) -> int:
    return x ** const()


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

    @extractor(depends_on=[UserInfoDataset])
    @includes(power_4, cube)
    @inputs(userid)
    @outputs(age, age_power_four, age_cubed, is_name_common)
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
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


def test_includes_proto_conversion():
    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfoSingleExtractor)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 1
    assert len(sync_request.extractors) == 1
    assert len(sync_request.features) == 5
    extractor_proto = sync_request.extractors[0]
    includes = extractor_proto.pycode.includes
    assert len(includes) == 2
    entry_points = [x.entry_point for x in includes]
    assert set(entry_points) == set(["power_4", "cube"])
    index_of_power_4 = entry_points.index("power_4")
    assert len(includes[index_of_power_4].includes) == 1
    assert includes[index_of_power_4].includes[0].entry_point == "square"

    assert len(includes[index_of_power_4].includes[0].includes) == 1
    assert (
        includes[index_of_power_4].includes[0].includes[0].entry_point
        == "const"
    )


@pytest.mark.integration
@mock
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
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    if client.is_integration_client():
        client.sleep()
    feature_df = client.extract_features(
        output_feature_list=[UserInfoSingleExtractor],
        input_feature_list=[UserInfoSingleExtractor.userid],
        input_dataframe=pd.DataFrame(
            {"UserInfoSingleExtractor.userid": [18232, 18234]}
        ),
    )
    assert feature_df.shape == (2, 5)
    assert feature_df["UserInfoSingleExtractor.age"].tolist() == [32, 24]
    assert feature_df["UserInfoSingleExtractor.age_power_four"].tolist() == [
        1048576,
        331776,
    ]
    assert feature_df["UserInfoSingleExtractor.age_cubed"].tolist() == [
        32768,
        13824,
    ]
