from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field
from fennel.featuresets import extractor, depends_on
from fennel.featuresets import feature, featureset
from fennel.lib.metadata import meta
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import mock_client


# docsnip featureset
@featureset
class Movies:
    duration: int = feature(id=1)
    over_2hrs: bool = feature(id=2)

    @extractor
    def my_extractor(
            cls, ts: Series[datetime], durations: Series[duration]
    ) -> Series[over_2hrs]:
        return durations > 2 * 3600


# /docsnip


def test_featureset():
    ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    durations = pd.Series([3600, 7200, 7201, 10800])
    res = Movie.my_extractor(Movie, ts, durations)
    assert res.tolist() == [False, False, True, True]


@featureset
class Movie:
    duration: int = feature(id=1)
    over_2hrs: bool = feature(id=2)

    # docsnip featureset_extractor
    @extractor
    def my_extractor(
            cls, ts: Series[datetime], durations: Series[duration]
    ) -> Series[over_2hrs]:
        return durations > 2 * 3600

    # /docsnip


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class UserInfo:
    uid: int = field(key=True)
    city: str
    update_time: datetime = field(timestamp=True)


# docsnip multiple_feature_extractor
@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserLocationFeatures:
    uid: int = feature(id=1)
    latitude: float = feature(id=2)
    longitude: float = feature(id=3)

    @extractor
    @depends_on(UserInfo)
    def get_user_city_coordinates(
            cls, ts: Series[datetime], uid: Series[uid]
    ) -> DataFrame[latitude, longitude]:
        from geopy.geocoders import Nominatim

        df, found = UserInfo.lookup(ts, uid=uid)
        geolocator = Nominatim(user_agent="adityanambiar@fennel.ai")
        coordinates = (
            df["city"]
            .apply(geolocator.geocode)
            .apply(lambda x: (x.latitude, x.longitude))
        )
        df["latitude"] = coordinates.apply(lambda x: round(x[0], 1))
        df["longitude"] = coordinates.apply(lambda x: round(x[1], 1))
        return df[["latitude", "longitude"]]


# /docsnip


@mock_client
def test_multiple_features_extracted(client):
    client.sync(datasets=[UserInfo], featuresets=[UserLocationFeatures])
    now = datetime.now()
    data = [[1, "New York", now], [2, "London", now], [3, "Paris", now]]
    df = pd.DataFrame(data, columns=["uid", "city", "update_time"])
    res = client.log("UserInfo", df)
    assert res.status_code == 200

    df = client.extract_features(
        output_feature_list=[UserLocationFeatures],
        input_feature_list=[UserLocationFeatures.uid],
        input_dataframe=pd.DataFrame(
            {"UserLocationFeatures.uid": [1, 2, 3]},
        ),
    )
    assert df["UserLocationFeatures.latitude"].round(1).tolist() == [
        40.7,
        51.5,
        48.8,
    ]
    assert df["UserLocationFeatures.longitude"].round(1).tolist() == [
        -74.0,
        -0.1,
        2.4,
    ]


# docsnip extractors_across_featuresets
@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class Request:
    uid: int = feature(id=1)
    request_timestamp: datetime = feature(id=2)
    ip: str = feature(id=3)


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserLocationFeaturesRefactored:
    uid: int = feature(id=1)
    latitude: float = feature(id=2)
    longitude: float = feature(id=3)

    @extractor
    @depends_on(UserInfo)
    def get_country_geoid(
            cls, ts: Series[datetime], uid: Series[Request.uid]
    ) -> DataFrame[uid, latitude, longitude]:
        from geopy.geocoders import Nominatim

        df, found = UserInfo.lookup(ts, uid=uid)
        geolocator = Nominatim(user_agent="adityanambiar@fennel.ai")
        coordinates = (
            df["city"]
            .apply(geolocator.geocode)
            .apply(lambda x: (x.latitude, x.longitude))
        )
        df["uid"] = uid
        df["latitude"] = coordinates.apply(lambda x: round(x[0], 1))
        df["longitude"] = coordinates.apply(lambda x: round(x[1], 1))
        return df[["uid", "latitude", "longitude"]]


# /docsnip


@pytest.mark.slow
@mock_client
def test_extractors_across_featuresets(client):
    client.sync(
        datasets=[UserInfo],
        featuresets=[Request, UserLocationFeaturesRefactored],
    )
    now = datetime.now()
    data = [[1, "New York", now], [2, "London", now], [3, "Paris", now]]
    df = pd.DataFrame(data, columns=["uid", "city", "update_time"])
    res = client.log("UserInfo", df)
    assert res.status_code == 200

    df = client.extract_features(
        output_feature_list=[UserLocationFeaturesRefactored],
        input_feature_list=[Request.uid],
        input_dataframe=pd.DataFrame(
            {"Request.uid": [1, 2, 3]},
        ),
    )

    assert df["UserLocationFeaturesRefactored.latitude"].tolist() == [
        40.7,
        51.5,
        48.9,
    ]
    assert df["UserLocationFeaturesRefactored.longitude"].tolist() == [
        -74.0,
        -0.1,
        2.3,
    ]
