from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field
from fennel.featuresets import extractor
from fennel.featuresets import feature, featureset
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.test_lib import mock_client


# docsnip featureset
@featureset
class Movies:
    duration: int = feature(id=1)
    over_2hrs: bool = feature(id=2)

    @extractor
    @inputs(duration)
    @outputs(over_2hrs)
    def my_extractor(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
        return pd.Series(name="over_2hrs", data=durations > 2 * 3600)


# /docsnip


def test_featureset():
    ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    durations = pd.Series([3600, 7200, 7201, 10800])
    res = Movie.my_extractor(Movie, ts, durations)
    assert res.tolist() == [False, False, True, True]


# docsnip featureset_zero_extractors
@featureset
class MoviesZeroExtractors:
    duration: int = feature(id=1)
    over_2hrs: bool = feature(id=2)


# /docsnip


# docsnip featureset_many_extractors
@featureset
class MoviesManyExtractors:
    duration: int = feature(id=1)
    over_2hrs: bool = feature(id=2)
    over_3hrs: bool = feature(id=3)

    @extractor
    @inputs(duration)
    @outputs(over_2hrs)
    def e1(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
        return pd.Series(name="over_2hrs", data=durations > 2 * 3600)

    @extractor
    @inputs(duration)
    @outputs(over_3hrs)
    def e2(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
        return pd.Series(name="over_3hrs", data=durations > 3 * 3600)


# /docsnip


def test_multiple_extractors_of_same_feature():
    with pytest.raises(Exception):
        # docsnip featureset_extractors_of_same_feature
        @featureset
        class Movies:
            duration: int = feature(id=1)
            over_2hrs: bool = feature(id=2)
            # invalid: both e1 & e2 output `over_3hrs`
            over_3hrs: bool = feature(id=3)

            @extractor
            @inputs(duration)
            @outputs(over_2hrs, over_3hrs)
            def e1(cls, ts: pd.Series, durations: pd.Series) -> pd.DataFrame:
                two_hrs = durations > 2 * 3600
                three_hrs = durations > 3 * 3600
                return pd.DataFrame(
                    {"over_2hrs": two_hrs, "over_3hrs": three_hrs}
                )

            @extractor
            @inputs(duration)
            @outputs(over_3hrs)
            def e2(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
                return pd.Series(name="over_3hrs", data=durations > 3 * 3600)


# /docsnip


# docsnip remote_feature_as_input
@featureset
class Length:
    limit_secs: int = feature(id=1)


@featureset
class MoviesForeignFeatureInput:
    duration: int = feature(id=1)
    over_limit: bool = feature(id=2)

    @extractor
    @inputs(Length.limit_secs, duration)
    @outputs(over_limit)
    def e(cls, ts: pd.Series, limits: pd.Series, durations: pd.Series):
        return pd.Series(name="over_limit", data=durations > limits)


# /docsnip


def test_remote_feature_as_output():
    with pytest.raises(Exception):
        # docsnip remote_feature_as_output
        @featureset
        class Request:
            too_long: bool = feature(id=1)

        @featureset
        class Movies:
            duration: int = feature(id=1)
            limit_secs: int = feature(id=2)

            @extractor
            @inputs(limit_secs, duration)
            @outputs(
                Request.too_long
            )  # can not output feature of another featureset
            def e(cls, ts: pd.Series, limits: pd.Series, durations: pd.Series):
                return pd.Series(name="movie_too_long", data=durations > limits)


# /docsnip


@featureset
class Movie:
    duration: int = feature(id=1)
    over_2hrs: bool = feature(id=2)

    # docsnip featureset_extractor
    @extractor
    @inputs(duration)
    @outputs(over_2hrs)
    def my_extractor(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
        return pd.Series(name="over_2hrs", data=durations > 2 * 3600)

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

    @extractor(depends_on=[UserInfo])
    @inputs(uid)
    @outputs(latitude, longitude)
    def get_user_city_coordinates(cls, ts: pd.Series, uid: pd.Series):
        from geopy.geocoders import Nominatim

        df, found = UserInfo.lookup(ts, uid=uid)
        # Fetch the coordinates using a geocoding service / API.
        # If the service is down, use some dummy coordinates as fallback.
        try:
            geolocator = Nominatim(user_agent="adityanambiar@fennel.ai")
            coordinates = (
                df["city"]
                .apply(geolocator.geocode)
                .apply(lambda x: (x.latitude, x.longitude))
            )
        except Exception:
            coordinates = pd.Series([(41, -74), (52, -0), (49, 2)])
        df["latitude"] = coordinates.apply(lambda x: round(x[0], 1))
        df["longitude"] = coordinates.apply(lambda x: round(x[1], 1))
        return df[["latitude", "longitude"]]


# /docsnip


@pytest.mark.slow
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
    assert df["UserLocationFeatures.latitude"].round(0).tolist() == [
        41,
        52,
        49,
    ]
    assert df["UserLocationFeatures.longitude"].round(0).tolist() == [
        -74,
        0,
        2,
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

    @extractor(depends_on=[UserInfo])
    @inputs(Request.uid)
    @outputs(uid, latitude, longitude)
    def get_country_geoid(cls, ts: pd.Series, uid: pd.Series):
        from geopy.geocoders import Nominatim

        df, found = UserInfo.lookup(ts, uid=uid)
        # Fetch the coordinates using a geocoding service / API.
        # If the service is down, use some dummy coordinates as fallback.
        try:
            geolocator = Nominatim(user_agent="adityanambiar@fennel.ai")
            coordinates = (
                df["city"]
                .apply(geolocator.geocode)
                .apply(lambda x: (x.latitude, x.longitude))
            )
        except Exception:
            coordinates = pd.Series([(41, -74), (52, -0), (49, 2)])
        df["uid"] = uid
        df["latitude"] = coordinates.apply(lambda x: round(x[0], 0))
        df["longitude"] = coordinates.apply(lambda x: round(x[1], 0))
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
        41,
        52,
        49,
    ]
    assert df["UserLocationFeaturesRefactored.longitude"].tolist() == [
        -74,
        0,
        2,
    ]
