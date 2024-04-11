from datetime import datetime

import pandas as pd
import pytest

from fennel.datasets import dataset, field, index
from fennel.connectors import source, Webhook
from fennel.testing import mock, InternalTestClient

webhook = Webhook(name="fennel_webhook")
__owner__ = "nikhil@fennel.ai"


def test_featureset_overview():
    # docsnip featureset
    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    @featureset
    class Movie:
        duration: int
        over_2hrs: bool

        @extractor
        @inputs("duration")
        @outputs("over_2hrs")
        def my_extractor(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
            return pd.Series(name="over_2hrs", data=durations > 2 * 3600)

    # /docsnip

    ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    durations = pd.Series([3600, 7200, 7201, 10800])
    res = Movie.my_extractor(Movie, ts, durations)
    assert res.tolist() == [False, False, True, True]


def test_featureset_zero_extractors():
    # docsnip featureset_zero_extractors
    from fennel.featuresets import featureset

    @featureset
    class MoviesZeroExtractors:
        duration: int
        over_2hrs: bool

    # /docsnip


def test_featureset_many_extractors():
    # docsnip featureset_many_extractors
    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    @featureset
    class MoviesManyExtractors:
        duration: int
        over_2hrs: bool
        over_3hrs: bool

        @extractor
        @inputs("duration")
        @outputs("over_2hrs")  # docsnip-highlight
        def e1(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
            return pd.Series(name="over_2hrs", data=durations > 2 * 3600)

        @extractor
        @inputs("duration")
        @outputs("over_3hrs")  # docsnip-highlight
        def e2(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
            return pd.Series(name="over_3hrs", data=durations > 3 * 3600)

    # /docsnip


@mock
def test_multiple_extractors_of_same_feature(client):
    # docsnip featureset_extractors_of_same_feature
    from fennel.featuresets import featureset, extractor
    from fennel.lib import meta, inputs, outputs

    @meta(owner="aditya@xyz.ai")
    @featureset
    class Movies:
        duration: int
        over_2hrs: bool
        # invalid: both e1 & e2 output `over_3hrs`
        over_3hrs: bool

        @extractor(tier=["default"])
        @inputs("duration")
        @outputs("over_2hrs", "over_3hrs")  # docsnip-highlight
        def e1(cls, ts: pd.Series, durations: pd.Series) -> pd.DataFrame:
            two_hrs = durations > 2 * 3600
            three_hrs = durations > 3 * 3600
            return pd.DataFrame({"over_2hrs": two_hrs, "over_3hrs": three_hrs})

        @extractor(tier=["non-default"])
        @inputs("duration")
        @outputs("over_3hrs")  # docsnip-highlight
        def e2(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
            return pd.Series(name="over_3hrs", data=durations > 3 * 3600)

    # /docsnip

    view = InternalTestClient()
    view.add(Movies)
    with pytest.raises(Exception) as e:
        view._get_sync_request_proto()
    assert (
        str(e.value)
        == "Feature `over_3hrs` is extracted by multiple extractors including `e2` in featureset `Movies`."
    )


def test_remote_feature_as_input():
    # docsnip remote_feature_as_input
    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    @featureset
    class Length:
        limit_secs: int

    @featureset
    class MoviesForeignFeatureInput:
        duration: int
        over_limit: bool

        @extractor
        @inputs(Length.limit_secs, "duration")  # docsnip-highlight
        @outputs("over_limit")
        def e(cls, ts: pd.Series, limits: pd.Series, durations: pd.Series):
            return pd.Series(name="over_limit", data=durations > limits)

    # /docsnip


def test_remote_feature_as_output():
    with pytest.raises(Exception):
        # docsnip remote_feature_as_output
        from fennel.featuresets import featureset, extractor
        from fennel.lib import inputs, outputs

        @featureset
        class Request:
            too_long: bool

        @featureset
        class Movies:
            duration: int
            limit_secs: int

            @extractor
            @inputs("limit_secs", "duration")
            @outputs(Request.too_long)  # docsnip-highlight
            def e(cls, ts: pd.Series, limits: pd.Series, durations: pd.Series):
                return pd.Series(name="movie_too_long", data=durations > limits)

        # /docsnip


@pytest.mark.slow
@mock
def test_multiple_features_extracted(client):
    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    @featureset
    class Movie:
        duration: int
        over_2hrs: bool

        # docsnip featureset_extractor
        @extractor
        @inputs("duration")
        @outputs("over_2hrs")
        def my_extractor(cls, ts: pd.Series, durations: pd.Series) -> pd.Series:
            return pd.Series(name="over_2hrs", data=durations > 2 * 3600)

    # /docsnip
    @source(webhook.endpoint("UserInfo"), disorder="14d", cdc="append")
    @index
    @dataset
    class UserInfo:
        uid: int = field(key=True)
        city: str
        update_time: datetime = field(timestamp=True)

    # docsnip multiple_feature_extractor
    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    @featureset
    class UserLocationFeatures:
        uid: int
        latitude: float
        longitude: float

        @extractor(deps=[UserInfo])
        @inputs("uid")
        @outputs("latitude", "longitude")
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

    client.commit(
        message="msg",
        datasets=[UserInfo],
        featuresets=[UserLocationFeatures],
    )
    now = datetime.utcnow()
    data = [[1, "New York", now], [2, "London", now], [3, "Paris", now]]
    df = pd.DataFrame(data, columns=["uid", "city", "update_time"])
    res = client.log("fennel_webhook", "UserInfo", df)
    assert res.status_code == 200

    df = client.query(
        outputs=[UserLocationFeatures],
        inputs=[UserLocationFeatures.uid],
        input_dataframe=pd.DataFrame(
            {"UserLocationFeatures.uid": [1, 2, 3]},
        ),
    )
    expected = pd.DataFrame([[41, 52, 49], [-74, 0, 2]]).T
    expected.columns = [
        "UserLocationFeatures.latitude",
        "UserLocationFeatures.longitude",
    ]
    for col in expected.columns:
        assert all(abs(df - expected) <= 1)


@pytest.mark.slow
@mock
def test_extractors_across_featuresets(client):
    @source(webhook.endpoint("UserInfo"), disorder="14d", cdc="append")
    @index
    @dataset
    class UserInfo:
        uid: int = field(key=True)
        city: str
        update_time: datetime = field(timestamp=True)

    # docsnip extractors_across_featuresets
    from fennel.featuresets import featureset, extractor
    from fennel.lib import inputs, outputs

    @featureset
    class Request:
        uid: int
        request_timestamp: datetime
        ip: str

    @featureset
    class UserLocationFeaturesRefactored:
        uid: int
        latitude: float
        longitude: float

        @extractor(deps=[UserInfo])
        @inputs(Request.uid)
        @outputs("uid", "latitude", "longitude")
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

    client.commit(
        message="some commit message",
        datasets=[UserInfo],
        featuresets=[Request, UserLocationFeaturesRefactored],
    )
    now = datetime.utcnow()
    data = [[1, "New York", now], [2, "London", now], [3, "Paris", now]]
    df = pd.DataFrame(data, columns=["uid", "city", "update_time"])
    res = client.log("fennel_webhook", "UserInfo", df)
    assert res.status_code == 200

    df = client.query(
        outputs=[UserLocationFeaturesRefactored],
        inputs=[Request.uid],
        input_dataframe=pd.DataFrame(
            {"Request.uid": [1, 2, 3]},
        ),
    )

    expected = pd.DataFrame([[41, 52, 49], [-74, 0, 2]]).T
    expected.columns = [
        "UserLocationFeatures.latitude",
        "UserLocationFeatures.longitude",
    ]
    for col in expected.columns:
        assert all(abs(df - expected) <= 1)
