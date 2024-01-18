import pandas as pd
from fraud.datasets.vehicle import (
    IdToMarketAreaDS,
    LocationToNewMarketArea,
    LocationDS2,
    LocationDS,
    VehicleSummary,
    MarketAreaDS,
)
from fraud.featuresets.driver import Request
from fraud.featuresets.vehicle import VehicleFS

from fennel.test_lib import mock


@mock
def test_vehicle_features(client):
    sync_response = client.sync(
        datasets=[
            IdToMarketAreaDS,
            LocationToNewMarketArea,
            LocationDS2,
            LocationDS,
            VehicleSummary,
            MarketAreaDS,
        ],
        featuresets=[VehicleFS, Request],
        tier="local",
    )
    assert sync_response.status_code == 200, sync_response.json()

    df = (
        pd.read_csv("examples/fraud/data/tbd/location.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(latitude=lambda x: x["latitude"].astype(float))
        .assign(longitude=lambda x: x["longitude"].astype(float))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="LocationDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/tbd/location_to_new_market_area.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(latitude=lambda x: x["latitude"].astype(float))
        .assign(longitude=lambda x: x["longitude"].astype(float))
        .assign(gid=lambda x: x["gid"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="LocationToNewMarketArea",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/vehicle/vehicle_summary.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(vehicle_id=lambda x: x["vehicle_id"].astype(int))
        .assign(latitude=lambda x: x["latitude"].astype(int))
        .assign(longitude=lambda x: x["longitude"].astype(float))
        .assign(location_id=lambda x: x["location_id"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="VehicleSummary",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    feature_df = client.extract_features(
        output_feature_list=[VehicleFS],
        input_feature_list=[Request.vehicle_id],
        input_dataframe=pd.DataFrame(
            {"Request.vehicle_id": [1027415, 1145620, 900208]}
        ),
    )
    assert feature_df.shape == (3, 3)
    assert feature_df["VehicleFS.market_area_id"].to_list() == [0, 0, 0]
    assert feature_df["VehicleFS.vehicle_state"].to_list() == [
        "Unknown",
        "Unknown",
        "Unknown",
    ]
