from datetime import datetime

import pandas as pd
from fraud.datasets.velocity import (
    CancelledTripsDS,
    PastApprovedDS,
    CheckoutPagesLastDayDS,
    LoginsLastDayDS,
    NumCompletedTripsDS,
    ReservationDS,
    LoginEventsDS,
    ReservationSummaryDS,
    BookingFlowCheckoutPageDS,
)
from fraud.featuresets.driver import Request
from fraud.featuresets.velocity import DriverVelocityFS

from fennel.testing import mock


@mock
def test_velocity_features(client):
    sync_response = client.commit(
        message="sync velocity features",
        datasets=[
            BookingFlowCheckoutPageDS,
            ReservationDS,
            NumCompletedTripsDS,
            LoginEventsDS,
            LoginsLastDayDS,
            CheckoutPagesLastDayDS,
            PastApprovedDS,
            ReservationSummaryDS,
            CancelledTripsDS,
        ],
        featuresets=[DriverVelocityFS, Request],
    )
    assert sync_response.status_code == 200

    df = (
        pd.read_csv("examples/fraud/data/driver/reservation.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(vehicle_id=lambda x: x["vehicle_id"].astype(int))
        .assign(reservation_id=lambda x: x["reservation_id"].astype(int))
        .assign(driver_id=lambda x: x["driver_id"].astype(int))
        .assign(is_completed_trip=lambda x: x["is_completed_trip"].astype(int))
        .assign(current_status=lambda x: x["current_status"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="ReservationDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/driver/booking_flow_checkout_page.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(driver_id=lambda x: x["driver_id"].astype("Int64"))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="BookingFlowCheckoutPageDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/driver/reservation_summary.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(
            trip_end_ts=lambda x: pd.to_datetime(x["trip_end_ts"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(driver_id=lambda x: x["driver_id"].astype(int))
        .assign(reservation_id=lambda x: x["reservation_id"].astype(int))
        .assign(vehicle_id=lambda x: x["vehicle_id"].astype(int))
        .assign(is_ever_approved=lambda x: x["is_ever_approved"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook", endpoint="ReservationSummaryDS", df=df
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/driver/login_event.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(driver_id=lambda x: x["driver_id"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="LoginEventsDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    feature_df = client.query(
        inputs=["Request.driver_id"],
        input_dataframe=pd.DataFrame(
            {
                "Request.driver_id": [
                    23315855,
                    23348774,
                ],
            }
        ),
        outputs=[DriverVelocityFS],
    )
    assert feature_df.shape == (2, 6)
    assert feature_df["DriverVelocityFS.driver_id"].to_list() == [
        23315855,
        23348774,
    ]
    assert feature_df[
        "DriverVelocityFS.num_past_completed_trips"
    ].to_list() == [14, 15]
    assert feature_df[
        "DriverVelocityFS.percent_past_guest_cancelled_trips"
    ].to_list() == [0.2632, 0.1667]
    assert feature_df["DriverVelocityFS.num_logins_last_day"].to_list() == [
        0,
        0,
    ]
    assert feature_df[
        "DriverVelocityFS.num_checkout_pages_last_day"
    ].to_list() == [
        0,
        0,
    ]
    assert feature_df["DriverVelocityFS.num_past_approved_trips"].to_list() == [
        19,
        17,
    ]

    # Lets do an query_offline to test out the num_logins_last_day and num_checkout_pages_last_day

    ts = pd.Series(
        [
            datetime.strptime(date_time_str, "%Y-%m-%d %H:%M:%S")
            for date_time_str in ["2022-06-22 21:00:38", "2022-03-26 22:35:03"]
        ]
    )
    feature_df = client.query_offline(
        inputs=[
            "Request.driver_id",
            "Request.reservation_id",
        ],
        input_dataframe=pd.DataFrame(
            {
                "Request.driver_id": [
                    23315855,
                    23348774,
                ],
                "Request.reservation_id": [13176875, 13398944],
                "timestamp": ts,
            }
        ),
        outputs=[DriverVelocityFS],
        timestamp_column="timestamp",
    )
    assert feature_df.shape == (2, 7)
    assert feature_df["DriverVelocityFS.num_logins_last_day"].to_list() == [
        0,
        0,
    ]
    assert feature_df[
        "DriverVelocityFS.num_checkout_pages_last_day"
    ].to_list() == [
        0,
        0,
    ]
