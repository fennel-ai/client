import pandas as pd
from fraud.datasets.payment import (
    ChargesDS,
    TransactionsDS,
    PaymentEventDS,
    LastPaymentDS,
    PaymentDS,
)
from fraud.datasets.payment_ids import (
    PaymentAccountSrcDS,
    PaymentAccountAssociationSrcDS,
    AccountSrcDS,
    PaymentIdentifierDS,
)
from fraud.datasets.sourced import (
    DriverDS,
    DriverCreditScoreDS,
    RentCarCheckoutEventDS,
)
from fraud.datasets.vehicle import (
    IdToMarketAreaDS,
    LocationToNewMarketArea,
    LocationDS2,
    LocationDS,
    VehicleSummary,
    MarketAreaDS,
)
from fraud.datasets.velocity import (
    BookingFlowCheckoutPageDS,
    ReservationDS,
    NumCompletedTripsDS,
    LoginEventsDS,
    LoginsLastDayDS,
    CheckoutPagesLastDayDS,
    PastApprovedDS,
    ReservationSummaryDS,
    CancelledTripsDS,
)
from fraud.featuresets.all_features import FraudModel
from fraud.featuresets.driver import (
    Request,
    AgeFS,
    CreditScoreFS,
    ReservationLevelFS,
)
from fraud.featuresets.payment import PaymentFS
from fraud.featuresets.vehicle import VehicleFS
from fraud.featuresets.velocity import DriverVelocityFS
from fraud.tests.test_driver_payment_features import (
    log_payment_identifier_datasets,
)

from fennel.testing import mock


@mock
def test_all_features(client):
    client.commit(
        message="some commit message",
        datasets=[
            DriverDS,
            DriverCreditScoreDS,
            AccountSrcDS,
            PaymentIdentifierDS,
            ChargesDS,
            PaymentAccountSrcDS,
            PaymentAccountAssociationSrcDS,
            PaymentDS,
            TransactionsDS,
            LastPaymentDS,
            PaymentEventDS,
            IdToMarketAreaDS,
            LocationToNewMarketArea,
            RentCarCheckoutEventDS,
            LocationDS2,
            LocationDS,
            VehicleSummary,
            BookingFlowCheckoutPageDS,
            ReservationDS,
            NumCompletedTripsDS,
            LoginEventsDS,
            LoginsLastDayDS,
            CheckoutPagesLastDayDS,
            PastApprovedDS,
            ReservationSummaryDS,
            CancelledTripsDS,
            MarketAreaDS,
        ],
        featuresets=[
            Request,
            AgeFS,
            CreditScoreFS,
            FraudModel,
            PaymentFS,
            DriverVelocityFS,
            ReservationLevelFS,
            VehicleFS,
        ],
        env="local",
    )
    log_payment_identifier_datasets(client)

    source_df = (
        pd.read_csv("examples/fraud/data/driver/driver.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(
            birthdate=lambda x: pd.to_datetime(x["birthdate"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="DriverDS",
        df=source_df,
    )
    assert log_response.status_code == 200, log_response.json()

    socure_df = (
        pd.read_csv("examples/fraud/data/driver/driver_credit_score.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(score=lambda x: x["score"].astype(float))
    )

    log_response = client.log(
        webhook="app_webhook",
        endpoint="DriverCreditScoreDS",
        df=socure_df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/payment/payment_event.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(invoice_id=lambda x: x["invoice_id"].astype("Int64"))
        .assign(debit_card=lambda x: x["debit_card"].astype("Int64"))
        .assign(postal_code="123")
    )
    log_response = client.log(
        webhook="app_webhook", endpoint="PaymentEventDS", df=df
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/payment/charges.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(risk_score=lambda x: x["risk_score"].astype(float))
        .assign(amount=lambda x: x["amount"].astype(float))
        .assign(amount_refunded=lambda x: x["amount_refunded"].astype(float))
        .assign(reservation_id=lambda x: x["reservation_id"].astype("Int64"))
    )

    log_response = client.log(
        webhook="app_webhook",
        endpoint="ChargesDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

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

    df = (
        pd.read_csv("examples/fraud/data/payment/location.csv")
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
        pd.read_csv(
            "examples/fraud/data/payment/location_to_new_market_area.csv"
        )
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

    feature_df = client.query(
        inputs=[
            "Request.driver_id",
            "Request.vehicle_id",
            "Request.reservation_id",
        ],
        input_dataframe=pd.DataFrame(
            {
                "Request.driver_id": [
                    23315855,
                    23348774,
                ],
                "Request.vehicle_id": [1145620, 900208],
                "Request.reservation_id": [13176875, 13398944],
            }
        ),
        outputs=[FraudModel],
    )
    assert feature_df.shape == (2, 21)
    # Test features from each group
    assert feature_df["FraudModel.num_past_completed_trips"].to_list() == [
        14,
        15,
    ]
    assert feature_df["FraudModel.num_past_approved_trips"].to_list() == [
        19,
        17,
    ]
    assert feature_df["FraudModel.max_radar_score"].to_list() == [0.0, 5.0]
    assert feature_df["FraudModel.trip_duration_hours"].to_list() == [
        0.0,
        0.0,
    ]
    assert feature_df["FraudModel.market_area_id"].to_list() == [0, 0]
