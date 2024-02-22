from typing import Optional

from fennel.featuresets import featureset, feature
from fennel.dtypes import oneof

__owner__ = "eng@app.com"

from fraud.featuresets.driver import (
    AgeFS,
    ReservationLevelFS,
)
from fraud.featuresets.payment import PaymentFS
from fraud.featuresets.vehicle import VehicleFS
from fraud.featuresets.velocity import DriverVelocityFS
from fraud.featuresets.request import Request


@featureset
class FraudModel:
    driver_id: int = feature(id=1).extract(feature=Request.driver_id)

    # Velocity FS
    num_past_completed_trips: int = feature(id=2).extract(
        feature=DriverVelocityFS.num_past_completed_trips
    )
    percent_past_guest_cancelled_trips: float = feature(id=3).extract(
        feature=DriverVelocityFS.percent_past_guest_cancelled_trips
    )
    num_logins_last_day: int = feature(id=4).extract(
        feature=DriverVelocityFS.num_logins_last_day
    )
    num_checkout_pages_last_day: int = feature(id=5).extract(
        feature=DriverVelocityFS.num_checkout_pages_last_day
    )
    num_past_approved_trips: int = feature(id=6).extract(
        feature=DriverVelocityFS.num_past_approved_trips
    )

    # Payment features
    num_postal_codes: int = feature(id=7).extract(
        feature=PaymentFS.num_postal_codes
    )
    num_failed_payment_verification_attempts: int = feature(id=8).extract(
        feature=PaymentFS.num_failed_payment_verification_attempts,
    )
    payment_type: str = feature(id=9).extract(feature=PaymentFS.payment_type)
    is_debit_card: bool = feature(id=10).extract(
        feature=PaymentFS.is_debit_card
    )
    max_radar_score: float = feature(id=11).extract(
        feature=PaymentFS.max_radar_score
    )
    min_radar_score: float = feature(id=12).extract(
        feature=PaymentFS.min_radar_score
    )

    # Age features
    account_age: float = feature(id=13).extract(feature=AgeFS.account_age)
    age: float = feature(id=14).extract(feature=AgeFS.age)

    # Reservation features
    guest_protection_level: Optional[str] = feature(id=15).extract(
        feature=ReservationLevelFS.guest_protection_level
    )
    total_trip_price_amount: float = feature(id=16).extract(
        feature=ReservationLevelFS.total_trip_price_amount
    )
    delivery_type: oneof(str, ["AIRPORT", "HOME"]) = feature(id=17).extract(
        feature=ReservationLevelFS.delivery_type
    )
    trip_duration_hours: float = feature(id=18).extract(
        feature=ReservationLevelFS.trip_duration_hours
    )

    # Vehicle features
    vehicle_id: int = feature(id=19).extract(feature=Request.vehicle_id)
    vehicle_state: str = feature(id=20).extract(feature=VehicleFS.vehicle_state)
    market_area_id: int = feature(id=21).extract(
        feature=VehicleFS.market_area_id
    )
