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
    driver_id: int = feature(Request.driver_id)

    # Velocity FS
    num_past_completed_trips: int = feature(
        DriverVelocityFS.num_past_completed_trips
    )
    percent_past_guest_cancelled_trips: float = feature(
        DriverVelocityFS.percent_past_guest_cancelled_trips
    )
    num_logins_last_day: int = feature(DriverVelocityFS.num_logins_last_day)
    num_checkout_pages_last_day: int = feature(
        DriverVelocityFS.num_checkout_pages_last_day
    )
    num_past_approved_trips: int = feature(
        DriverVelocityFS.num_past_approved_trips
    )

    # Payment features
    num_postal_codes: int = feature(PaymentFS.num_postal_codes)
    num_failed_payment_verification_attempts: int = feature(
        PaymentFS.num_failed_payment_verification_attempts,
    )
    payment_type: str = feature(PaymentFS.payment_type)
    is_debit_card: bool = feature(PaymentFS.is_debit_card)
    max_radar_score: float = feature(PaymentFS.max_radar_score)
    min_radar_score: float = feature(PaymentFS.min_radar_score)

    # Age features
    account_age: float = feature(AgeFS.account_age)
    age: float = feature(AgeFS.age)

    # Reservation features
    guest_protection_level: Optional[str] = feature(
        ReservationLevelFS.guest_protection_level
    )
    total_trip_price_amount: float = feature(
        ReservationLevelFS.total_trip_price_amount
    )
    delivery_type: oneof(str, ["AIRPORT", "HOME"]) = feature(
        ReservationLevelFS.delivery_type
    )
    trip_duration_hours: float = feature(ReservationLevelFS.trip_duration_hours)

    # Vehicle features
    vehicle_id: int = feature(Request.vehicle_id)
    vehicle_state: str = feature(VehicleFS.vehicle_state)
    market_area_id: int = feature(VehicleFS.market_area_id)
