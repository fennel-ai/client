from typing import Optional

from fennel.featuresets import featureset, feature as F
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
    driver_id: int = F(Request.driver_id)

    # Velocity FS
    num_past_completed_trips: int = F(DriverVelocityFS.num_past_completed_trips)
    percent_past_guest_cancelled_trips: float = F(
        DriverVelocityFS.percent_past_guest_cancelled_trips
    )
    num_logins_last_day: int = F(DriverVelocityFS.num_logins_last_day)
    num_checkout_pages_last_day: int = F(
        DriverVelocityFS.num_checkout_pages_last_day
    )
    num_past_approved_trips: int = F(DriverVelocityFS.num_past_approved_trips)

    # Payment features
    num_postal_codes: int = F(PaymentFS.num_postal_codes)
    num_failed_payment_verification_attempts: int = F(
        PaymentFS.num_failed_payment_verification_attempts,
    )
    payment_type: str = F(PaymentFS.payment_type)
    is_debit_card: bool = F(PaymentFS.is_debit_card)
    max_radar_score: float = F(PaymentFS.max_radar_score)
    min_radar_score: float = F(PaymentFS.min_radar_score)

    # Age features
    account_age: float = F(AgeFS.account_age)
    age: float = F(AgeFS.age)

    # Reservation features
    guest_protection_level: Optional[str] = F(
        ReservationLevelFS.guest_protection_level
    )
    total_trip_price_amount: float = F(
        ReservationLevelFS.total_trip_price_amount
    )
    delivery_type: oneof(str, ["AIRPORT", "HOME"]) = F(
        ReservationLevelFS.delivery_type
    )
    trip_duration_hours: float = F(ReservationLevelFS.trip_duration_hours)

    # Vehicle features
    vehicle_id: int = F(Request.vehicle_id)
    vehicle_state: str = F(VehicleFS.vehicle_state)
    market_area_id: int = F(VehicleFS.market_area_id)
