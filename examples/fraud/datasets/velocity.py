"""
This file contains all velocity related datasets.
"""

from datetime import datetime
from typing import Optional

from fennel.datasets import dataset, field, pipeline, Dataset, Count, index
from fennel.lib import inputs
from fennel.sources import Webhook, source

__owner__ = "eng@app.com"

webhook = Webhook(name="app_webhook")


@source(
    webhook.endpoint("ReservationDS"),
    tier="local",
    disorder="14d",
    cdc="append",
)
@dataset
class ReservationDS:
    driver_id: int
    vehicle_id: int
    reservation_id: int
    is_completed_trip: int
    payment_method: str
    current_status: int
    created: datetime


@index
@dataset
class NumCompletedTripsDS:
    driver_id: int = field(key=True)
    num_past_completed_trips: int
    created: datetime

    @pipeline
    @inputs(ReservationDS)
    def num_completed_trips(cls, reservations: Dataset):
        return (
            reservations.filter(lambda df: df["is_completed_trip"] == 1)
            .groupby("driver_id")
            .aggregate(
                Count(
                    of="reservation_id",
                    unique=True,
                    approx=True,
                    window="forever",
                    into_field="num_past_completed_trips",
                ),
            )
        )


@index
@dataset
class CancelledTripsDS:
    driver_id: int = field(key=True)
    num_past_cancelled_trips: int
    created: datetime

    @pipeline
    @inputs(ReservationDS)
    def num_completed_trips(cls, reservations: Dataset):
        return (
            reservations.filter(lambda df: df["current_status"].isin([2, 7]))
            .groupby("driver_id")
            .aggregate(
                Count(
                    of="reservation_id",
                    unique=True,
                    approx=True,
                    window="forever",
                    into_field="num_past_cancelled_trips",
                ),
            )
        )


@source(
    webhook.endpoint("LoginEventsDS"),
    disorder="14d",
    cdc="append",
    tier="local",
)
@dataset
class LoginEventsDS:
    driver_id: int
    id: str
    session_id: Optional[str]
    created: datetime


@index
@dataset
class LoginsLastDayDS:
    driver_id: int = field(key=True)
    num_logins_last_day: int
    created: datetime

    @pipeline
    @inputs(LoginEventsDS)
    def logins_per_day(cls, logins: Dataset):
        return logins.groupby("driver_id").aggregate(
            Count(
                of="id",
                unique=True,
                approx=True,
                window="1d",
                into_field="num_logins_last_day",
            ),
        )


@dataset
@source(
    webhook.endpoint("BookingFlowCheckoutPageDS"),
    disorder="14d",
    cdc="append",
    tier="local",
)
class BookingFlowCheckoutPageDS:
    id: str
    created: datetime
    driver_id: Optional[int]
    device_id: str


@index
@dataset
class CheckoutPagesLastDayDS:
    driver_id: int = field(key=True)
    num_checkout_pages_last_day: int
    created: datetime

    @pipeline
    @inputs(BookingFlowCheckoutPageDS)
    def checkout_pages_per_day(cls, logins: Dataset):
        return (
            logins.dropnull("driver_id")
            .groupby("driver_id")
            .aggregate(
                Count(
                    of="id",
                    unique=True,
                    approx=True,
                    window="1d",
                    into_field="num_checkout_pages_last_day",
                ),
            )
        )


@source(
    webhook.endpoint("ReservationSummaryDS"),
    disorder="14d",
    cdc="append",
    tier="local",
)
@dataset
class ReservationSummaryDS:
    reservation_id: int = field(key=True)
    driver_id: int
    vehicle_id: int
    is_ever_approved: int
    trip_end_ts: Optional[datetime]
    created: datetime = field(timestamp=True)


@index
@dataset
class PastApprovedDS:
    driver_id: int = field(key=True)
    num_past_approved_trips: int
    created: datetime

    @pipeline
    @inputs(ReservationSummaryDS)
    def past_approved_trips(cls, reservations: Dataset):
        return (
            reservations.filter(lambda df: df["is_ever_approved"] == 1)
            .groupby("driver_id")
            .aggregate(
                Count(
                    of="reservation_id",
                    unique=True,
                    approx=True,
                    window="forever",
                    into_field="num_past_approved_trips",
                ),
            )
        )
