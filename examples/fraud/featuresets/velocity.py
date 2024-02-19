import pandas as pd
from fraud.datasets.velocity import (
    NumCompletedTripsDS,
    CancelledTripsDS,
    CheckoutPagesLastDayDS,
    PastApprovedDS,
    LoginsLastDayDS,
)
from fraud.featuresets.request import Request

from fennel import extractor
from fennel.featuresets import featureset, feature

__owner__ = "eng@app.com"

from fennel.lib import inputs, outputs


@featureset
class DriverVelocityFS:
    driver_id: int = feature(id=1).extract(feature=Request.driver_id)
    percent_past_guest_cancelled_trips: float = feature(id=2)
    num_past_completed_trips: int = feature(id=3).extract(
        field=NumCompletedTripsDS.num_past_completed_trips, default=0
    )
    num_logins_last_day: int = feature(id=4).extract(
        field=LoginsLastDayDS.num_logins_last_day, default=0
    )
    num_checkout_pages_last_day: int = feature(id=5).extract(
        field=CheckoutPagesLastDayDS.num_checkout_pages_last_day, default=0
    )
    num_past_approved_trips: int = feature(id=6).extract(
        field=PastApprovedDS.num_past_approved_trips, default=0
    )

    @extractor(depends_on=[NumCompletedTripsDS, CancelledTripsDS], version=1)
    @inputs(driver_id)
    @outputs(percent_past_guest_cancelled_trips)
    def calculate_percent_past_guest_cancelled_trips(
        cls, ts: pd.Series, driver_ids: pd.Series
    ):
        completed_trips, _ = NumCompletedTripsDS.lookup(
            ts, driver_id=driver_ids, fields=["num_past_completed_trips"]
        )
        cancelled_trips, _ = CancelledTripsDS.lookup(
            ts, driver_id=driver_ids, fields=["num_past_cancelled_trips"]
        )
        completed_trips = completed_trips.fillna(0)
        cancelled_trips = cancelled_trips.fillna(0)

        df = pd.DataFrame(
            {
                "completed_trips": completed_trips["num_past_completed_trips"],
                "cancelled_trips": cancelled_trips["num_past_cancelled_trips"],
            }
        )
        df["percent_past_guest_cancelled_trips"] = df[
            ["completed_trips", "cancelled_trips"]
        ].apply(
            lambda row: (
                round(row[1] / (row[0] + row[1]), 4)
                if row[0] + row[1] > 0
                else 0
            ),
            axis=1,
        )
        return df[["percent_past_guest_cancelled_trips"]]
