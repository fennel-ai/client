from typing import Optional

import pandas as pd
from fraud.datasets.sourced import (
    DriverDS,
    DriverCreditScoreDS,
    RentCarCheckoutEventDS,
)

from fraud.featuresets.request import Request
from fennel.featuresets import featureset, feature as F, extractor
from fennel.lib import inputs, outputs
from fennel.dtypes import oneof

__owner__ = "eng@app.com"


@featureset
class AgeFS:
    account_age: float
    age: float

    @extractor(depends_on=[DriverDS])
    @inputs(Request.driver_id)
    @outputs("account_age", "age")
    def extract(cls, ts: pd.Series, driver_id: pd.Series):
        df, _ = DriverDS.lookup(
            ts, id=driver_id, fields=["created", "birthdate"]
        )
        # false rows in found could be null
        df.fillna(ts[0], inplace=True)
        df["account_age"] = (ts - df["created"]).dt.total_seconds() / 60
        df["age"] = (ts - df["birthdate"]).dt.total_seconds() / 60
        return df[["account_age", "age"]]


@featureset
class ReservationLevelFS:
    driver_id: int = F(Request.driver_id)
    guest_protection_level: Optional[str] = F(
        RentCarCheckoutEventDS.protection_level,
    )
    total_trip_price_amount: float = F(
        RentCarCheckoutEventDS.total_trip_price_amount,
        default=0.0,
    )
    delivery_type: oneof(str, ["AIRPORT", "HOME"]) = F(
        RentCarCheckoutEventDS.delivery_type,
        default="AIRPORT",
    )
    trip_duration_hours: float

    @extractor(depends_on=[RentCarCheckoutEventDS])
    @inputs(Request.driver_id)
    @outputs("trip_duration_hours")
    def extract(cls, ts: pd.Series, driver_id: pd.Series):
        df, found = RentCarCheckoutEventDS.lookup(
            ts, driver_id=driver_id, fields=["local_start_ts", "local_end_ts"]
        )
        df.fillna(ts[0], inplace=True)
        df["trip_duration_hours"] = (
            (df["local_end_ts"] - df["local_start_ts"]).dt.total_seconds()
            / 60
            / 60
        )
        return df[["trip_duration_hours"]]


@featureset
class CreditScoreFS:
    driver_id: int = F(Request.driver_id)
    ais_score: float = F(
        DriverCreditScoreDS.score,
        default=0.0,
    )
