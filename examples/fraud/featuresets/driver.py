from typing import Optional

import pandas as pd
from fraud.datasets.sourced import (
    DriverDS,
    DriverCreditScoreDS,
    RentCarCheckoutEventDS,
)

from fraud.featuresets.request import Request
from fennel.featuresets import featureset, feature, extractor
from fennel.lib import inputs, outputs
from fennel.dtype import oneof

__owner__ = "eng@app.com"


@featureset
class AgeFS:
    account_age: float = feature(id=1)
    age: float = feature(id=2)

    @extractor(depends_on=[DriverDS])
    @inputs(Request.driver_id)
    @outputs(account_age, age)
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
    driver_id: int = feature(id=1).extract(feature=Request.driver_id)
    guest_protection_level: Optional[str] = feature(id=2).extract(
        field=RentCarCheckoutEventDS.protection_level,
    )
    total_trip_price_amount: float = feature(id=3).extract(
        field=RentCarCheckoutEventDS.total_trip_price_amount,
        default=0.0,
    )
    delivery_type: oneof(str, ["AIRPORT", "HOME"]) = feature(id=4).extract(
        field=RentCarCheckoutEventDS.delivery_type,
        default="AIRPORT",
    )
    trip_duration_hours: float = feature(id=5)

    @extractor(depends_on=[RentCarCheckoutEventDS])
    @inputs(Request.driver_id)
    @outputs(trip_duration_hours)
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
    # or you could define a feature named driver_id and extract it from Request.driver_id
    ais_score: float = feature(id=1).extract(
        field=DriverCreditScoreDS.score,
        default=0.0,
        provider=Request,
    )
