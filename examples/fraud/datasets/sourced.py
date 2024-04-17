from datetime import datetime
from typing import Optional

from fennel.connectors import Webhook, S3, MySQL, source
from fennel.datasets import dataset, field
from fennel.dtypes import oneof

__owner__ = "eng@app.com"

webhook = Webhook(name="app_webhook")

# Get an already defined MySql source.
mysql = MySQL.get(name="mysql_fraud")

s3 = S3(
    name="s3_fraud",
    aws_access_key_id="aws_access_key_id",
    aws_secret_access_key="aws_secret_access_key",
)


@source(
    webhook.endpoint("EventTrackerDS"),
    disorder="14d",
    cdc="append",
    tier="local",
)
@source(
    s3.bucket(
        bucket_name="prod",
        prefix="event_tracker/payment_fraud.parquet",
    ),
    every="1d",
    disorder="14d",
    cdc="append",
    tier="prod",
)
@dataset
class EventTrackerDS:
    """
    Event tracker is a list of driver reservation sessions.
    It has no key since, it is a stream of events.
    """

    driver_id: int
    reservation_id: Optional[int]
    session_id: str
    is_us_dl: str
    created: datetime


@source(
    webhook.endpoint("DriverLicenseCountryDS"),
    disorder="14d",
    cdc="append",
    tier="local",
)
@source(
    s3.bucket(
        bucket_name="prod",
        prefix="rides/driver_license_country.parquet",
    ),
    every="1d",
    disorder="14d",
    cdc="upsert",
    tier="prod",
)
@dataset
class DriverLicenseCountryDS:
    driver_id: int = field(key=True)
    country_code: str
    created: datetime


@source(
    webhook.endpoint("VehicleSummaryDS"),
    disorder="14d",
    cdc="upsert",
    tier="local",
)
@source(
    s3.bucket(
        bucket_name="prode",
        prefix="rides/vehicle_summary.parquet",
    ),
    every="2h",
    disorder="14d",
    cdc="upsert",
    tier="prod",
)
@dataset
class VehicleSummaryDS:
    vehicle_id: int = field(key=True)
    state: str
    longitude: float
    latitude: float
    market_area_id: int
    country: str
    created: datetime


@source(
    webhook.endpoint("RentCarCheckoutEventDS"),
    disorder="14d",
    cdc="upsert",
    tier="local",
)
@source(
    s3.bucket(
        bucket_name="prod",
        prefix="event_tracker/RentCarCheckoutEventDS.parquet",
    ),
    every="2h",
    disorder="14d",
    cdc="upsert",
    tier="prod",
)
@dataset(index=True)
class RentCarCheckoutEventDS:
    driver_id: int = field(key=True)
    delivery_type: oneof(str, ["AIRPORT", "HOME"])  # type: ignore
    protection_level: Optional[str]
    local_start_ts: datetime
    local_end_ts: datetime
    total_trip_price_amount: float
    vehicle_id: int
    session_id: Optional[str]
    created: datetime = field(timestamp=True)


@source(
    webhook.endpoint("DriverDS"), disorder="14d", cdc="upsert", tier="local"
)
@dataset(index=True)
class DriverDS:
    id: int = field(key=True)
    created: datetime = field(timestamp=True)
    birthdate: datetime


@source(
    webhook.endpoint("DriverCreditScoreDS"),
    disorder="14d",
    cdc="upsert",
    tier="local",
)
@dataset(index=True)
class DriverCreditScoreDS:
    driver_id: int = field(key=True)
    score: Optional[float]
    created: datetime = field(timestamp=True)
