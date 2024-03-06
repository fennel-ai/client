from datetime import datetime
from typing import Optional

import pandas as pd
import pytest
from dateutil.relativedelta import relativedelta  # type: ignore

from fennel import meta, Count, featureset, feature, extractor
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib import inputs, outputs
from fennel.sources import Webhook
from fennel.sources import source
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")

__owner__ = "uber-data@eng.com"


@dataset
@source(
    webhook.endpoint("RiderDataset"), cdc="append", disorder="14d", tier="local"
)
class RiderDataset:
    rider_id: int = field(key=True)
    created: datetime = field(timestamp=True)
    birthdate: datetime


@dataset
@source(
    webhook.endpoint("RiderCreditScoreDataset"),
    disorder="14d",
    cdc="append",
    tier="local",
)
class RiderCreditScoreDataset:
    rider_id: int = field(key=True)
    created: datetime
    score: float


@dataset
@source(
    webhook.endpoint("CountryLicenseDataset"),
    disorder="14d",
    cdc="append",
    tier="local",
)
@meta(owner="data@eng.com")
class CountryLicenseDataset:
    rider_id: int = field(key=True)
    created: datetime
    country_code: str


@dataset
@source(
    webhook.endpoint("ReservationsDataset"),
    disorder="14d",
    cdc="append",
    tier="local",
)
class ReservationsDataset:
    rider_id: int
    vehicle_id: int
    is_completed_trip: int
    created: datetime


@dataset
@source(
    webhook.endpoint("NumCompletedTripsDataset"),
    disorder="14d",
    cdc="append",
    tier="local",
)
class NumCompletedTripsDataset:
    rider_id: int = field(key=True)
    count_num_completed_trips: int
    created: datetime

    @pipeline
    @inputs(ReservationsDataset)
    def my_pipeline(cls, reservations: Dataset):
        completed = reservations.filter(lambda df: df["is_completed_trip"] == 1)
        return completed.groupby("rider_id").aggregate(
            Count(
                of="vehicle_id",
                unique=True,
                approx=True,
                window="forever",
                into_field="count_num_completed_trips",
            ),
        )


@featureset
class RequestFeatures0:
    ts: datetime = feature(id=1)
    rider_id: int = feature(id=2)


@featureset
class RequestFeatures1:
    ts: datetime = feature(id=1)
    id1: int = feature(id=2).extract(feature=RequestFeatures0.rider_id)  # type: ignore


@featureset
class RequestFeatures2:
    ts: datetime = feature(id=1)
    id2: int = feature(id=2).extract(feature=RequestFeatures1.id1)  # type: ignore
    const: int = feature(id=3)
    num_trips: int = feature(id=4).extract(  # type: ignore
        field=NumCompletedTripsDataset.count_num_completed_trips,
        provider=RequestFeatures0,
        default=0,
    )

    @extractor()
    @inputs(id2)
    @outputs(const)
    def extract_const(cls, ts: pd.Series, id2: pd.Series) -> pd.DataFrame:
        return pd.DataFrame({"const": [1] * len(ts)})


@featureset
class RequestFeatures3:
    ts: datetime = feature(id=1)
    rider_id: int = feature(id=2).extract(feature=RequestFeatures2.id2)  # type: ignore
    vehicle_id: int = feature(id=3)
    reservation_id: Optional[int] = feature(id=4)


@featureset
class RiderFeatures:
    id: int = feature(id=1).extract(feature=RequestFeatures2.id2)  # type: ignore
    created: datetime = feature(id=2).extract(  # type: ignore
        field=RiderDataset.created,
        provider=RequestFeatures3,
        default=datetime(2000, 1, 1, 0, 0, 0),
    )
    birthdate: datetime = feature(id=3).extract(  # type: ignore
        field=RiderDataset.birthdate,
        provider=RequestFeatures3,
        default=datetime(2000, 1, 1, 0, 0, 0),
    )
    age_years: int = feature(id=4)
    ais_score: float = feature(id=5).extract(  # type: ignore
        field=RiderCreditScoreDataset.score,
        provider=RequestFeatures3,
        default=-1.0,
    )
    dl_state: str = feature(id=6).extract(  # type: ignore
        field=CountryLicenseDataset.country_code,
        provider=RequestFeatures3,
        default="Unknown",
    )
    is_us_dl: bool = feature(id=7)
    num_past_completed_trips: int = feature(id=8).extract(  # type: ignore
        field=NumCompletedTripsDataset.count_num_completed_trips,
        provider=RequestFeatures3,
        default=0,
    )
    dl_state_population: int = feature(id=9)

    @extractor
    @inputs(dl_state)
    @outputs(is_us_dl)
    def extract_is_us_dl(
        cls, ts: pd.Series, dl_state: pd.Series
    ) -> pd.DataFrame:
        is_us_dl = dl_state == "US"
        return pd.DataFrame({"is_us_dl": is_us_dl})

    @extractor
    @inputs(birthdate)
    @outputs(age_years)
    def extract_age_years(
        cls, ts: pd.Series, birthdate: pd.Series
    ) -> pd.DataFrame:
        age_years = (datetime.now() - birthdate).dt.total_seconds() / (
            60 * 60 * 24 * 365
        )
        age_years = age_years.astype(int)
        return pd.DataFrame({"age_years": age_years})

    @extractor
    @inputs(dl_state)
    @outputs(dl_state_population)
    def extract_dl_state_population(
        cls, ts: pd.Series, dl_state: pd.Series
    ) -> pd.DataFrame:
        dl_state_population = dl_state.map(
            {
                "US": 328200000,
                "CA": 37600000,
                "GB": 66650000,
                "AU": 25400000,
                "DE": 83020000,
                "Unknown": 9999999,
            }
        )
        return pd.DataFrame({"dl_state_population": dl_state_population})


@mock
def test_complex_auto_gen_extractors(client):
    """
    This test tries to test the complex interactions between auto-generated and user-defined extractors.
    It mainly ensures that the sync call is able to generate the correct code for the featuresets, the defaults
    are correctly handled, and the extractors are correctly called.
    We also test user-defined extractors that depend on auto-generated extractors.

    :param client:
    :return:
    """
    with pytest.raises(ValueError) as e:
        _ = client.commit(
            message="some commit msg",
            datasets=[
                RiderDataset,
                RiderCreditScoreDataset,
                CountryLicenseDataset,
                ReservationsDataset,
                NumCompletedTripsDataset,
            ],
            featuresets=[
                RiderFeatures,
                RequestFeatures1,
                RequestFeatures2,
                RequestFeatures3,
            ],
        )
    error_msg1 = "Featureset `RequestFeatures0` is required by `RequestFeatures1` but is not present in the sync call. Please ensure that all featuresets are present in the sync call."
    error_msg2 = error_msg1.replace("RequestFeatures1", "RequestFeatures2")
    assert str(e.value) == error_msg1 or str(e.value) == error_msg2

    with pytest.raises(ValueError) as e:
        _ = client.commit(
            message="some commit msg",
            datasets=[
                RiderDataset,
                RiderCreditScoreDataset,
                CountryLicenseDataset,
                ReservationsDataset,
                NumCompletedTripsDataset,
            ],
            featuresets=[
                RiderFeatures,
                RequestFeatures0,
                RequestFeatures1,
                RequestFeatures3,
            ],
        )
    error_msg1 = "Featureset `RequestFeatures2` is required by `RiderFeatures` but is not present in the sync call. Please ensure that all featuresets are present in the sync call."
    error_msg2 = error_msg1.replace("RiderFeatures", "RequestFeatures3")
    assert str(e.value) == error_msg1 or str(e.value) == error_msg2

    with pytest.raises(ValueError) as e:
        _ = client.commit(
            message="some commit msg",
            datasets=[
                RiderDataset,
                RiderCreditScoreDataset,
                CountryLicenseDataset,
                ReservationsDataset,
            ],
            featuresets=[
                RiderFeatures,
                RequestFeatures0,
                RequestFeatures1,
                RequestFeatures2,
                RequestFeatures3,
            ],
        )
    assert (
        str(e.value)
        == "Dataset `NumCompletedTripsDataset` not found in sync call"
    )

    resp = client.commit(
        message="some commit msg",
        datasets=[
            RiderDataset,
            RiderCreditScoreDataset,
            CountryLicenseDataset,
            ReservationsDataset,
            NumCompletedTripsDataset,
        ],
        featuresets=[
            RiderFeatures,
            RequestFeatures0,
            RequestFeatures1,
            RequestFeatures2,
            RequestFeatures3,
        ],
    )

    assert resp.status_code == 200

    rider_df = pd.DataFrame(
        {
            "rider_id": [1],
            "created": [datetime.utcnow()],
            "birthdate": [datetime.utcnow() - relativedelta(years=30)],
            "country_code": ["US"],
        }
    )

    log_response = client.log(
        webhook="fennel_webhook", endpoint="RiderDataset", df=rider_df
    )
    assert log_response.status_code == 200

    reservation_df = pd.DataFrame(
        {
            "rider_id": [1],
            "vehicle_id": [1],
            "is_completed_trip": [1],
            "created": [datetime.utcnow()],
        }
    )
    log_response = client.log(
        webhook="fennel_webhook",
        endpoint="ReservationsDataset",
        df=reservation_df,
    )
    assert log_response.status_code == 200

    country_license_df = pd.DataFrame(
        {
            "rider_id": [1],
            "created": [datetime.utcnow()],
            "country_code": ["US"],
        }
    )
    log_response = client.log(
        webhook="fennel_webhook",
        endpoint="CountryLicenseDataset",
        df=country_license_df,
    )
    assert log_response.status_code == 200

    extracted_df = client.query(
        inputs=[RequestFeatures0.rider_id],
        outputs=[RiderFeatures],
        input_dataframe=pd.DataFrame({"RequestFeatures0.rider_id": [1, 2]}),
    )
    assert extracted_df.shape[0] == 2
    assert (
        extracted_df["RiderFeatures.created"].iloc[0]
        == rider_df["created"].iloc[0]
    )
    assert extracted_df["RiderFeatures.dl_state"].to_list() == ["US", "Unknown"]
    assert extracted_df["RiderFeatures.is_us_dl"].to_list() == [True, False]

    age_years = datetime.now().year - 2000
    assert extracted_df["RiderFeatures.age_years"].to_list() == [30, age_years]
    assert extracted_df["RiderFeatures.dl_state_population"].to_list() == [
        328200000,
        9999999,
    ]
