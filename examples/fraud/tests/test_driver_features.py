import pandas as pd
from fraud.datasets.sourced import (
    DriverDS,
    DriverCreditScoreDS,
)
from fraud.featuresets.driver import Request, AgeFS, CreditScoreFS

from fennel.testing import mock


class TestAgeFS:
    @mock
    def test_account_age_features(self, client):
        sync_response = client.commit(
            message="Syncing driver features",
            datasets=[
                DriverDS,
            ],
            featuresets=[Request, AgeFS],
            env="local",
        )

        assert sync_response.status_code == 200

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

        feature_df = client.query(
            outputs=[AgeFS],
            inputs=[Request.driver_id],
            input_dataframe=pd.DataFrame(
                {
                    "Request.driver_id": [
                        23348774,
                        6982917,
                        1234,
                        6982917,
                        23120871,
                    ]
                }
            ),
        )
        assert feature_df.shape == (5, 2)


class TestCreditScore:
    @mock
    def test_credit_score_features(self, client):
        sync_response = client.commit(
            message="Syncing driver credit score features",
            datasets=[
                DriverCreditScoreDS,
            ],
            featuresets=[Request, CreditScoreFS],
            env="local",
        )

        assert sync_response.status_code == 200

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

        feature_df = client.query(
            outputs=[CreditScoreFS],
            inputs=[Request.driver_id],
            input_dataframe=pd.DataFrame(
                {
                    "Request.driver_id": [
                        23348774,
                        6982917,
                        1234,
                        6982917,
                        23120871,
                    ]
                }
            ),
        )
        assert feature_df.shape == (5, 2)
