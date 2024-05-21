from datetime import datetime, timezone

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import connectors
from fennel.connectors import source
from fennel.datasets import (
    dataset,
    Dataset,
    pipeline,
    field,
)
from fennel.dtypes import Window, struct, Session
from fennel.lib import meta, inputs
from fennel.testing import mock

webhook = connectors.Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("AppEvent"), disorder="14d", cdc="append")
@dataset
class AppEvent:
    user_id: int
    star: int
    timestamp: datetime = field(timestamp=True)


@struct
class WindowStats:
    avg_star: float
    count: int


@meta(owner="test@test.com")
@dataset(index=True)
class Sessions:
    user_id: int = field(key=True)
    window: Window = field(key=True)
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(AppEvent)
    def get_sessions(cls, app_event: Dataset):
        return app_event.groupby("user_id", window=Session("3s")).aggregate()


def log_app_events_data(client):
    data = {
        "user_id": [1] * 20,
        "timestamp": [
            datetime(2023, 1, 16, 11, 0, 1),
            datetime(2023, 1, 16, 11, 0, 2),
            datetime(2023, 1, 16, 11, 0, 3),
            datetime(2023, 1, 16, 11, 0, 7),
            datetime(2023, 1, 16, 11, 0, 8),
            datetime(2023, 1, 16, 11, 0, 9),
            datetime(2023, 1, 16, 11, 0, 10),
            datetime(2023, 1, 16, 11, 0, 11),
            datetime(2023, 1, 16, 11, 0, 15),
            datetime(2023, 1, 16, 11, 0, 20),
            datetime(2023, 1, 16, 11, 0, 25),
            datetime(2023, 1, 16, 11, 0, 26),
            datetime(2023, 1, 16, 11, 0, 27),
            datetime(2023, 1, 16, 11, 0, 28),
            datetime(2023, 1, 16, 11, 0, 29),
            datetime(2023, 1, 16, 11, 0, 30),
            datetime(2023, 1, 16, 11, 0, 32),
            datetime(2023, 1, 16, 11, 0, 33),
            datetime(2023, 1, 16, 11, 0, 37),
            datetime(2023, 1, 16, 11, 0, 38),
        ],
        "star": [1, 2, 3, 2, 4, 5, 1, 4, 2, 3, 1, 5, 3, 1, 5, 2, 3, 5, 2, 4],
    }
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "AppEvent", df)
    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_session_window_operator(client):
    # Sync to mock client
    client.commit(
        message="Initial commit",
        datasets=[AppEvent, Sessions],
        featuresets=[],
    )

    # Log data to test the pipeline
    log_app_events_data(client)

    client.sleep()

    user_id_keys = pd.Series([1])
    window_keys = pd.Series(
        [
            {
                "begin": datetime(2023, 1, 16, 11, 0, 25, tzinfo=timezone.utc),
                "end": datetime(
                    2023,
                    1,
                    16,
                    11,
                    0,
                    33,
                    microsecond=1,
                    tzinfo=timezone.utc,
                ),
            }
        ]
    )
    df_session, _ = client.lookup(
        "Sessions",
        keys=pd.DataFrame(
            {
                "user_id": user_id_keys,
                "window": window_keys,
            }
        ),
    )
    assert df_session.shape[0] == 1
    assert list(df_session["user_id"].values) == [1]
    if client.is_integration_client():
        assert df_session["window"].values[0].begin == datetime(
            2023, 1, 16, 11, 0, 25
        )
    else:
        assert df_session["window"].values[0].begin == datetime(
            2023, 1, 16, 11, 0, 25, tzinfo=timezone.utc
        )
    if client.is_integration_client():
        assert df_session["window"].values[0].end == datetime(
            2023, 1, 16, 11, 0, 33, microsecond=1
        )
    else:
        assert df_session["window"].values[0].end == datetime(
            2023, 1, 16, 11, 0, 33, microsecond=1, tzinfo=timezone.utc
        )
