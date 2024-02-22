from datetime import datetime
from typing import List

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import sources
from fennel.datasets import dataset, Dataset, pipeline, field, Average, LastK
from fennel.featuresets import featureset, feature, extractor
from fennel.lib import meta, inputs, outputs
from fennel.dtypes import Window
from fennel.sources import source
from fennel.testing import mock

webhook = sources.Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("AppEvent"))
@dataset
class AppEvent:
    user_id: int
    timestamp: datetime = field(timestamp=True)


@meta(owner="test@test.com")
@dataset
class Sessions:
    user_id: int = field(key=True)
    window: Window = field(key=True)
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(AppEvent)
    def get_sessions(cls, app_event: Dataset):
        return app_event.groupby("user_id").window(
            type="session", gap="3s", field="window"
        )


@meta(owner="test@test.com")
@dataset
class SessionStats:
    user_id: int = field(key=True)
    timestamp: datetime = field(timestamp=True)
    avg_count: float
    avg_length: float
    last_visitor_session: List[Window]

    @pipeline
    @inputs(Sessions)
    def get_session_stats(cls, sessions: Dataset):
        stats = (
            sessions.assign(
                "length",
                int,
                lambda df: df["window"].apply(
                    lambda x: int((x["end"] - x["begin"]).total_seconds())
                ),
            )
            .assign(
                "count",
                int,
                lambda df: df["window"].apply(lambda x: x["count"]),
            )
            .groupby("user_id")
            .aggregate(
                Average(
                    of="length",
                    window="forever",
                    into_field="avg_length",
                ),
                Average(
                    of="count",
                    window="forever",
                    into_field="avg_count",
                ),
                LastK(
                    of="window",
                    window="forever",
                    limit=1,
                    dedup=False,
                    into_field="last_visitor_session",
                ),
            )
        )
        return stats


@meta(owner="test@test.com")
@featureset
class UserSessionStats:
    user_id: int = feature(id=1)
    avg_count: float = feature(id=2)
    avg_length: float = feature(id=3)
    last_visitor_session: List[Window] = feature(id=4)

    @extractor(depends_on=[SessionStats])
    @inputs(user_id)
    @outputs(avg_count, avg_length, last_visitor_session)
    def extract_cast(cls, ts: pd.Series, user_ids: pd.Series):
        res, _ = SessionStats.lookup(ts, user_id=user_ids, fields=["avg_count", "avg_length", "last_visitor_session"])  # type: ignore
        return res


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
    }
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "AppEvent", df)
    print(response.json())
    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_window_operator(client):
    # Sync to mock client
    client.commit(
        datasets=[AppEvent, Sessions, SessionStats],
        featuresets=[UserSessionStats],
    )

    # Log data to test the pipeline
    log_app_events_data(client)

    client.sleep()

    now = datetime.now()
    ts = pd.Series([now])
    user_id_keys = pd.Series([1])
    window_keys = pd.Series(
        [
            {
                "begin": datetime(2023, 1, 16, 11, 0, 25),
                "end": datetime(2023, 1, 16, 11, 0, 34),
                "count": 8,
            }
        ]
    )
    input_df = pd.DataFrame({"UserSessionStats.user_id": [1]})
    input_extract_historical_df = pd.DataFrame(
        {
            "UserSessionStats.user_id": [1],
            "timestamp": [datetime(2023, 1, 16, 11, 0, 11)],
        }
    )

    df_session, _ = Sessions.lookup(
        ts, user_id=user_id_keys, window=window_keys
    )
    assert df_session.shape[0] == 1
    assert df_session["user_id"].values == [1]
    assert df_session["window"].values[0].begin == datetime(
        2023, 1, 16, 11, 0, 25
    )
    assert df_session["window"].values[0].end == datetime(
        2023, 1, 16, 11, 0, 34
    )
    assert df_session["window"].values[0].count == 8

    df_stats, _ = SessionStats.lookup(ts, user_id=user_id_keys)
    assert df_stats.shape[0] == 1
    assert df_stats["user_id"].values == [1]
    assert df_stats["avg_length"].values == [2.5]
    # Round to 3 places before comparing
    assert round(df_stats["avg_count"].values[0], 3) == round(3.333333, 3)

    df_featureset = client.query(
        inputs=[
            UserSessionStats.user_id,
        ],
        outputs=[
            UserSessionStats.avg_count,
            UserSessionStats.avg_length,
            UserSessionStats.last_visitor_session,
        ],
        input_dataframe=input_df,
    )
    assert df_featureset.shape[0] == 1
    assert df_featureset["UserSessionStats.avg_length"].values == [2.5]
    assert round(
        df_featureset["UserSessionStats.avg_count"].values[0], 3
    ) == round(3.333333, 3)
    assert df_featureset["UserSessionStats.last_visitor_session"].size == 1

    if client.is_integration_client():
        return

    df_historical = client.query_offline(
        input_dataframe=input_extract_historical_df,
        inputs=["UserSessionStats.user_id"],
        outputs=[
            "UserSessionStats.avg_count",
            "UserSessionStats.avg_length",
            "UserSessionStats.last_visitor_session",
        ],
        timestamp_column="timestamp",
        format="pandas",
    )
    assert df_historical.shape[0] == 1
    assert df_historical["UserSessionStats.avg_length"].values == [3]
    assert df_historical["UserSessionStats.avg_count"].values == [4]
    assert df_featureset["UserSessionStats.last_visitor_session"].size == 1
