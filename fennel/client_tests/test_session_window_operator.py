from datetime import datetime
from typing import List

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import sources
from fennel.datasets import dataset, Dataset, pipeline, field, Average, LastK
from fennel.featuresets import featureset, feature, extractor
from fennel.lib import meta, inputs, outputs
from fennel.dtypes import Window, struct
from fennel.sources import source
from fennel.testing import mock

webhook = sources.Webhook(name="fennel_webhook")


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
@dataset
class Sessions:
    user_id: int = field(key=True)
    window: Window = field(key=True)
    timestamp: datetime = field(timestamp=True)
    window_stats: WindowStats

    @pipeline
    @inputs(AppEvent)
    def get_sessions(cls, app_event: Dataset):
        return (
            app_event.groupby("user_id")
            .window(type="session", gap="3s", field="window")
            .summarize(
                field="window_stats",
                dtype=WindowStats,
                func=lambda df: {
                    "avg_star": float(df["star"].mean()),
                    "count": len(df),
                },
            )
        )


@meta(owner="test@test.com")
@dataset
class SessionStats:
    user_id: int = field(key=True)
    timestamp: datetime = field(timestamp=True)
    avg_count: float
    avg_length: float
    last_visitor_session: List[Window]
    avg_star: float

    @pipeline
    @inputs(Sessions)
    def get_session_stats(cls, sessions: Dataset):
        stats = (
            sessions.assign(
                "length",
                int,
                lambda df: df["window"].apply(
                    lambda x: int((x["end"] - x["begin"]).total_seconds() - 1)
                ),
            )
            .assign(
                "count",
                int,
                lambda df: df["window_stats"].apply(lambda x: x["count"]),
            )
            .assign(
                "avg_star",
                float,
                lambda df: df["window_stats"].apply(lambda x: x["avg_star"]),
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
                Average(
                    of="avg_star",
                    window="forever",
                    into_field="avg_star",
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
    avg_star: float = feature(id=5)

    @extractor(depends_on=[SessionStats])
    @inputs(user_id)
    @outputs(avg_count, avg_length, last_visitor_session, avg_star)
    def extract_cast(cls, ts: pd.Series, user_ids: pd.Series):
        res, _ = SessionStats.lookup(ts, user_id=user_ids, fields=["avg_count", "avg_length", "last_visitor_session", "avg_star"])  # type: ignore
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
        "star": [1, 2, 3, 2, 4, 5, 1, 4, 2, 3, 1, 5, 3, 1, 5, 2, 3, 5, 2, 4],
    }
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "AppEvent", df)
    print(response.json())
    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_session_window_operator(client):
    # Sync to mock client
    client.commit(
        message="Initial commit",
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
                "begin": pd.Timestamp(datetime(2023, 1, 16, 11, 0, 25)),
                "end": pd.Timestamp(
                    datetime(2023, 1, 16, 11, 0, 33, microsecond=1)
                ),
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
    assert list(df_session["user_id"].values) == [1]
    assert df_session["window"].values[0].begin == datetime(
        2023, 1, 16, 11, 0, 25
    )
    assert df_session["window"].values[0].end == datetime(
        2023, 1, 16, 11, 0, 33, microsecond=1
    )
    print(df_session["window_stats"])
    assert df_session["window_stats"].values[0].count == 8
    assert df_session["window_stats"].values[0].avg_star == 3.125

    df_stats, _ = SessionStats.lookup(ts, user_id=user_id_keys)
    assert df_stats.shape[0] == 1
    assert list(df_stats["user_id"].values) == [1]
    assert list(df_stats["avg_length"].values) == [pytest.approx(1.833333333)]
    assert list(df_stats["avg_count"].values) == [pytest.approx(3.333333)]
    assert list(df_stats["avg_star"].values) == [pytest.approx(2.72083333)]

    df_featureset = client.query(
        inputs=[
            UserSessionStats.user_id,
        ],
        outputs=[
            UserSessionStats.avg_count,
            UserSessionStats.avg_length,
            UserSessionStats.last_visitor_session,
            UserSessionStats.avg_star,
        ],
        input_dataframe=input_df,
    )
    assert df_featureset.shape[0] == 1
    assert list(df_featureset["UserSessionStats.avg_length"].values) == [
        pytest.approx(1.833333333)
    ]
    assert list(df_featureset["UserSessionStats.avg_count"].values) == [
        pytest.approx(3.333333)
    ]
    assert list(df_featureset["UserSessionStats.avg_star"].values) == [
        pytest.approx(2.720833333)
    ]
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
            "UserSessionStats.avg_star",
        ],
        timestamp_column="timestamp",
        format="pandas",
    )
    assert df_historical.shape[0] == 1
    assert list(df_historical["UserSessionStats.avg_length"].values) == [1]
    assert list(df_historical["UserSessionStats.avg_count"].values) == [3]
    assert list(df_featureset["UserSessionStats.avg_star"].values) == [
        pytest.approx(2.720833333)
    ]
    assert df_featureset["UserSessionStats.last_visitor_session"].size == 1
