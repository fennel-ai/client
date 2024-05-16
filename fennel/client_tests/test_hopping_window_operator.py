from datetime import datetime, timezone
from typing import List

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel import connectors
from fennel.connectors import source
from fennel.datasets import dataset, Dataset, pipeline, field
from fennel.dtypes import Continuous
from fennel.featuresets import featureset, extractor
from fennel.lib.aggregate import Average, LastK
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs, Window, struct
from fennel.testing import mock

webhook = connectors.Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("AppEvent"), cdc="append", disorder="14d")
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
    window_stats: WindowStats

    @pipeline()
    @inputs(AppEvent)
    def get_sessions(cls, app_event: Dataset):
        return (
            app_event.groupby("user_id")
            .window(
                type="hopping", stride="5s", duration="10s", into_field="window"
            )
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
@dataset(index=True)
class SessionStats:
    user_id: int = field(key=True)
    timestamp: datetime = field(timestamp=True)
    avg_count: float
    avg_length: float
    last_visitor_session: List[Window]
    avg_star: float

    @pipeline()
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
                    window=Continuous("forever"),
                    into_field="avg_length",
                ),
                Average(
                    of="count",
                    window=Continuous("forever"),
                    into_field="avg_count",
                ),
                LastK(
                    of="window",
                    window=Continuous("forever"),
                    limit=1,
                    dedup=False,
                    into_field="last_visitor_session",
                ),
                Average(
                    of="avg_star",
                    window=Continuous("forever"),
                    into_field="avg_star",
                ),
            )
        )
        return stats


@meta(owner="test@test.com")
@featureset
class UserSessionStats:
    user_id: int
    avg_count: float
    avg_length: float
    last_visitor_session: List[Window]
    avg_star: float

    @extractor(deps=[SessionStats])  # type: ignore
    @inputs("user_id")
    @outputs("avg_count", "avg_length", "last_visitor_session", "avg_star")
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
    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_hopping_window_operator(client):
    # Sync to mock client
    client.commit(
        message="Initial commit",
        datasets=[AppEvent, Sessions, SessionStats],
        featuresets=[UserSessionStats],
    )

    # Log data to test the pipeline
    log_app_events_data(client)

    client.sleep()

    now = datetime.now(timezone.utc)
    ts = pd.Series([now])
    user_id_keys = pd.Series([1])
    window_keys = pd.Series(
        [
            {
                "begin": pd.Timestamp(
                    datetime(2023, 1, 16, 11, 0, 0, tzinfo=timezone.utc)
                ),
                "end": pd.Timestamp(
                    datetime(2023, 1, 16, 11, 0, 10, tzinfo=timezone.utc)
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
    df_session, found = client.lookup(
        "Sessions",
        keys=pd.DataFrame(
            {
                "user_id": user_id_keys,
                "window": window_keys,
            }
        ),
    )
    assert list(found) == [True]
    assert df_session.shape[0] == 1
    assert df_session["user_id"].values == [1]
    if client.is_integration_client():
        assert df_session["window"].values[0].begin == datetime(
            2023, 1, 16, 11, 0, 0
        )
    else:
        assert df_session["window"].values[0].begin == datetime(
            2023, 1, 16, 11, 0, 0, tzinfo=timezone.utc
        )
    if client.is_integration_client():
        assert df_session["window"].values[0].end == datetime(
            2023, 1, 16, 11, 0, 10
        )
    else:
        assert df_session["window"].values[0].end == datetime(
            2023, 1, 16, 11, 0, 10, tzinfo=timezone.utc
        )
    assert df_session["window_stats"].values[0].count == 6
    assert df_session["window_stats"].values[0].avg_star == pytest.approx(
        2.8333333333
    )

    df_stats, _ = SessionStats.lookup(ts, user_id=user_id_keys)
    assert df_stats.shape[0] == 1
    assert list(df_stats["user_id"].values) == [1]
    assert list(df_stats["avg_length"].values) == [10.0]
    assert list(df_stats["avg_count"].values) == [pytest.approx(4.44444444)]

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
    assert list(df_featureset["UserSessionStats.avg_length"].values) == [10.0]
    assert list(df_featureset["UserSessionStats.avg_count"].values) == [
        pytest.approx(4.44444444)
    ]

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
    assert list(df_historical["UserSessionStats.avg_length"].values) == [10.0]
    assert list(df_historical["UserSessionStats.avg_count"].values) == [4.5]
