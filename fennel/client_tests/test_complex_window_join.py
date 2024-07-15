from datetime import datetime, date, timezone, timedelta

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import Webhook, source
from fennel.datasets import dataset, Dataset, field, pipeline
from fennel.dtypes import Window, Tumbling
from fennel.lib import inputs, bucketize
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "nitin@fennel.ai"


@source(webhook.endpoint("AppEvent"), disorder="14d", cdc="append")
@dataset
class AppEvent:
    user_id: int
    page_id: str
    timestamp: datetime = field(timestamp=True)


@dataset(index=True)
class AppEventDailySession:
    user_id: int = field(key=True)
    window: Window = field(key=True)
    timestamp: datetime = field(timestamp=True)

    @pipeline
    @inputs(AppEvent)
    def pipeline(cls, app_event: Dataset):
        return app_event.groupby("user_id", window=Tumbling("1d")).aggregate(
            emit="final"
        )


@source(webhook.endpoint("UserDataset"), disorder="14d", cdc="append")
@dataset
class UserDataset:
    user_id: int
    timestamp: datetime = field(timestamp=True)


@dataset(index=True)
class DailyStats:
    user_id: int = field(key=True)
    activity_date: date = field(key=True)
    timestamp: datetime = field(timestamp=True)
    window: Window

    @pipeline
    @inputs(UserDataset, AppEventDailySession)
    def pipeline(cls, user_dataset: Dataset, daily_session: Dataset):
        return (
            user_dataset.assign(
                "activity_date", date, lambda x: x["timestamp"].dt.date
            )
            .assign(
                "window",
                Window,
                lambda x: x["timestamp"].apply(
                    lambda y: bucketize(y, window=Tumbling("1d"))
                ),
            )
            .join(daily_session, on=["user_id", "window"], how="inner")
            .groupby("user_id", "activity_date")
            .latest()
        )


@pytest.mark.integration
@mock
def test_window_join(client):
    response = client.commit(
        datasets=[AppEvent, AppEventDailySession, UserDataset, DailyStats],
        featuresets=[],
        message="first_commit",
    )
    assert response.status_code == requests.codes.OK, response.json()

    now = datetime.now(timezone.utc)
    now_1d = datetime.now(timezone.utc) - timedelta(days=1)
    now_2d = datetime.now(timezone.utc) - timedelta(days=2)
    now_3d = datetime.now(timezone.utc) - timedelta(days=3)

    app_event = pd.DataFrame(
        {
            "user_id": [1] * 11,
            "timestamp": [
                now_3d,
                now_3d,
                now_2d,
                now_2d,
                now_1d,
                now_2d,
                now_2d,
                now_1d,
                now_3d,
                now_3d,
                now,
            ],
            "page_id": ["login_page"] * 11,
        }
    )
    response = client.log("fennel_webhook", "AppEvent", app_event)
    assert response.status_code == requests.codes.OK, response.json()

    user_dataset = pd.DataFrame(
        {
            "user_id": [1] * 10,
            "timestamp": [
                now_2d,
                now_2d,
                now_2d,
                now_2d,
                now_1d,
                now_1d,
                now,
                now,
                now,
                now,
            ],
        }
    )
    response = client.log("fennel_webhook", "UserDataset", user_dataset)
    assert response.status_code == requests.codes.OK, response.json()
    client.sleep(30)

    df, _ = client.lookup(
        AppEventDailySession,
        keys=pd.DataFrame(
            {
                "user_id": [1, 1, 1],
                "window": [
                    {
                        "begin": now_1d.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat(),
                        "end": now.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat(),
                    },
                    {
                        "begin": now_2d.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat(),
                        "end": now_1d.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat(),
                    },
                    {
                        "begin": now_3d.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat(),
                        "end": now_2d.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ).isoformat(),
                    },
                ],
            }
        ),
    )
    assert df["timestamp"].tolist() == [
        now.replace(hour=0, minute=0, second=0, microsecond=0),
        now_1d.replace(hour=0, minute=0, second=0, microsecond=0),
        now_2d.replace(hour=0, minute=0, second=0, microsecond=0),
    ]

    df, _ = client.lookup(
        DailyStats,
        keys=pd.DataFrame(
            {
                "user_id": [1, 1, 1],
                "activity_date": [
                    str(now.date()),
                    str(now_1d.date()),
                    str(now_2d.date()),
                ],
            }
        ),
    )

    assert [
        x.as_json() if pd.notna(x) else x for x in df["window"].tolist()
    ] == [
        {
            "begin": now_1d.replace(hour=0, minute=0, second=0, microsecond=0),
            "end": now.replace(hour=0, minute=0, second=0, microsecond=0),
        },
        {
            "begin": now_2d.replace(hour=0, minute=0, second=0, microsecond=0),
            "end": now_1d.replace(hour=0, minute=0, second=0, microsecond=0),
        },
        {
            "begin": now_3d.replace(hour=0, minute=0, second=0, microsecond=0),
            "end": now_2d.replace(hour=0, minute=0, second=0, microsecond=0),
        },
    ]
