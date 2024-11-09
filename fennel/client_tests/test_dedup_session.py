import time
from datetime import datetime, timedelta, timezone
import pandas as pd

from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Sum,
    Count,
)
from fennel.dtypes import Continuous, Session
from fennel.lib import inputs
from fennel.testing import mock, log

__owner__ = "eng@fennel.ai"


@mock
def test_dedup_session(client):
    @dataset
    class DatasetA:
        a: int
        user_id: str
        time: datetime

    @dataset(index=True)
    class AggDataset:
        user_id: str = field(key=True)
        count_a: int
        sum_a: int
        time: datetime

        @pipeline
        @inputs(DatasetA)
        def pipeline(cls, data_a):
            data_b = data_a.dedup(by=["user_id", "a"], window=Session(gap="1s"))
            return data_b.groupby("user_id").aggregate(
                count_a=Count(window=Continuous("forever")),
                sum_a=Sum(of="a", window=Continuous("forever")),
            )

    client.commit(
        message="initial commit",
        datasets=[DatasetA, AggDataset],
    )

    day = timedelta(days=1)
    min = timedelta(minutes=1)
    now = datetime.now(timezone.utc)

    time.sleep(2)
    now2 = datetime.now(timezone.utc)

    columns = ["a", "user_id", "time"]
    data = [
        [1, "u1", now],
        [1, "u1", now],
        [1, "u2", now],
    ]
    df = pd.DataFrame(data, columns=columns)
    log(DatasetA, df)

    data = [
        [1, "u1", now2 + day],
        [2, "u1", now2 + day],
        [0, "u2", now2 + day],
        [5, "u1", now2 + 2 * day + min],
    ]
    df = pd.DataFrame(data, columns=columns)
    log(DatasetA, df)

    data, found = client.lookup(
        "AggDataset",
        timestamps=pd.Series(
            [now + min, now2 + day + min, now2 + 2 * day + 2 * min]
        ),
        keys=pd.DataFrame({"user_id": ["u1", "u1", "u1"]}),
    )
    assert data.shape == (3, 4)
    assert data.iloc[0]["count_a"] == 1
    assert data.iloc[0]["sum_a"] == 1
    assert data.iloc[1]["count_a"] == 3
    assert data.iloc[1]["sum_a"] == 4
    assert data.iloc[2]["count_a"] == 4
    assert data.iloc[2]["sum_a"] == 9
