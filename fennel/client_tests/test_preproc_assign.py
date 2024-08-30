import sys
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pytest

from fennel.connectors import Webhook, source, eval
from fennel.datasets import (
    dataset,
    field,
)
from fennel.expr import col, lit
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@pytest.mark.integration
@mock
def test_simple_preproc_assign(client):
    if sys.version_info >= (3, 10):

        @source(
            webhook.endpoint("A1"),
            cdc="upsert",
            disorder="14d",
            preproc={
                "age": eval(
                    col("buggy_age") + lit(1), schema={"buggy_age": int}
                ),
                "height": eval(
                    lambda x: x["height_str"].apply(lambda y: int(y)),
                    schema={"height_str": str},
                ),
            },
        )
        @dataset(index=True)
        class A1:
            user_id: int = field(key=True)
            age: int
            height: int
            t: datetime

        client.commit(datasets=[A1], message="first_commit")

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "buggy_age": [10, 11, 12, 13, 14],
                "height_str": ["10", "11", "12", "13", "14"],
                "t": [now, now, now, now, now],
            }
        )

        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A1,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
        )
        assert df["age"].tolist() == [11, 12, 13, 14, 15]
        assert df["height"].tolist() == [10, 11, 12, 13, 14]


@pytest.mark.integration
@mock
def test_remove_null_preproc_assign(client):
    if sys.version_info >= (3, 10):

        @source(
            webhook.endpoint("A1"),
            cdc="upsert",
            disorder="14d",
            preproc={
                "user_id": eval(
                    col("user_id").fillnull(-1).astype(int),
                    schema={"user_id": Optional[int]},
                ),
            },
            where=lambda x: x["user_id"] != -1,
        )
        @dataset(index=True)
        class A1:
            user_id: int = field(key=True)
            height: int
            t: datetime

        client.commit(datasets=[A1], message="first_commit")

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5, None],
                "height": [10, 11, 12, 13, 14, 15],
                "t": [now, now, now, now, now, now],
            }
        )
        df["user_id"] = df["user_id"].astype(pd.Int64Dtype())

        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A1,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5, -1]}),
        )
        assert df["height"].tolist() == [10, 11, 12, 13, 14, pd.NA]


@pytest.mark.integration
@mock
def test_change_dtype_preproc_assign(client):
    if sys.version_info >= (3, 10):

        @source(
            webhook.endpoint("A1"),
            cdc="upsert",
            disorder="14d",
            preproc={
                "age": eval(
                    lambda x: pd.to_numeric(x["age"]),
                    schema={"age": str},
                ),
            },
        )
        @dataset(index=True)
        class A1:
            user_id: int = field(key=True)
            age: int
            t: datetime

        client.commit(datasets=[A1], message="first_commit")

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 2, 3, 4, 5],
                "age": ["10", "11", "12", "13", "14"],
                "t": [now, now, now, now, now],
            }
        )

        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A1,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
        )
        assert df["age"].tolist() == [10, 11, 12, 13, 14]
