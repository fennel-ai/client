import sys
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from fennel.connectors import Webhook, eval, source
from fennel.datasets import (
    Dataset,
    dataset,
    field,
    pipeline,
)
from fennel.expr import col
from fennel.lib import inputs, meta
from fennel.testing import mock

__owner__ = "nitin@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@pytest.mark.integration
@mock
def test_where(client):
    if sys.version_info >= (3, 10):

        @meta(owner="nitin@fennel.ai")
        @source(
            webhook.endpoint("A1"),
            cdc="append",
            disorder="14d",
            env="prod",
            where=lambda x: ((x["age"] > 0) & (x["age"] == 1)),
        )
        @source(
            webhook.endpoint("A1"),
            cdc="append",
            disorder="14d",
            env="staging",
            # repeat the conditions to test long lamda
            where=lambda x: (
                (x["age"] > 0)
                & (x["age"] == 1)
                & (x["age"] > 0)
                & (x["age"] == 1)
                & True
                & (x["age"] > 0)
                & (x["age"] == 1)
                & (x["age"] > 0)
                & (x["age"] == 1)
                & True
                & (x["age"] > 0)
                & (x["age"] == 1)
                & (x["age"] > 0)
                & (x["age"] == 1)
                & True
            ),
        )
        @dataset
        class A1:
            user_id: int
            age: int
            t: datetime

        @dataset(index=True)
        class A2:
            user_id: int = field(key=True)
            age: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").latest()

        client.commit(datasets=[A1, A2], message="first_commit", env="prod")

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 2, 2, 3, 4, 5, 5, 5],
                "age": [100, 11, 12, 1, 2, 2, 3, 3, 4, 5],
                "t": [now, now, now, now, now, now, now, now, now, now],
            }
        )

        client.sleep(30)
        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A2,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
        )
        assert df["age"].tolist() == [pd.NA, 1, pd.NA, pd.NA, pd.NA]


@pytest.mark.integration
@mock
def test_where_expression(client):
    if sys.version_info >= (3, 10):

        @meta(owner="satwant@fennel.ai")
        @source(
            webhook.endpoint("A1"),
            cdc="append",
            disorder="14d",
            env="prod",
            where=eval(
                (col("age") > 0) & (col("age") % 100 == 1), schema={"age": int}
            ),
        )
        @dataset
        class A1:
            user_id: int
            age: int
            t: datetime

        @dataset(index=True)
        class A2:
            user_id: int = field(key=True)
            age: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").latest()

        client.commit(datasets=[A1, A2], message="first_commit", env="prod")

        now = datetime.now(timezone.utc)
        df = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 2, 2, 3, 4, 5, 5, 5],
                "age": [100, 11, 12, 1, 2, 2, 3, 3, 4, 5],
                "t": [now, now, now, now, now, now, now, now, now, now],
            }
        )
        df["age"] = df["age"].astype(pd.Int64Dtype())

        client.sleep(30)
        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A2,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
        )
        assert df["age"].tolist() == [pd.NA, 1, pd.NA, pd.NA, pd.NA]


@pytest.mark.integration
@mock
def test_where_expression_with_additional_schema(client):
    if sys.version_info >= (3, 10):

        @meta(owner="satwant@fennel.ai")
        @source(
            webhook.endpoint("A1"),
            cdc="append",
            disorder="14d",
            env="prod",
            where=eval(col("name").str.len() <= 5, schema={"name": str}),
        )
        @dataset
        class A1:
            user_id: int
            age: int
            t: datetime

        @dataset(index=True)
        class A2:
            user_id: int = field(key=True)
            age: int
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").latest()

        client.commit(datasets=[A1, A2], message="first_commit", env="prod")

        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        day_before = yesterday - timedelta(days=1)

        df = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 2, 2, 3, 4, 5, 5, 5],
                "age": [100, 11, 12, 1, 2, 2, 3, 3, 4, 5],
                "name": [
                    "Bob", "Bob", "Bob", "Jon", "Jon",
                    "Alice", "Jonathon", "Christopher",
                    "Christopher", "Christopher"
                ],
                "t": [
                    day_before, yesterday, now,
                    yesterday, now, now, now,
                    day_before, yesterday, now
                ],
            }
        )
        df["name"] = df["name"].astype(pd.StringDtype())

        client.sleep(30)
        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A2,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
        )
        assert df["age"].tolist() == [12, 2, 2, pd.NA, pd.NA]


@pytest.mark.integration
@mock
def test_where_expression_with_additional_schema_and_preproc(client):
    if sys.version_info >= (3, 10):

        @meta(owner="satwant@fennel.ai")
        @source(
            webhook.endpoint("A1"),
            cdc="append",
            disorder="14d",
            env="prod",
            preproc={"name": eval(col("first_name"), schema={"first_name": str})},
            where=eval(col("name").str.len() <= 5, schema={"name": str}),
        )
        @dataset
        class A1:
            user_id: int
            age: int
            name: str
            t: datetime

        @dataset(index=True)
        class A2:
            user_id: int = field(key=True)
            age: int
            name: str
            t: datetime

            @pipeline
            @inputs(A1)
            def pipeline_window(cls, event: Dataset):
                return event.groupby("user_id").latest()

        client.commit(datasets=[A1, A2], message="first_commit", env="prod")

        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        day_before = yesterday - timedelta(days=1)

        df = pd.DataFrame(
            {
                "user_id": [1, 1, 1, 2, 2, 3, 4, 5, 5, 5],
                "age": [100, 11, 12, 1, 2, 2, 3, 3, 4, 5],
                "first_name": [
                    "Bob", "Bob", "Bob", "Jon", "Jon",
                    "Alice", "Jonathon", "Christopher",
                    "Christopher", "Christopher"
                ],
                "t": [
                    day_before, yesterday, now,
                    yesterday, now, now, now,
                    day_before, yesterday, now
                ],
            }
        )
        df["first_name"] = df["first_name"].astype(pd.StringDtype())

        client.sleep(30)
        client.log("fennel_webhook", "A1", df)
        client.sleep(60)

        df, _ = client.lookup(
            A2,
            keys=pd.DataFrame({"user_id": [1, 2, 3, 4, 5]}),
        )
        assert df["age"].tolist() == [12, 2, 2, pd.NA, pd.NA]
