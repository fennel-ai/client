from datetime import datetime

import pandas as pd
import pytest

from fennel.testing import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.datasets import (
        dataset,
        field,
        pipeline,
        Dataset,
        Average,
    )
    from fennel.dtypes import Continuous
    from fennel.lib import inputs
    from fennel.connectors import source, Webhook

    webhook = Webhook(name="webhook")

    @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
    @dataset
    class Transaction:
        uid: int
        amt: int
        timestamp: datetime

    @dataset(index=True)
    class Aggregated:
        uid: int = field(key=True)
        # docsnip-highlight start
        avg_1d: float
        avg_1w: float
        # docsnip-highlight end
        timestamp: datetime

        @pipeline
        @inputs(Transaction)
        def avg_pipeline(cls, ds: Dataset):
            return ds.groupby("uid").aggregate(
                # docsnip-highlight start
                avg_1d=Average(of="amt", window=Continuous("1d"), default=-1.0),
                avg_1w=Average(of="amt", window=Continuous("1w"), default=-1.0),
                # docsnip-highlight end
            )

    # /docsnip
    client.commit(message="msg", datasets=[Transaction, Aggregated])
    # log some rows to the transaction dataset
    client.log(
        "webhook",
        "Transaction",
        pd.DataFrame(
            [
                {
                    "uid": 1,
                    "vendor": "A",
                    "amt": 10,
                    "timestamp": "2021-01-01T00:00:00",
                },
                {
                    "uid": 1,
                    "vendor": "B",
                    "amt": 20,
                    "timestamp": "2021-01-02T00:00:00",
                },
                {
                    "uid": 2,
                    "vendor": "A",
                    "amt": 30,
                    "timestamp": "2021-01-03T00:00:00",
                },
                {
                    "uid": 2,
                    "vendor": "B",
                    "amt": 40,
                    "timestamp": "2021-01-04T00:00:00",
                },
                {
                    "uid": 3,
                    "vendor": "A",
                    "amt": 50,
                    "timestamp": "2021-01-05T00:00:00",
                },
                {
                    "uid": 3,
                    "vendor": "B",
                    "amt": 60,
                    "timestamp": "2021-01-06T00:00:00",
                },
            ]
        ),
    )

    # do lookup on the Aggregated dataset
    ts = pd.Series(
        [
            datetime(2021, 1, 6, 0, 0, 0),
            datetime(2021, 1, 6, 0, 0, 0),
            datetime(2021, 1, 6, 0, 0, 0),
        ]
    )
    df, found = Aggregated.lookup(ts, uid=pd.Series([1, 2, 3]))
    assert found.tolist() == [True, True, True]
    assert df["uid"].tolist() == [1, 2, 3]
    assert df["avg_1d"].tolist() == [-1.0, -1.0, 55.0]
    assert df["avg_1w"].tolist() == [15.0, 35.0, 55.0]


@mock
def test_invalid_type(client):
    with pytest.raises(Exception):
        # docsnip incorrect_type
        from fennel.datasets import dataset, field, pipeline, Dataset, Average
        from fennel.dtypes import Continuous
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            # docsnip-highlight next-line
            zip: str
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            avg_1d: str
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def invalid_pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    # docsnip-highlight start
                    avg_1d=Average(
                        of="zip", window=Continuous("1d"), default="avg"
                    ),
                    # docsnip-highlight end
                )

        # /docsnip


@mock
def test_non_matching_types(client):
    with pytest.raises(Exception):
        # docsnip non_matching_types
        from fennel.datasets import dataset, field, pipeline, Dataset, Average
        from fennel.dtypes import Continuous
        from fennel.lib import inputs
        from fennel.connectors import source, Webhook

        webhook = Webhook(name="webhook")

        @source(webhook.endpoint("Transaction"), disorder="14d", cdc="append")
        @dataset
        class Transaction:
            uid: int
            amt: float
            timestamp: datetime

        @dataset
        class Aggregated:
            uid: int = field(key=True)
            # docsnip-highlight start
            # output of avg is always float
            ret: int
            # docsnip-highlight end
            timestamp: datetime

            @pipeline
            @inputs(Transaction)
            def invalid_pipeline(cls, ds: Dataset):
                return ds.groupby("uid").aggregate(
                    # docsnip-highlight start
                    ret=Average(of="amt", window=Continuous("1d"), default=1.0),
                    # docsnip-highlight end
                )

        # /docsnip
