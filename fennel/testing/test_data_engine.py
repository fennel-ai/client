from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field
from fennel.testing import mock, log

__owner__ = "aditya@fennel.ai"

from fennel.testing.execute_aggregation import FENNEL_DELETE_TIMESTAMP


@dataset(index=True)
class KeyedTestDataset:
    key1: int = field(key=True)
    key2: str = field(key=True)
    val1: float
    val2: str
    ts: datetime


@dataset
class UnkeyedTestDataset:
    key1: int
    key2: str
    val1: float
    val2: str
    ts: datetime


@mock
def test_add_delete_timestamps(client):
    df = pd.DataFrame(
        {
            "key1": [1, 2, 3, 1, 1, 2],
            "key2": ["a", "b", "c", "a", "a", "b"],
            "val1": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "val2": ["x", "y", "z", "p", "q", "r"],
            "ts": [
                datetime(2021, 1, 1),
                datetime(2021, 1, 2),
                datetime(2021, 1, 3),
                datetime(2021, 1, 4),
                datetime(2021, 1, 5),
                datetime(2021, 1, 6),
            ],
        }
    )

    client.commit(datasets=[KeyedTestDataset], message="Add data")
    log(KeyedTestDataset, df)

    internal_df = (
        client.branches_map["main"]
        .data_engine.datasets["KeyedTestDataset"]
        .data
    )
    # There should be 3 deleted rows
    deleted_rows = internal_df[internal_df[FENNEL_DELETE_TIMESTAMP].notna()]
    assert deleted_rows.shape[0] == 3
    assert internal_df.loc[0, FENNEL_DELETE_TIMESTAMP] == pd.Timestamp(
        "2021-01-04 00:00:00+0000", tz="UTC"
    ) - pd.Timedelta("1us")
    assert internal_df.loc[1, FENNEL_DELETE_TIMESTAMP] == pd.Timestamp(
        "2021-01-06 00:00:00+0000", tz="UTC"
    ) - pd.Timedelta("1us")
    assert internal_df.loc[3, FENNEL_DELETE_TIMESTAMP] == pd.Timestamp(
        "2021-01-05 00:00:00+0000", tz="UTC"
    ) - pd.Timedelta("1us")

    client.commit(datasets=[UnkeyedTestDataset], message="Add data")
    log(UnkeyedTestDataset, df)

    internal_df = client.get_dataset_df("UnkeyedTestDataset")
    # Assert both dataframes are similar sized
    assert internal_df.shape == df.shape


def _y2d(y):
    return datetime(y, 1, 1)


@mock
def test_dataset_lookup(client):
    df = pd.DataFrame(
        {
            "key1": [1, 2, 3, 1, 1, 2],
            "key2": ["a", "b", "c", "a", "a", "b"],
            "val1": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            "val2": ["x", "y", "z", "p", "q", "r"],
            "ts": [
                _y2d(2000),
                _y2d(2010),
                _y2d(2020),
                _y2d(2030),
                _y2d(2040),
                _y2d(2050),
            ],
        }
    )

    client.commit(datasets=[KeyedTestDataset], message="Add data")
    log(KeyedTestDataset, df)

    keys = pd.DataFrame(
        {
            "key1": [1, 1, 1, 2, 2],
            "key2": ["a", "a", "a", "b", "b"],
        }
    )
    results, found = client.lookup(
        KeyedTestDataset,
        keys=keys,
        timestamps=pd.Series(
            [_y2d(1999), _y2d(2002), _y2d(2032), _y2d(2042), _y2d(2052)]
        ),
    )
    assert found.to_list() == [
        False,
        True,
        True,
        True,
        True,
    ]
    assert results["val1"].tolist() == [pd.NA, 1.0, 4.0, 2.0, 6.0]
    assert results["val2"].to_list() == [pd.NA, "x", "p", "y", "r"]
