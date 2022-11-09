import json
import typing
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests  # type: ignore
from google.protobuf.json_format import ParseDict

import fennel.gen.dataset_pb2 as proto
from fennel.dataset import dataset, pipeline, field
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.aggregate import Count
from fennel.lib.window import Window
from fennel.test_lib import *


@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    gender: str
    # Users date of birth
    dob: str
    age: int
    account_creation_date: datetime
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


def test_SimpleDataset(grpc_stub):
    assert UserInfoDataset._max_staleness == timedelta(days=30)
    assert UserInfoDataset._retention == timedelta(days=730)
    view = InternalTestView(grpc_stub)
    view.add(UserInfoDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "UserInfoDataset",
                "signature": "be97463e5a8eb09c87a55084dac59234",
                "fields": [
                    {
                        "name": "user_id",
                        "isKey": True,
                    },
                    {
                        "name": "name",
                    },
                    {
                        "name": "gender",
                    },
                    {
                        "name": "dob",
                        "description": "Users date of birth",
                    },
                    {
                        "name": "age",
                    },
                    {
                        "name": "account_creation_date",
                    },
                    {"name": "country", "isNullable": True},
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    },
                ],
                "maxStaleness": 2592000000000,
                "retention": 63072000000000,
                "mode": "pandas",
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


@dataset(retention="4m")
class Activity:
    user_id: int
    action_type: float
    amount: Optional[float]
    timestamp: datetime


def test_DatasetWithRetention(grpc_stub):
    assert Activity._max_staleness == timedelta(days=30)
    assert Activity._retention == timedelta(days=120)
    view = InternalTestView(grpc_stub)
    view.add(Activity)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "Activity",
                "signature": "80a9e6e74588df2c425b67e9d40b046f",
                "fields": [
                    {
                        "name": "user_id",
                    },
                    {
                        "name": "action_type",
                    },
                    {"name": "amount", "isNullable": True},
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    },
                ],
                "maxStaleness": 2592000000000,
                "retention": 10368000000000,
                "mode": "pandas",
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_DatasetWithPull(grpc_stub):
    API_ENDPOINT_URL = "http://transunion.com/v1/credit_score"

    @dataset(retention="1y", max_staleness="7d")
    class UserCreditScore:
        user_id: int = field(key=True)
        credit_score: float
        timestamp: datetime

        @staticmethod
        def pull(
            user_id: pd.Series, names: pd.Series, timestamps: pd.Series
        ) -> pd.DataFrame:
            user_list = user_id.tolist()
            names = names.tolist()
            resp = requests.get(
                API_ENDPOINT_URL, json={"users": user_list, "names": names}
            )
            df = pd.DataFrame(columns=["user_id", "credit_score", "timestamp"])
            if resp.status_code != 200:
                return df
            results = resp.json()["results"]
            df["user_id"] = user_id
            df["names"] = names
            df["timestamp"] = timestamps
            df["credit_score"] = pd.Series(results)
            return df

    assert UserCreditScore._max_staleness == timedelta(days=7)
    assert UserCreditScore._retention == timedelta(days=365)
    view = InternalTestView(grpc_stub)
    view.add(UserCreditScore)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "UserCreditScore",
        "fields": [
            {
                "name": "user_id",
                "isKey": True,
            },
            {
                "name": "credit_score",
            },
            {
                "name": "timestamp",
                "isTimestamp": True,
            },
        ],
        "retention": 31536000000000,
        "maxStaleness": 604800000000,
        "mode": "pandas",
        "pullLookup": {
            "functionSourceCode": "",
        },
    }

    # Ignoring schema validation since they are bytes and not human-readable
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_ds_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_ds_request, error_message(
        dataset_req, expected_ds_request
    )


def test_DatasetWithPipes(grpc_stub):
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @dataset
    class B:
        b1: int = field(key=True)
        t: datetime

    @dataset
    class C:
        t: datetime

    @dataset
    class ABCDataset:
        a: int = field(key=True)
        b: int = field(key=True)
        c: int
        d: datetime

        @pipeline
        def pipeline1(a: A, b: B):
            return a.join(b, left_on=["a1"], right_on=["b1"])  # type: ignore

        @pipeline
        def pipeline2(a: A, b: B, c: C):
            return c

    view = InternalTestView(grpc_stub)
    view.add(ABCDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "ABCDataset",
        "fields": [
            {"name": "a", "isKey": True},
            {"name": "b", "isKey": True},
            {"name": "c"},
            {"name": "d", "isTimestamp": True},
        ],
        "pipelines": [
            {
                "root": {"nodeId": "816d3f87d7dc94cfb4c9d8513e0d9234"},
                "nodes": [
                    {
                        "operator": {
                            "join": {
                                "node": {"dataset": {"name": "A"}},
                                "dataset": {"name": "B"},
                                "on": {"a1": "b1"},
                            },
                            "id": "816d3f87d7dc94cfb4c9d8513e0d9234",
                        }
                    }
                ],
                "signature": "816d3f87d7dc94cfb4c9d8513e0d9234",
                "inputs": [{"name": "A"}, {"name": "B"}],
            },
            {
                "root": {"dataset": {"name": "C"}},
                "signature": "C",
                "inputs": [{"name": "A"}, {"name": "B"}, {"name": "C"}],
            },
        ],
        "mode": "pandas",
        "retention": "63072000000000",
        "maxStaleness": "2592000000000",
        "pullLookup": {},
    }
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


def test_DatasetWithComplexPipe(grpc_stub):
    @dataset
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int
        num_merchant_fraudulent_transactions_7d: int

        @pipeline
        @typing.no_type_check
        def create_fraud_dataset(
            activity: Activity, user_info: UserInfoDataset
        ):
            def extract_info(df: pd.DataFrame) -> pd.DataFrame:
                df["metadata_dict"] = (
                    df["metadata"].apply(json.loads).apply(pd.Series)
                )
                df["transaction_amount"] = df["metadata_dict"].apply(
                    lambda x: x["transaction_amt"]
                )
                df["timestamp"] = df["metadata_dict"].apply(
                    lambda x: x["transaction_amt"]
                )
                df["merchant_id"] = df["metadata_dict"].apply(
                    lambda x: x["merchant_id"]
                )
                return df[
                    [
                        "merchant_id",
                        "transaction_amount",
                        "user_id",
                        "timestamp",
                    ]
                ]

            filtered_ds = activity.transform(
                lambda df: df[df["action_type"] == "report_txn"]
            )
            ds = filtered_ds.join(
                user_info,
                on=["user_id"],
            )
            ds = ds.transform(extract_info)
            return ds.groupby("merchant_id", "timestamp").aggregate(
                [
                    Count(
                        window=Window(),
                        name="num_merchant_fraudulent_transactions",
                    ),
                    Count(
                        window=Window("1w"),
                        name="num_merchant_fraudulent_transactions_7d",
                    ),
                ]
            )

    view = InternalTestView(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "FraudReportAggregatedDataset",
        "fields": [
            {"name": "merchant_id", "isKey": True},
            {"name": "timestamp", "isTimestamp": True},
            {"name": "num_merchant_fraudulent_transactions"},
            {"name": "num_merchant_fraudulent_transactions_7d"},
        ],
        "pipelines": [
            {
                "root": {"nodeId": "88f2ca3744539c938468ed3875aeced2"},
                "nodes": [
                    {
                        "operator": {
                            "transform": {
                                "node": {"dataset": {"name": "Activity"}}
                            },
                            "id": "3ce7cd3364f302f5c84804e3834a6649",
                        }
                    },
                    {
                        "operator": {
                            "join": {
                                "node": {
                                    "nodeId": "3ce7cd3364f302f5c84804e3834a6649"
                                },
                                "dataset": {"name": "UserInfoDataset"},
                                "on": {"user_id": "user_id"},
                            },
                            "id": "86383e405dc4a9a638ca27b6444b9384",
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "nodeId": "86383e405dc4a9a638ca27b6444b9384"
                                }
                            },
                            "id": "36374f97b2523d41e62df5bedd69de84",
                        }
                    },
                    {
                        "operator": {
                            "aggregate": {
                                "node": {
                                    "nodeId": "36374f97b2523d41e62df5bedd69de84"
                                },
                                "keys": ["merchant_id", "timestamp"],
                                "aggregates": [
                                    {
                                        "type": "COUNT",
                                        "windowSpec": {"foreverWindow": True},
                                    },
                                    {
                                        "type": "COUNT",
                                        "windowSpec": {
                                            "window": {"start": "604800000000"}
                                        },
                                    },
                                ],
                            },
                            "id": "88f2ca3744539c938468ed3875aeced2",
                        }
                    },
                ],
                "signature": "88f2ca3744539c938468ed3875aeced2",
                "inputs": [{"name": "Activity"}, {"name": "UserInfoDataset"}],
            }
        ],
        "mode": "pandas",
        "retention": "63072000000000",
        "maxStaleness": "2592000000000",
        "pullLookup": {},
    }

    # Ignoring schema validation since they are bytes and not human-readable
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


def test_UnionDatasets(grpc_stub):
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @dataset
    class B:
        b1: int = field(key=True)
        t2: datetime

    @dataset
    class ABCDataset:
        a1: int = field(key=True)
        t: datetime

        @pipeline
        @typing.no_type_check
        def pipeline1(a: A, b: B):
            def convert(df: pd.DataFrame) -> pd.DataFrame:
                df["a1"] = df["b1"]
                df["a1"] = df["a1"].astype(int) * 2
                df["t"] = df["t2"]
                return df[["a1", "t"]]

            return a + b.transform(convert, timestamp="t")

        @pipeline
        @typing.no_type_check
        def pipeline2_diamond(a: A):
            b = a.transform(lambda df: df)
            c = a.transform(lambda df: df * 2)
            d = b + c
            e = d.transform(lambda df: df * 3)
            f = d.transform(lambda df: df * 4)
            return e + f

    view = InternalTestView(grpc_stub)
    view.add(ABCDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "ABCDataset",
        "fields": [
            {"name": "a1", "isKey": True},
            {"name": "t", "isTimestamp": True},
        ],
        "pipelines": [
            {
                "root": {"nodeId": "03b6c4403eaec702c28449014c7b9099"},
                "nodes": [
                    {
                        "operator": {
                            "transform": {
                                "node": {"dataset": {"name": "B"}},
                                "timestampField": "t",
                            },
                            "id": "7ed6f7376dfcc5de3f92f26a5231c9d4",
                        }
                    },
                    {
                        "operator": {
                            "union": {
                                "nodes": [
                                    {"dataset": {"name": "A"}},
                                    {
                                        "nodeId": "7ed6f7376dfcc5de3f92f26a5231c9d4"
                                    },
                                ]
                            },
                            "id": "03b6c4403eaec702c28449014c7b9099",
                        }
                    },
                ],
                "signature": "03b6c4403eaec702c28449014c7b9099",
                "inputs": [{"name": "A"}, {"name": "B"}],
            },
            {
                "root": {"nodeId": "e06da8d90a0bf45075191dd34e31c2f7"},
                "nodes": [
                    {
                        "operator": {
                            "transform": {"node": {"dataset": {"name": "A"}}},
                            "id": "d7fc6b5df9256b0db0bc8affb34d49af",
                        }
                    },
                    {
                        "operator": {
                            "transform": {"node": {"dataset": {"name": "A"}}},
                            "id": "1b9bdd9c128aa5af52cab7ee67e1fc0a",
                        }
                    },
                    {
                        "operator": {
                            "union": {
                                "nodes": [
                                    {
                                        "nodeId": "d7fc6b5df9256b0db0bc8affb34d49af"
                                    },
                                    {
                                        "nodeId": "1b9bdd9c128aa5af52cab7ee67e1fc0a"
                                    },
                                ]
                            },
                            "id": "dff7e7ce7a347cf76d2fc090bb9923d9",
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "nodeId": "dff7e7ce7a347cf76d2fc090bb9923d9"
                                }
                            },
                            "id": "7deb097e70beb5fa67fcbe334580ba16",
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "nodeId": "dff7e7ce7a347cf76d2fc090bb9923d9"
                                }
                            },
                            "id": "c5c872b9e957f6ec0262e237b11ca81e",
                        }
                    },
                    {
                        "operator": {
                            "union": {
                                "nodes": [
                                    {
                                        "nodeId": "7deb097e70beb5fa67fcbe334580ba16"
                                    },
                                    {
                                        "nodeId": "c5c872b9e957f6ec0262e237b11ca81e"
                                    },
                                ]
                            },
                            "id": "e06da8d90a0bf45075191dd34e31c2f7",
                        }
                    },
                ],
                "signature": "e06da8d90a0bf45075191dd34e31c2f7",
                "inputs": [{"name": "A"}],
            },
        ],
        "mode": "pandas",
        "retention": "63072000000000",
        "maxStaleness": "2592000000000",
        "pullLookup": {},
    }
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )
