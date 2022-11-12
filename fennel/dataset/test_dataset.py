import json
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests  # type: ignore
from google.protobuf.json_format import ParseDict

import fennel.gen.dataset_pb2 as proto
from fennel.dataset import dataset, pipeline, field, Dataset
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.aggregate import Count
from fennel.lib.metadata import meta
from fennel.lib.window import Window
from fennel.test_lib import *


@meta(owner="test@test.com")
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
    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "UserInfoDataset",
                "fields": [
                    {"name": "user_id", "isKey": True, "metadata": {}},
                    {"name": "name", "metadata": {}},
                    {"name": "gender", "metadata": {}},
                    {
                        "name": "dob",
                        "metadata": {"description": "Users date of birth"},
                    },
                    {"name": "age", "metadata": {}},
                    {"name": "account_creation_date", "metadata": {}},
                    {"name": "country", "isNullable": True, "metadata": {}},
                    {"name": "timestamp", "isTimestamp": True, "metadata": {}},
                ],
                "signature": "3cb848e839199cd8161e095dc1ebf536",
                "metadata": {"owner": "test@test.com"},
                "mode": "pandas",
                "retention": "63072000000000",
                "maxStaleness": "2592000000000",
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


@meta(owner="test@test.com")
@dataset(retention="120d")
class Activity:
    user_id: int
    action_type: float
    amount: Optional[float]
    timestamp: datetime


def test_DatasetWithRetention(grpc_stub):
    assert Activity._max_staleness == timedelta(days=30)
    assert Activity._retention == timedelta(days=120)
    view = InternalTestClient(grpc_stub)
    view.add(Activity)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "Activity",
                "fields": [
                    {"name": "user_id", "metadata": {}},
                    {"name": "action_type", "metadata": {}},
                    {"name": "amount", "isNullable": True, "metadata": {}},
                    {"name": "timestamp", "isTimestamp": True, "metadata": {}},
                ],
                "signature": "2ec6c8c90cc6df1b6eb0258a2bdc2b1c",
                "metadata": {"owner": "test@test.com"},
                "mode": "pandas",
                "retention": "10368000000000",
                "maxStaleness": "2592000000000",
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

    @meta(owner="test@test.com")
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
    view = InternalTestClient(grpc_stub)
    view.add(UserCreditScore)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "UserCreditScore",
        "fields": [
            {"name": "user_id", "isKey": True, "metadata": {}},
            {"name": "credit_score", "metadata": {}},
            {"name": "timestamp", "isTimestamp": True, "metadata": {}},
        ],
        "mode": "pandas",
        "metadata": {"owner": "test@test.com"},
        "retention": "31536000000000",
        "maxStaleness": "604800000000",
        "pullLookup": {},
    }

    # Ignoring schema validation since they are bytes and not human-readable
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_ds_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_ds_request, error_message(
        dataset_req, expected_ds_request
    )


def test_DatasetWithPipes(grpc_stub):
    @meta(owner="test@test.com")
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @meta(owner="test@test.com")
    @dataset
    class B:
        b1: int = field(key=True)
        t: datetime

    @meta(owner="test@test.com")
    @dataset
    class C:
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDataset:
        a: int = field(key=True)
        b: int = field(key=True)
        c: int
        d: datetime

        @staticmethod
        @pipeline(A, B)
        def pipeline1(a: Dataset, b: Dataset):
            return a.join(b, left_on=["a1"], right_on=["b1"])

        @staticmethod
        @pipeline(A, B, C)
        def pipeline2(a: Dataset, b: Dataset, c: Dataset):
            return c

    view = InternalTestClient(grpc_stub)
    view.add(ABCDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "ABCDataset",
        "fields": [
            {"name": "a", "isKey": True, "metadata": {}},
            {"name": "b", "isKey": True, "metadata": {}},
            {"name": "c", "metadata": {}},
            {"name": "d", "isTimestamp": True, "metadata": {}},
        ],
        "pipelines": [
            {
                "nodes": [
                    {
                        "id": "816d3f87d7dc94cfb4c9d8513e0d9234",
                        "operator": {
                            "join": {
                                "lhsNodeId": "A",
                                "rhsDatasetName": "B",
                                "on": {"a1": "b1"},
                            }
                        },
                    }
                ],
                "root": "816d3f87d7dc94cfb4c9d8513e0d9234",
                "signature": "ABCDataset.816d3f87d7dc94cfb4c9d8513e0d9234",
                "inputs": ["A", "B"],
            },
            {
                "root": "C",
                "signature": "ABCDataset.C",
                "inputs": ["A", "B", "C"],
            },
        ],
        "mode": "pandas",
        "metadata": {"owner": "aditya@fennel.ai"},
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
    @meta(owner="test@test.com")
    @dataset
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int
        num_merchant_fraudulent_transactions_7d: int

        @staticmethod
        @pipeline(Activity, UserInfoDataset)
        def create_fraud_dataset(activity: Dataset, user_info: Dataset):
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
            ds_transform = ds.transform(extract_info)
            return ds_transform.groupby("merchant_id").aggregate(
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

    view = InternalTestClient(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "FraudReportAggregatedDataset",
        "fields": [
            {"name": "merchant_id", "isKey": True, "metadata": {}},
            {"name": "timestamp", "isTimestamp": True, "metadata": {}},
            {"name": "num_merchant_fraudulent_transactions", "metadata": {}},
            {"name": "num_merchant_fraudulent_transactions_7d", "metadata": {}},
        ],
        "pipelines": [
            {
                "nodes": [
                    {
                        "id": "44fb25a177c0daa12b22a96e1d0b9b77",
                        "operator": {
                            "transform": {"operandNodeId": "Activity"}
                        },
                    },
                    {
                        "id": "4839efdf9e0f3526ce9d222c09596b8a",
                        "operator": {
                            "join": {
                                "lhsNodeId": "44fb25a177c0daa12b22a96e1d0b9b77",
                                "rhsDatasetName": "UserInfoDataset",
                                "on": {"user_id": "user_id"},
                            }
                        },
                    },
                    {
                        "id": "24089ee9611046fd829aebce51b9d3a2",
                        "operator": {
                            "transform": {
                                "operandNodeId": "4839efdf9e0f3526ce9d222c09596b8a"
                            }
                        },
                    },
                    {
                        "id": "0e1d7d7f8ecb0ded643709f413ebdd9d",
                        "operator": {
                            "aggregate": {
                                "operandNodeId": "24089ee9611046fd829aebce51b9d3a2",
                                "keys": ["merchant_id"],
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
                            }
                        },
                    },
                ],
                "root": "0e1d7d7f8ecb0ded643709f413ebdd9d",
                "signature": "FraudReportAggregatedDataset.0e1d7d7f8ecb0ded643709f413ebdd9d",
                "inputs": ["Activity", "UserInfoDataset"],
            }
        ],
        "mode": "pandas",
        "metadata": {"owner": "test@test.com"},
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

    @meta(owner="test@test.com")
    @dataset
    class ABCDataset:
        a1: int = field(key=True)
        t: datetime

        @staticmethod
        @pipeline(A, B)
        def pipeline1(a: Dataset, b: Dataset):
            def convert(df: pd.DataFrame) -> pd.DataFrame:
                df["a1"] = df["b1"]
                df["a1"] = df["a1"].astype(int) * 2
                df["t"] = df["t2"]
                return df[["a1", "t"]]

            return a + b.transform(convert, timestamp="t")

        @staticmethod
        @pipeline(A)
        def pipeline2_diamond(a: Dataset):
            b = a.transform(lambda df: df)
            c = a.transform(lambda df: df * 2)
            d = b + c
            e = d.transform(lambda df: df * 3)
            f = d.transform(lambda df: df * 4)
            return e + f

    view = InternalTestClient(grpc_stub)
    view.add(ABCDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "ABCDataset",
        "fields": [
            {"name": "a1", "isKey": True, "metadata": {}},
            {
                "name": "t",
                "isTimestamp": True,
                "metadata": {},
            },
        ],
        "pipelines": [
            {
                "nodes": [
                    {
                        "id": "2ab0c0f6f921e2362d17484f05a2a9a5",
                        "operator": {
                            "transform": {
                                "operandNodeId": "B",
                                "timestampField": "t",
                            }
                        },
                    },
                    {
                        "id": "8afd27c0a99adea847df61e3b7bcab5e",
                        "operator": {
                            "union": {
                                "operandNodeIds": [
                                    "A",
                                    "2ab0c0f6f921e2362d17484f05a2a9a5",
                                ]
                            }
                        },
                    },
                ],
                "root": "8afd27c0a99adea847df61e3b7bcab5e",
                "signature": "ABCDataset.8afd27c0a99adea847df61e3b7bcab5e",
                "inputs": ["A", "B"],
            },
            {
                "nodes": [
                    {
                        "id": "62b55ed86a3147da80cc9f6533d804a7",
                        "operator": {"transform": {"operandNodeId": "A"}},
                    },
                    {
                        "id": "7120ae27df71756bf09abf2c2a056711",
                        "operator": {"transform": {"operandNodeId": "A"}},
                    },
                    {
                        "id": "88e9b9c78fe82996482c517d8a2d0cc8",
                        "operator": {
                            "union": {
                                "operandNodeIds": [
                                    "62b55ed86a3147da80cc9f6533d804a7",
                                    "7120ae27df71756bf09abf2c2a056711",
                                ]
                            }
                        },
                    },
                    {
                        "id": "2445ff121a8c4f3fcf55cc450a2d3e0b",
                        "operator": {
                            "transform": {
                                "operandNodeId": "88e9b9c78fe82996482c517d8a2d0cc8"
                            }
                        },
                    },
                    {
                        "id": "32256da42d662b0d99de4acdadc91818",
                        "operator": {
                            "transform": {
                                "operandNodeId": "88e9b9c78fe82996482c517d8a2d0cc8"
                            }
                        },
                    },
                    {
                        "id": "bf50512eeb345ab02b8ad15d6b8c8c86",
                        "operator": {
                            "union": {
                                "operandNodeIds": [
                                    "2445ff121a8c4f3fcf55cc450a2d3e0b",
                                    "32256da42d662b0d99de4acdadc91818",
                                ]
                            }
                        },
                    },
                ],
                "root": "bf50512eeb345ab02b8ad15d6b8c8c86",
                "signature": "ABCDataset.bf50512eeb345ab02b8ad15d6b8c8c86",
                "inputs": ["A"],
            },
        ],
        "mode": "pandas",
        "metadata": {"owner": "test@test.com"},
        "retention": "63072000000000",
        "maxStaleness": "2592000000000",
        "pullLookup": {},
    }
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )
