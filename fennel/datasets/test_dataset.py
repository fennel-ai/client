import json
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import pytest
import requests  # type: ignore
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.dataset_pb2 as proto
from fennel.datasets import dataset, pipeline, field, Dataset, on_demand
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.aggregate import Count
from fennel.lib.metadata import meta
from fennel.lib.schema import Embedding
from fennel.lib.schema import Series
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
    assert UserInfoDataset._history == timedelta(days=730)
    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "UserInfoDataset",
                "fields": [
                    {
                        "name": "user_id",
                        "ftype": "Key",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "name",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "gender",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "dob",
                        "ftype": "Val",
                        "dtype": {"scalarType": "STRING"},
                        "metadata": {"description": "Users date of birth"},
                    },
                    {
                        "name": "age",
                        "ftype": "Val",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "account_creation_date",
                        "ftype": "Val",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                    {
                        "name": "country",
                        "ftype": "Val",
                        "dtype": {"isNullable": True, "scalarType": "STRING"},
                        "metadata": {},
                    },
                    {
                        "name": "timestamp",
                        "ftype": "Timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                ],
                "signature": "88468f81bc9e7be9c988fb90d3299df9",
                "metadata": {"owner": "test@test.com"},
                "mode": "pandas",
                "history": "63072000000000",
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


@meta(owner="test@test.com")
@dataset(history="120d")
class Activity:
    user_id: int
    action_type: float
    amount: Optional[float]
    timestamp: datetime


def test_DatasetWithRetention(grpc_stub):
    assert Activity._history == timedelta(days=120)
    view = InternalTestClient(grpc_stub)
    view.add(Activity)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "Activity",
                "fields": [
                    {
                        "name": "user_id",
                        "ftype": "Val",
                        "dtype": {"scalarType": "INT"},
                        "metadata": {},
                    },
                    {
                        "name": "action_type",
                        "ftype": "Val",
                        "dtype": {"scalarType": "FLOAT"},
                        "metadata": {},
                    },
                    {
                        "name": "amount",
                        "ftype": "Val",
                        "dtype": {"isNullable": True, "scalarType": "FLOAT"},
                        "metadata": {},
                    },
                    {
                        "name": "timestamp",
                        "ftype": "Timestamp",
                        "dtype": {"scalarType": "TIMESTAMP"},
                        "metadata": {},
                    },
                ],
                "signature": "378b4a4e8da9a6f20d989e255ae8d8e5",
                "metadata": {"owner": "test@test.com"},
                "mode": "pandas",
                "history": "10368000000000",
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_DatasetWithPull(grpc_stub):
    API_ENDPOINT_URL = "http://transunion.com/v1/credit_score"

    @meta(owner="test@test.com")
    @dataset(
        history="1y",
    )
    class UserCreditScore:
        user_id: int = field(key=True)
        name: str = field(key=True)
        credit_score: float
        timestamp: datetime

        @on_demand(expires_after="7d")
        def pull_from_api(
            cls, ts: Series[datetime], user_id: Series[int], names: Series[str]
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
            df[str(cls.user_id)] = user_id
            df[str(cls.name)] = names
            df[str(cls.timestamp)] = ts
            df[str(cls.credit_score)] = pd.Series(results)
            return df, pd.Series([True] * len(df))

    assert UserCreditScore._history == timedelta(days=365)
    view = InternalTestClient(grpc_stub)
    view.add(UserCreditScore)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "UserCreditScore",
        "fields": [
            {
                "name": "user_id",
                "ftype": "Key",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "name",
                "ftype": "Key",
                "dtype": {"scalarType": "STRING"},
                "metadata": {},
            },
            {
                "name": "credit_score",
                "ftype": "Val",
                "dtype": {"scalarType": "FLOAT"},
                "metadata": {},
            },
            {
                "name": "timestamp",
                "ftype": "Timestamp",
                "dtype": {"scalarType": "TIMESTAMP"},
                "metadata": {},
            },
        ],
        "mode": "pandas",
        "metadata": {"owner": "test@test.com"},
        "history": "31536000000000",
        "onDemand": {"expiresAfter": "604800000000"},
    }

    # Ignoring schema validation since they are bytes and not human-readable
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_ds_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_ds_request, error_message(
        dataset_req, expected_ds_request
    )

    with pytest.raises(TypeError) as e:

        @meta(owner="test@test.com")
        @dataset(history="1y")
        class UserCreditScore2:
            user_id: int = field(key=True)
            credit_score: float
            timestamp: datetime

            @on_demand
            def pull_from_api(
                cls, user_id: pd.Series, names: pd.Series, timestamps: pd.Series
            ) -> pd.DataFrame:
                pass

    assert (
        str(e.value) == "on_demand must be defined with a parameter "
        "expires_after of type Duration for eg: 30d."
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
        a1: int = field(key=True)
        t: datetime

        @pipeline(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.left_join(b, left_on=["a1"], right_on=["b1"])

    view = InternalTestClient(grpc_stub)
    view.add(ABCDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "ABCDataset",
        "fields": [
            {
                "name": "a1",
                "ftype": "Key",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "t",
                "ftype": "Timestamp",
                "dtype": {"scalarType": "TIMESTAMP"},
                "metadata": {},
            },
        ],
        "pipelines": [
            {
                "nodes": [
                    {
                        "id": "B",
                        "dataset": "B",
                    },
                    {
                        "id": "A",
                        "dataset": "A",
                    },
                    {
                        "id": "816d3f87d7dc94cfb4c9d8513e0d9234",
                        "operator": {
                            "join": {
                                "lhsNodeId": "A",
                                "rhsDatasetName": "B",
                                "on": {"a1": "b1"},
                            }
                        },
                    },
                ],
                "root": "816d3f87d7dc94cfb4c9d8513e0d9234",
                "name": "pipeline1",
                "signature": "ABCDataset.816d3f87d7dc94cfb4c9d8513e0d9234",
                "inputs": ["A", "B"],
            },
        ],
        "mode": "pandas",
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000000000",
        "onDemand": {},
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

        @pipeline(Activity, UserInfoDataset)
        def create_fraud_dataset(cls, activity: Dataset, user_info: Dataset):
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

            filtered_ds = activity.filter(
                lambda df: df[df["action_type"] == "report_txn"]
            )
            ds = filtered_ds.left_join(
                user_info,
                on=["user_id"],
            )
            ds_transform = ds.transform(
                extract_info,
                schema={
                    "merchant_id": int,
                    "transaction_amount": float,
                    "timestamp": datetime,
                    "user_id": int,
                },
            )
            return ds_transform.groupby("merchant_id").aggregate(
                [
                    Count(
                        window=Window("forever"),
                        into_field=str(
                            cls.num_merchant_fraudulent_transactions
                        ),
                    ),
                    Count(
                        window=Window("1w"),
                        into_field=str(
                            cls.num_merchant_fraudulent_transactions_7d
                        ),
                    ),
                ]
            )

    view = InternalTestClient(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "FraudReportAggregatedDataset",
        "fields": [
            {
                "name": "merchant_id",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "timestamp",
                "ftype": "Timestamp",
                "dtype": {"scalarType": "TIMESTAMP"},
                "metadata": {},
            },
            {
                "name": "num_merchant_fraudulent_transactions",
                "ftype": "Val",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "num_merchant_fraudulent_transactions_7d",
                "ftype": "Val",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
        ],
        "pipelines": [
            {
                "nodes": [
                    {"id": "UserInfoDataset", "dataset": "UserInfoDataset"},
                    {"id": "Activity", "dataset": "Activity"},
                    {
                        "id": "227c9aa16517c6c73371a71dfa8aacd2",
                        "operator": {"filter": {}},
                    },
                    {
                        "id": "d41e03e92a5a7e4a01fa04ce487c46ef",
                        "operator": {
                            "join": {
                                "lhsNodeId": "227c9aa16517c6c73371a71dfa8aacd2",
                                "rhsDatasetName": "UserInfoDataset",
                                "on": {"user_id": "user_id"},
                            }
                        },
                    },
                    {
                        "id": "273b322b23d9316ccd54d3eb61c1039d",
                        "operator": {
                            "transform": {
                                "operandNodeId": "d41e03e92a5a7e4a01fa04ce487c46ef",
                                "schema": {
                                    "user_id": {"scalarType": "INT"},
                                    "merchant_id": {"scalarType": "INT"},
                                    "timestamp": {"scalarType": "TIMESTAMP"},
                                    "transaction_amount": {
                                        "scalarType": "FLOAT"
                                    },
                                },
                            }
                        },
                    },
                    {
                        "id": "04b74f251fd2ca9c97c01eb7d48a2dd7",
                        "operator": {
                            "aggregate": {
                                "operandNodeId": "273b322b23d9316ccd54d3eb61c1039d",
                                "keys": ["merchant_id"],
                                "aggregates": [
                                    {
                                        "aggType": "COUNT",
                                        "windowSpec": {"foreverWindow": True},
                                        "field": "num_merchant_fraudulent_transactions",
                                    },
                                    {
                                        "aggType": "COUNT",
                                        "windowSpec": {
                                            "window": {"start": "604800000000"}
                                        },
                                        "field": "num_merchant_fraudulent_transactions_7d",
                                    },
                                ],
                            }
                        },
                    },
                ],
                "root": "04b74f251fd2ca9c97c01eb7d48a2dd7",
                "signature": "FraudReportAggregatedDataset.04b74f251fd2ca9c97c01eb7d48a2dd7",
                "inputs": ["Activity", "UserInfoDataset"],
                "name": "create_fraud_dataset",
            }
        ],
        "metadata": {"owner": "test@test.com"},
        "mode": "pandas",
        "history": "63072000000000",
        "onDemand": {},
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
        t: datetime

    @meta(owner="test@test.com")
    @dataset
    class ABCDataset:
        a1: int = field(key=True)
        t: datetime

        @pipeline(A)
        def pipeline2_diamond(cls, a: Dataset):
            b = a.transform(lambda df: df)
            c = a.transform(lambda df: df * 2)
            d = b + c
            e = d.transform(lambda df: df * 3)
            f = d.transform(lambda df: df * 4)
            return e + f

    view = InternalTestClient(grpc_stub)
    view.add(ABCDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "ABCDataset",
        "fields": [
            {
                "name": "a1",
                "ftype": "Key",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "t",
                "ftype": "Timestamp",
                "dtype": {"scalarType": "TIMESTAMP"},
                "metadata": {},
            },
        ],
        "pipelines": [
            {
                "nodes": [
                    {
                        "id": "A",
                        "dataset": "A",
                    },
                    {
                        "id": "c11a7a6052cdbb1759969dd10613ac8b",
                        "operator": {"transform": {"operandNodeId": "A"}},
                    },
                    {
                        "id": "b1f19f0df67793dfec442938232b07c4",
                        "operator": {"transform": {"operandNodeId": "A"}},
                    },
                    {
                        "id": "3e00aad7fe8a3f2c35b3abeb42540705",
                        "operator": {
                            "union": {
                                "operandNodeIds": [
                                    "c11a7a6052cdbb1759969dd10613ac8b",
                                    "b1f19f0df67793dfec442938232b07c4",
                                ]
                            }
                        },
                    },
                    {
                        "id": "bbea68029557c9f85a5f7fa8f92d632b",
                        "operator": {
                            "transform": {
                                "operandNodeId": "3e00aad7fe8a3f2c35b3abeb42540705"
                            }
                        },
                    },
                    {
                        "id": "95a98aebceb48a64d9b2a8a7001d10df",
                        "operator": {
                            "transform": {
                                "operandNodeId": "3e00aad7fe8a3f2c35b3abeb42540705"
                            }
                        },
                    },
                    {
                        "id": "281385065b983e434ee8dd13c934cc08",
                        "operator": {
                            "union": {
                                "operandNodeIds": [
                                    "bbea68029557c9f85a5f7fa8f92d632b",
                                    "95a98aebceb48a64d9b2a8a7001d10df",
                                ]
                            }
                        },
                    },
                ],
                "root": "281385065b983e434ee8dd13c934cc08",
                "name": "pipeline2_diamond",
                "signature": "ABCDataset.281385065b983e434ee8dd13c934cc08",
                "inputs": ["A"],
            },
        ],
        "metadata": {"owner": "test@test.com"},
        "mode": "pandas",
        "history": "63072000000000",
        "onDemand": {},
    }
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


@meta(owner="e2@company.com")
@dataset
class Document:
    doc_id: int = field(key=True).meta(owner="aditya@fennel.ai")  # type: ignore
    body: str
    title: str
    owner: str
    origin: str
    creation_timestamp: datetime


def get_content_features(df: pd.DataFrame) -> pd.DataFrame:
    pass


def test_SearchDataset(grpc_stub):
    @meta(owner="aditya@fennel.ai")
    @dataset
    class DocumentContentDataset:
        doc_id: int = field(key=True)
        bert_embedding: Embedding[128]
        fast_text_embedding: Embedding[256]
        num_words: int
        num_stop_words: int
        top_10_unique_words: List[str]
        creation_timestamp: datetime

        @pipeline(Document)
        def content_features(
            cls,
            ds: Dataset,
        ):
            return ds.transform(
                get_content_features,
                schema={
                    "doc_id": int,
                    "bert_embedding": Embedding[128],
                    "fast_text_embedding": Embedding[256],
                    "num_words": int,
                    "num_stop_words": int,
                    "top_10_unique_words": List[str],
                    "creation_timestamp": datetime,
                },
            )

    view = InternalTestClient(grpc_stub)
    view.add(DocumentContentDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    dataset_req = clean_ds_func_src_code(sync_request.dataset_requests[0])
    d = {
        "name": "DocumentContentDataset",
        "fields": [
            {"name": "doc_id", "dtype": {"scalarType": "INT"}, "metadata": {}},
            {
                "name": "bert_embedding",
                "ftype": "Val",
                "dtype": {"embeddingType": {"embeddingSize": 128}},
                "metadata": {},
            },
            {
                "name": "fast_text_embedding",
                "ftype": "Val",
                "dtype": {"embeddingType": {"embeddingSize": 256}},
                "metadata": {},
            },
            {
                "name": "num_words",
                "ftype": "Val",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "num_stop_words",
                "ftype": "Val",
                "dtype": {"scalarType": "INT"},
                "metadata": {},
            },
            {
                "name": "top_10_unique_words",
                "ftype": "Val",
                "dtype": {"arrayType": {"of": {"scalarType": "STRING"}}},
                "metadata": {},
            },
            {
                "name": "creation_timestamp",
                "ftype": "Timestamp",
                "dtype": {"scalarType": "TIMESTAMP"},
                "metadata": {},
            },
        ],
        "pipelines": [
            {
                "nodes": [
                    {"id": "Document", "dataset": "Document"},
                    {
                        "id": "ad1a76eb67071a839ac8ec856e90c97c",
                        "operator": {
                            "transform": {
                                "operandNodeId": "Document",
                                "schema": {
                                    "num_words": {"scalarType": "INT"},
                                    "bert_embedding": {
                                        "embeddingType": {"embeddingSize": 128}
                                    },
                                    "num_stop_words": {"scalarType": "INT"},
                                    "fast_text_embedding": {
                                        "embeddingType": {"embeddingSize": 256}
                                    },
                                    "creation_timestamp": {
                                        "scalarType": "TIMESTAMP"
                                    },
                                    "doc_id": {"scalarType": "INT"},
                                    "top_10_unique_words": {
                                        "arrayType": {
                                            "of": {"scalarType": "STRING"}
                                        }
                                    },
                                },
                            }
                        },
                    },
                ],
                "root": "ad1a76eb67071a839ac8ec856e90c97c",
                "name": "content_features",
                "signature": "DocumentContentDataset.ad1a76eb67071a839ac8ec856e90c97c",
                "inputs": ["Document"],
            }
        ],
        "metadata": {"owner": "aditya@fennel.ai"},
        "mode": "pandas",
        "history": "63072000000000",
        "onDemand": {},
    }
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )
