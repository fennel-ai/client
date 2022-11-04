import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.protobuf.json_format import ParseDict
from typing import Optional

import fennel.gen.dataset_pb2 as proto
from fennel.dataset import dataset, field, pipeline
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
                    {
                        "name": "country",
                        "isNullable": True
                    },
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    }
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
    assert sync_request == expected_sync_request


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
                "fields": [
                    {
                        "name": "user_id",
                    },
                    {
                        "name": "action_type",
                    },
                    {
                        "name": "amount",
                        "isNullable": True
                    },
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    }
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
    assert sync_request == expected_sync_request


def _clean_function_source_code(dataset_req: proto.CreateDatasetRequest) -> \
        proto.CreateDatasetRequest:
    def cleanup_node(node):
        if node.HasField("operator") and node.operator.HasField("transform"):
            return proto.Node(operator=proto.Operator(
                transform=proto.Transform(
                    node=cleanup_node(node.operator.transform.node), )))
        elif node.HasField("operator") and node.operator.HasField("aggregate"):
            return proto.Node(operator=proto.Operator(
                aggregate=proto.Aggregate(node=cleanup_node(
                    node.operator.aggregate.node),
                    keys=node.operator.aggregate.keys,
                    aggregates=node.operator.aggregate.aggregates)))
        elif node.HasField("operator") and node.operator.HasField("join"):
            return proto.Node(operator=proto.Operator(
                join=proto.Join(node=cleanup_node(
                    node.operator.join.node),
                    dataset=node.operator.join.dataset,
                    on=node.operator.join.on)))
        return node

    dataset_req.pull_lookup.function_source_code = ""
    dataset_req.pull_lookup.function = b""
    pipelines = []
    for j in range(len(dataset_req.pipelines)):
        pipelines.append(proto.Pipeline(
            root=cleanup_node(
                dataset_req.pipelines[j].root),
            signature=dataset_req.pipelines[
                j].signature,
            inputs=dataset_req.pipelines[j].inputs,
        ))
    return proto.CreateDatasetRequest(
        name=dataset_req.name,
        fields=dataset_req.fields,
        max_staleness=dataset_req.max_staleness,
        retention=dataset_req.retention,
        mode=dataset_req.mode,
        pipelines=pipelines,
        schema=b"",
        pull_lookup=dataset_req.pull_lookup,
    )


def test_DatasetWithPull(grpc_stub):
    API_ENDPOINT_URL = 'http://transunion.com/v1/credit_score'

    @dataset(retention="1y", max_staleness='7d')
    class UserCreditScore:
        user_id: int = field(key=True)
        credit_score: float
        timestamp: datetime

        @staticmethod
        def pull(user_id: pd.Series, names: pd.Series,
                 timestamps: pd.Series) -> pd.DataFrame:
            user_list = user_id.tolist()
            names = names.tolist()
            resp = requests.get(API_ENDPOINT_URL,
                json={"users": user_list, "names": names})
            df = pd.DataFrame(columns=['user_id', 'credit_score', 'timestamp'])
            if resp.status_code != 200:
                return df
            results = resp.json()['results']
            df['user_id'] = user_id
            df['names'] = names
            df['timestamp'] = timestamps
            df['credit_score'] = pd.Series(results)
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
        }
    }

    # Ignoring schema validation since they are bytes and not human-readable
    dataset_req = _clean_function_source_code(
        sync_request.dataset_requests[0])
    expected_ds_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_ds_request


def test_DatasetWithPipes(grpc_stub):
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime
        pass

    @dataset
    class B:
        b1: int = field(key=True)
        t: datetime
        pass

    @dataset
    class C:
        t: datetime
        pass

    @dataset
    class ABCDataset:
        a: int = field(key=True)
        b: int = field(key=True)
        c: int
        d: datetime

        @staticmethod
        @pipeline
        def pipeline1(a: A, b: B):
            return a.join(b, left_on=['a1'], right_on=['b1'])

        @staticmethod
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
            {
                "name": "a",
                "isKey": True
            },
            {
                "name": "b",
                "isKey": True
            },
            {
                "name": "c"
            },
            {
                "name": "d",
                "isTimestamp": True
            }
        ],
        "pipelines": [
            {
                "root": {
                    "operator": {
                        "join": {
                            "node": {
                                "dataset": {
                                    "name": "A"
                                }
                            },
                            "dataset": {
                                "name": "B"
                            },
                            "on": {
                                "a1": "b1"
                            }
                        }
                    }
                },
                "inputs": [
                    {
                        "name": "A"
                    },
                    {
                        "name": "B"
                    }
                ]
            },
            {
                "root": {
                    "dataset": {
                        "name": "C"
                    }
                },
                "inputs": [
                    {
                        "name": "A"
                    },
                    {
                        "name": "B"
                    },
                    {
                        "name": "C"
                    }
                ]
            }
        ],
        "retention": "63072000000000",
        "maxStaleness": "2592000000000",
        "mode": "pandas",
        "pullLookup": {}
    }
    dataset_req = _clean_function_source_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request


def test_DatasetWithComplexPipe(grpc_stub):
    @dataset
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int
        num_merchant_fraudulent_transactions_7d: int

        @staticmethod
        @pipeline
        def create_fraud_dataset(activity: Activity,
                                 user_info: UserInfoDataset):
            def extract_info(df: pd.DataFrame) -> pd.DataFrame:
                df['metadata_dict'] = df['metadata'].apply(json.loads).apply(
                    pd.Series)
                df['transaction_amount'] = df['metadata_dict'].apply(
                    lambda x: x['transaction_amt'])
                df['timestamp'] = df['metadata_dict'].apply(
                    lambda x: x['transaction_amt'])
                df['merchant_id'] = df['metadata_dict'].apply(
                    lambda x: x['merchant_id'])
                return df[
                    ['merchant_id', 'transaction_amount', 'user_id',
                     'timestamp']]

            filtered_ds = activity.transform(
                lambda df: df[df["action_type"] == "report_txn"])
            ds = filtered_ds.join(user_info, on=["user_id"], )
            ds = ds.transform(extract_info)
            return ds.groupby("merchant_id", "timestamp").aggregate([
                Count(window=Window(),
                    name="num_merchant_fraudulent_transactions"),
                Count(window=Window("1w"),
                    name="num_merchant_fraudulent_transactions_7d"),
            ])

    view = InternalTestView(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "name": "FraudReportAggregatedDataset",
        "fields": [
            {
                "name": "merchant_id",
                "isKey": True
            },
            {
                "name": "timestamp",
                "isTimestamp": True
            },
            {
                "name": "num_merchant_fraudulent_transactions"
            },
            {
                "name": "num_merchant_fraudulent_transactions_7d"
            }
        ],
        "pipelines": [
            {
                "root": {
                    "operator": {
                        "aggregate": {
                            "node": {
                                "operator": {
                                    "transform": {
                                        "node": {
                                            "operator": {
                                                "join": {
                                                    "node": {
                                                        "operator": {
                                                            "transform": {
                                                                "node": {
                                                                    "dataset": {
                                                                        "name": "Activity"
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    },
                                                    "dataset": {
                                                        "name": "UserInfoDataset"
                                                    },
                                                    "on": {
                                                        "user_id": "user_id"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            },
                            "keys": [
                                "merchant_id",
                                "timestamp"
                            ],
                            "aggregates": [
                                {
                                    "type": "COUNT",
                                    "windowSpec": {
                                        "foreverWindow": True
                                    }
                                },
                                {
                                    "type": "COUNT",
                                    "windowSpec": {
                                        "window": {
                                            "start": "604800000000"
                                        }
                                    }
                                }
                            ]
                        }
                    }
                },
                "inputs": [
                    {
                        "name": "Activity"
                    },
                    {
                        "name": "UserInfoDataset"
                    }
                ]
            }
        ],
        "retention": "63072000000000",
        "maxStaleness": "2592000000000",
        "mode": "pandas",
        "pullLookup": {}
    }

    # Ignoring schema validation since they are bytes and not human-readable
    dataset_req = _clean_function_source_code(sync_request.dataset_requests[0])
    expected_dataset_request = ParseDict(d, proto.CreateDatasetRequest())
    assert dataset_req == expected_dataset_request
