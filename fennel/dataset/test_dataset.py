import json
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import requests
from google.protobuf.json_format import ParseDict

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
        'datasetRequests': [
            {
                'name': 'UserInfoDataset',
                'fields': [
                    {
                        'name': 'user_id',
                        'isKey': True,
                    },
                    {
                        'name': 'name',
                    },
                    {
                        'name': 'gender',
                    },
                    {
                        'name': 'dob',
                        'description': 'Users date of birth',
                    },
                    {
                        'name': 'age',
                    },
                    {
                        'name': 'account_creation_date',
                    },
                    {
                        'name': 'country',
                        'isNullable': True
                    },
                    {
                        'name': 'timestamp',
                        'isTimestamp': True,
                    }
                ],
                'maxStaleness': 2592000000000,
                'retention': 63072000000000,
                'mode': 'pandas',
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b''
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request


@dataset(retention='4m')
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
        'datasetRequests': [
            {
                'name': 'Activity',
                'fields': [
                    {
                        'name': 'user_id',
                    },
                    {
                        'name': 'action_type',
                    },
                    {
                        'name': 'amount',
                        'isNullable': True
                    },
                    {
                        'name': 'timestamp',
                        'isTimestamp': True,
                    }
                ],
                'maxStaleness': 2592000000000,
                'retention': 10368000000000,
                'mode': 'pandas',
            }
        ]
    }
    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b''
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request


def _clean_function_source_code(dataset_req: proto.CreateDatasetRequest) -> \
        proto.CreateDatasetRequest:
    def cleanup_node(node):
        if node.HasField('operator') and node.operator.HasField('transform'):
            return proto.Node(operator=proto.Operator(
                transform=proto.Transform(
                    node=cleanup_node(node.operator.transform.node),
                    timestamp_field=node.operator.transform.timestamp_field,
                ), id=node.operator.id))
        elif node.HasField('operator') and node.operator.HasField('aggregate'):
            return proto.Node(operator=proto.Operator(
                aggregate=proto.Aggregate(node=cleanup_node(
                    node.operator.aggregate.node),
                    keys=node.operator.aggregate.keys,
                    aggregates=node.operator.aggregate.aggregates),
                id=node.operator.id))
        elif node.HasField('operator') and node.operator.HasField('join'):
            return proto.Node(operator=proto.Operator(
                join=proto.Join(node=cleanup_node(
                    node.operator.join.node),
                    dataset=node.operator.join.dataset,
                    on=node.operator.join.on),
                id=node.operator.id))
        elif node.HasField('operator') and node.operator.HasField('union'):
            return proto.Node(operator=proto.Operator(
                union=proto.Union(nodes=[cleanup_node(n) for n in
                                         node.operator.union.nodes]),
                id=node.operator.id))

        return node

    dataset_req.pull_lookup.function_source_code = ''
    dataset_req.pull_lookup.function = b''
    pipelines = []
    for j in range(len(dataset_req.pipelines)):
        pipelines.append(proto.Pipeline(
            root=cleanup_node(dataset_req.pipelines[j].root),
            nodes=[cleanup_node(n) for n in dataset_req.pipelines[j].nodes],
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
        schema=b'',
        pull_lookup=dataset_req.pull_lookup,
    )


def test_DatasetWithPull(grpc_stub):
    API_ENDPOINT_URL = 'http://transunion.com/v1/credit_score'

    @dataset(retention='1y', max_staleness='7d')
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
                json={'users': user_list, 'names': names})
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
        'name': 'UserCreditScore',
        'fields': [
            {
                'name': 'user_id',
                'isKey': True,
            },
            {
                'name': 'credit_score',
            },
            {
                'name': 'timestamp',
                'isTimestamp': True,
            },
        ],
        'retention': 31536000000000,
        'maxStaleness': 604800000000,
        'mode': 'pandas',
        'pullLookup': {
            'functionSourceCode': '',
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
                    "nodeId": "13c0d419337774e6f121b3dbece60f43"
                },
                "nodes": [
                    {
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
                            },
                            "id": "13c0d419337774e6f121b3dbece60f43"
                        }
                    }
                ],
                "signature": "13c0d419337774e6f121b3dbece60f43",
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
                "signature": "C",
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
        "pullLookup": {

        }
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
                lambda df: df[df['action_type'] == 'report_txn'])
            ds = filtered_ds.join(user_info, on=['user_id'], )
            ds = ds.transform(extract_info)
            return ds.groupby('merchant_id', 'timestamp').aggregate([
                Count(window=Window(),
                    name='num_merchant_fraudulent_transactions'),
                Count(window=Window('1w'),
                    name='num_merchant_fraudulent_transactions_7d'),
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
                    "nodeId": "dc951d37e6c9ca90936c8ce20ace9231"
                },
                "nodes": [
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "dataset": {
                                        "name": "Activity"
                                    }
                                }
                            },
                            "id": "5ae551fe033331a8a05f26ec882b319e"
                        }
                    },
                    {
                        "operator": {
                            "join": {
                                "node": {
                                    "nodeId": "5ae551fe033331a8a05f26ec882b319e"
                                },
                                "dataset": {
                                    "name": "UserInfoDataset"
                                },
                                "on": {
                                    "user_id": "user_id"
                                }
                            },
                            "id": "38ee67b85e42419377483191d5cd58b2"
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "nodeId": "38ee67b85e42419377483191d5cd58b2"
                                }
                            },
                            "id": "e02f638c43418f5e8a89af7d364cfc5d"
                        }
                    },
                    {
                        "operator": {
                            "aggregate": {
                                "node": {
                                    "nodeId": "e02f638c43418f5e8a89af7d364cfc5d"
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
                            },
                            "id": "dc951d37e6c9ca90936c8ce20ace9231"
                        }
                    }
                ],
                "signature": "dc951d37e6c9ca90936c8ce20ace9231",
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
        def pipeline1(a: A, b: B):
            def convert(df: pd.DataFrame) -> pd.DataFrame:
                df['a1'] = df['b1']
                df['a1'] = df['a1'].astype(int) * 2
                df['t'] = df['t2']
                return df[['a1', 't']]

            return a + b.transform(convert, timestamp='t')

        @pipeline
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
            {
                "name": "a1",
                "isKey": True
            },
            {
                "name": "t",
                "isTimestamp": True
            }
        ],
        "pipelines": [
            {
                "root": {
                    "nodeId": "a7e105e5fcd003a6562a0cefd15f265f"
                },
                "nodes": [
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "dataset": {
                                        "name": "B"
                                    }
                                },
                                "timestampField": "t"
                            },
                            "id": "a4146b5ca835026f8d38ad11d0d259c0"
                        }
                    },
                    {
                        "operator": {
                            "union": {
                                "nodes": [
                                    {
                                        "dataset": {
                                            "name": "A"
                                        }
                                    },
                                    {
                                        "nodeId": "a4146b5ca835026f8d38ad11d0d259c0"
                                    }
                                ]
                            },
                            "id": "a7e105e5fcd003a6562a0cefd15f265f"
                        }
                    }
                ],
                "signature": "a7e105e5fcd003a6562a0cefd15f265f",
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
                    "nodeId": "1d566a7237120ebb7915e90ffa2a4d16"
                },
                "nodes": [
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "dataset": {
                                        "name": "A"
                                    }
                                }
                            },
                            "id": "b634ba084bceb1189960dd90b4125e2f"
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "dataset": {
                                        "name": "A"
                                    }
                                }
                            },
                            "id": "30dee9d73c82508a1c43dcb0e840cf66"
                        }
                    },
                    {
                        "operator": {
                            "union": {
                                "nodes": [
                                    {
                                        "nodeId": "b634ba084bceb1189960dd90b4125e2f"
                                    },
                                    {
                                        "nodeId": "30dee9d73c82508a1c43dcb0e840cf66"
                                    }
                                ]
                            },
                            "id": "a355e8ff14e236f6872e570fb416f9e8"
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "nodeId": "a355e8ff14e236f6872e570fb416f9e8"
                                }
                            },
                            "id": "1ddb237041014cc5b4d4404197e0d55d"
                        }
                    },
                    {
                        "operator": {
                            "transform": {
                                "node": {
                                    "nodeId": "a355e8ff14e236f6872e570fb416f9e8"
                                }
                            },
                            "id": "19434f7f12999339b786c204f83217bc"
                        }
                    },
                    {
                        "operator": {
                            "union": {
                                "nodes": [
                                    {
                                        "nodeId": "1ddb237041014cc5b4d4404197e0d55d"
                                    },
                                    {
                                        "nodeId": "19434f7f12999339b786c204f83217bc"
                                    }
                                ]
                            },
                            "id": "1d566a7237120ebb7915e90ffa2a4d16"
                        }
                    }
                ],
                "signature": "1d566a7237120ebb7915e90ffa2a4d16",
                "inputs": [
                    {
                        "name": "A"
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
