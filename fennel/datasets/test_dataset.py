import json
from datetime import datetime, timedelta
from typing import Optional, List

import pandas as pd
import pytest
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.dataset_pb2 as ds_proto
from fennel.connectors import source, Webhook, Kafka
from fennel.datasets import (
    dataset,
    pipeline,
    field,
    Dataset,
    Count,
    Average,
    Stddev,
    Sum,
    Quantile,
)
from fennel.dtypes import Embedding, Window, Continuous, Session
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib import includes, meta, inputs, desc
from fennel.testing import *

webhook = Webhook(name="fennel_webhook", retention="30d")
__owner__ = "ml-eng@fennel.ai"


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True).meta(owner="xyz@fennel.ai")  # type: ignore
    name: str
    gender: str
    # Users date of birth
    dob: str
    # Users age
    age: int = field().meta(description="Users age lol")  # type: ignore
    account_creation_date: datetime
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


def test_simple_dataset():
    assert UserInfoDataset._history == timedelta(days=730)
    view = InternalTestClient()
    view.add(UserInfoDataset)
    assert desc(UserInfoDataset.age) == "Users age lol"
    assert desc(UserInfoDataset.dob) == "Users date of birth"

    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "datasets": [
            {
                "name": "UserInfoDataset",
                "metadata": {"owner": "ml-eng@fennel.ai"},
                "version": 1,
                "dsschema": {
                    "keys": {
                        "fields": [
                            {"name": "user_id", "dtype": {"intType": {}}}
                        ]
                    },
                    "values": {
                        "fields": [
                            {"name": "name", "dtype": {"stringType": {}}},
                            {"name": "gender", "dtype": {"stringType": {}}},
                            {"name": "dob", "dtype": {"stringType": {}}},
                            {"name": "age", "dtype": {"intType": {}}},
                            {
                                "name": "account_creation_date",
                                "dtype": {"timestampType": {}},
                            },
                            {
                                "name": "country",
                                "dtype": {
                                    "optionalType": {"of": {"stringType": {}}}
                                },
                            },
                        ]
                    },
                    "timestamp": "timestamp",
                },
                "history": "63072000s",
                "retention": "63072000s",
                "fieldMetadata": {
                    "age": {"description": "Users age lol"},
                    "name": {},
                    "account_creation_date": {},
                    "country": {},
                    "user_id": {"owner": "xyz@fennel.ai"},
                    "gender": {},
                    "timestamp": {},
                    "dob": {"description": "Users date of birth"},
                },
                "pycode": {},
                "isSourceDataset": True,
            }
        ],
        "sources": [
            {
                "table": {
                    "endpoint": {
                        "db": {
                            "name": "fennel_webhook",
                            "webhook": {
                                "name": "fennel_webhook",
                                "retention": "2592000s",
                            },
                        },
                        "endpoint": "UserInfoDataset",
                        "duration": "2592000s",
                    }
                },
                "dataset": "UserInfoDataset",
                "dsVersion": 1,
                "cdc": "Upsert",
                "disorder": "1209600s",
            }
        ],
        "extdbs": [
            {
                "name": "fennel_webhook",
                "webhook": {"name": "fennel_webhook", "retention": "2592000s"},
            }
        ],
        "offlineIndices": [
            {
                "dsName": "UserInfoDataset",
                "dsVersion": 1,
                "duration": {"forever": "forever"},
            }
        ],
        "onlineIndices": [
            {
                "dsName": "UserInfoDataset",
                "dsVersion": 1,
                "duration": {"forever": "forever"},
            }
        ],
    }
    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    sync_request.datasets[0].pycode.Clear()
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_dataset_with_webhook_retention():
    webhook_retention = Webhook(name="webhook_retention", retention="5d")

    @source(webhook_retention.endpoint("Test"), disorder="14d", cdc="upsert")
    @dataset(index=True)
    class Test:
        user_id: int = field(key=True)
        name: str
        timestamp: datetime = field(timestamp=True)

    assert Test._history == timedelta(days=730)
    view = InternalTestClient()
    view.add(Test)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "datasets": [
            {
                "name": "Test",
                "metadata": {"owner": "ml-eng@fennel.ai"},
                "version": 1,
                "dsschema": {
                    "keys": {
                        "fields": [
                            {"name": "user_id", "dtype": {"intType": {}}}
                        ]
                    },
                    "values": {
                        "fields": [
                            {"name": "name", "dtype": {"stringType": {}}},
                        ]
                    },
                    "timestamp": "timestamp",
                },
                "history": "63072000s",
                "retention": "63072000s",
                "fieldMetadata": {
                    "name": {},
                    "user_id": {},
                    "timestamp": {},
                },
                "pycode": {},
                "isSourceDataset": True,
            }
        ],
        "sources": [
            {
                "table": {
                    "endpoint": {
                        "db": {
                            "name": "webhook_retention",
                            "webhook": {
                                "name": "webhook_retention",
                                "retention": "432000s",
                            },
                        },
                        "endpoint": "Test",
                        "duration": "432000s",
                    }
                },
                "dataset": "Test",
                "dsVersion": 1,
                "cdc": "Upsert",
                "disorder": "1209600s",
            }
        ],
        "extdbs": [
            {
                "name": "webhook_retention",
                "webhook": {
                    "name": "webhook_retention",
                    "retention": "432000s",
                },
            }
        ],
        "offlineIndices": [
            {
                "dsName": "Test",
                "dsVersion": 1,
                "duration": {"forever": "forever"},
            }
        ],
        "onlineIndices": [
            {
                "dsName": "Test",
                "dsVersion": 1,
                "duration": {"forever": "forever"},
            }
        ],
    }
    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    sync_request.datasets[0].pycode.Clear()
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


def test_dataset_with_aggregates():
    @meta(owner="test@test.com")
    @dataset
    class UserAggregatesDataset:
        gender: str = field(key=True)
        timestamp: datetime = field(timestamp=True)
        count: int
        avg_age: float
        stddev_age: float

        @pipeline
        @inputs(UserInfoDataset)
        def create_aggregated_dataset(cls, user_info: Dataset):
            return user_info.groupby("gender").aggregate(
                [
                    Count(
                        window=Continuous("forever"),
                        into_field=str(cls.count),
                    ),
                    Average(
                        of="age",
                        window=Continuous("forever"),
                        into_field=str(cls.avg_age),
                    ),
                    Stddev(
                        of="age",
                        window=Continuous("forever"),
                        into_field=str(cls.stddev_age),
                    ),
                ]
            )

    view = InternalTestClient()
    view.add(UserAggregatesDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "UserAggregatesDataset",
        "metadata": {"owner": "test@test.com"},
        "version": 1,
        "dsschema": {
            "keys": {
                "fields": [{"name": "gender", "dtype": {"stringType": {}}}]
            },
            "values": {
                "fields": [
                    {"name": "count", "dtype": {"intType": {}}},
                    {"name": "avg_age", "dtype": {"doubleType": {}}},
                    {"name": "stddev_age", "dtype": {"doubleType": {}}},
                ]
            },
            "timestamp": "timestamp",
        },
        "history": "63072000s",
        "retention": "63072000s",
        "fieldMetadata": {
            "avg_age": {},
            "count": {},
            "gender": {},
            "stddev_age": {},
            "timestamp": {},
        },
        "pycode": {},
    }
    # Ignoring schema validation since they are bytes and not human readable
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


@source(webhook.endpoint("Activity"), disorder="14d", cdc="append")
@dataset(history="120d")
class Activity:
    user_id: int
    action_type: float
    amount: Optional[float]
    timestamp: datetime


def test_dataset_with_retention():
    assert Activity._history == timedelta(days=120)
    view = InternalTestClient()
    view.add(Activity)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "datasets": [
            {
                "name": "Activity",
                "metadata": {"owner": "ml-eng@fennel.ai"},
                "version": 1,
                "dsschema": {
                    "keys": {},
                    "values": {
                        "fields": [
                            {"name": "user_id", "dtype": {"intType": {}}},
                            {
                                "name": "action_type",
                                "dtype": {"doubleType": {}},
                            },
                            {
                                "name": "amount",
                                "dtype": {
                                    "optionalType": {"of": {"doubleType": {}}}
                                },
                            },
                        ]
                    },
                    "timestamp": "timestamp",
                },
                "history": "10368000s",
                "retention": "10368000s",
                "fieldMetadata": {
                    "action_type": {},
                    "user_id": {},
                    "timestamp": {},
                    "amount": {},
                },
                "pycode": {},
                "isSourceDataset": True,
            }
        ],
        "sources": [
            {
                "table": {
                    "endpoint": {
                        "db": {
                            "name": "fennel_webhook",
                            "webhook": {
                                "name": "fennel_webhook",
                                "retention": "2592000s",
                            },
                        },
                        "endpoint": "Activity",
                        "duration": "2592000s",
                    }
                },
                "dataset": "Activity",
                "dsVersion": 1,
                "disorder": "1209600s",
            }
        ],
        "extdbs": [
            {
                "name": "fennel_webhook",
                "webhook": {"name": "fennel_webhook", "retention": "2592000s"},
            }
        ],
    }

    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    sync_request.datasets[0].pycode.Clear()
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


class Manufacturer:
    name: str
    country: str
    timestamp: datetime


class Car:
    make: Manufacturer
    model: str
    year: int
    timestamp: datetime


@meta(owner="test@test.com")
@source(webhook.endpoint("DealerDataset"), disorder="14d", cdc="append")
@dataset
class Dealer:
    name: str
    address: str
    cars: List[Car]
    timestamp: datetime


def test_nested_dataset():
    view = InternalTestClient()
    view.add(Dealer)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "datasets": [
            {
                "name": "Dealer",
                "metadata": {"owner": "test@test.com"},
                "version": 1,
                "dsschema": {
                    "keys": {},
                    "values": {
                        "fields": [
                            {"name": "name", "dtype": {"stringType": {}}},
                            {"name": "address", "dtype": {"stringType": {}}},
                            {
                                "name": "cars",
                                "dtype": {
                                    "arrayType": {
                                        "of": {
                                            "structType": {
                                                "name": "Car",
                                                "fields": [
                                                    {
                                                        "name": "make",
                                                        "dtype": {
                                                            "structType": {
                                                                "name": "Manufacturer",
                                                                "fields": [
                                                                    {
                                                                        "name": "name",
                                                                        "dtype": {
                                                                            "stringType": {}
                                                                        },
                                                                    },
                                                                    {
                                                                        "name": "country",
                                                                        "dtype": {
                                                                            "stringType": {}
                                                                        },
                                                                    },
                                                                    {
                                                                        "name": "timestamp",
                                                                        "dtype": {
                                                                            "timestampType": {}
                                                                        },
                                                                    },
                                                                ],
                                                            }
                                                        },
                                                    },
                                                    {
                                                        "name": "model",
                                                        "dtype": {
                                                            "stringType": {}
                                                        },
                                                    },
                                                    {
                                                        "name": "year",
                                                        "dtype": {
                                                            "intType": {}
                                                        },
                                                    },
                                                    {
                                                        "name": "timestamp",
                                                        "dtype": {
                                                            "timestampType": {}
                                                        },
                                                    },
                                                ],
                                            }
                                        }
                                    }
                                },
                            },
                        ]
                    },
                    "timestamp": "timestamp",
                },
                "history": "63072000s",
                "retention": "63072000s",
                "fieldMetadata": {
                    "address": {},
                    "cars": {},
                    "timestamp": {},
                    "name": {},
                },
                "pycode": {},
                "isSourceDataset": True,
            }
        ],
        "sources": [
            {
                "table": {
                    "endpoint": {
                        "db": {
                            "name": "fennel_webhook",
                            "webhook": {
                                "name": "fennel_webhook",
                                "retention": "2592000s",
                            },
                        },
                        "endpoint": "DealerDataset",
                        "duration": "2592000s",
                    }
                },
                "dataset": "Dealer",
                "dsVersion": 1,
                "disorder": "1209600s",
            }
        ],
        "extdbs": [
            {
                "name": "fennel_webhook",
                "webhook": {"name": "fennel_webhook", "retention": "2592000s"},
            }
        ],
    }
    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    sync_request.datasets[0].pycode.Clear()
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


# TODO(mohit): Uncomment once support for ondemand funcs is added on protos
#
# def test_dataset_with_pull():
#     API_ENDPOINT_URL = "http://transunion.com/v1/credit_score"

#     @meta(owner="test@test.com")
#     @dataset(
#         history="1y",
#     )
#     class UserCreditScore:
#         user_id: int = field(key=True)
#         name: str = field(key=True)
#         credit_score: float
#         timestamp: datetime

#         @on_demand(expires_after="7d")
#         def pull_from_api(
#             cls, ts: pd.Series, user_id: pd.Series[int], names: pd.Series[str]
#         ) -> pd.DataFrame:
#             user_list = user_id.tolist()
#             names = names.tolist()
#             resp = requests.get(
#                 API_ENDPOINT_URL, json={"users": user_list, "names": names}
#             )
#             df = pd.DataFrame(columns=["user_id", "credit_score", "timestamp"])
#             if resp.status_code != 200:
#                 return df
#             results = resp.json()["results"]
#             df[str(cls.user_id)] = user_id
#             df[str(cls.name)] = names
#             df[str(cls.timestamp)] = ts
#             df[str(cls.credit_score)] = pd.Series(results)
#             return df, pd.Series([True] * len(df))

#     assert UserCreditScore._history == timedelta(days=365)
#     view = InternalTestClient()
#     view.add(UserCreditScore)
#     sync_request = view._get_sync_request_proto()
#     assert len(sync_request.datasets) == 1
#     d = {
#         "name": "UserCreditScore",
#         "fields": [
#             {
#                 "name": "user_id",
#                 "ftype": "Key",
#                 "dtype": {"scalarType": "INT"},
#                 "metadata": {},
#             },
#             {
#                 "name": "name",
#                 "ftype": "Key",
#                 "dtype": {"scalarType": "STRING"},
#                 "metadata": {},
#             },
#             {
#                 "name": "credit_score",
#                 "ftype": "Val",
#                 "dtype": {"scalarType": "FLOAT"},
#                 "metadata": {},
#             },
#             {
#                 "name": "timestamp",
#                 "ftype": "Timestamp",
#                 "dtype": {"scalarType": "TIMESTAMP"},
#                 "metadata": {},
#             },
#         ],
#         "mode": "pandas",
#         "metadata": {"owner": "test@test.com"},
#         "history": "31536000000000",
#         "onDemand": {"expiresAfter": "604800000000"},
#     }

#     # Ignoring schema validation since they are bytes and not human-readable
#     dataset_req = sync_request.datasets[0]
#     expected_ds_request = ParseDict(d, ds_proto.CoreDataset())
#     assert dataset_req == expected_ds_request, error_message(
#         dataset_req, expected_ds_request
#     )

#     with pytest.raises(TypeError) as e:

#         @meta(owner="test@test.com")
#         @dataset(history="1y")
#         class UserCreditScore2:
#             user_id: int = field(key=True)
#             credit_score: float
#             timestamp: datetime

#             @on_demand
#             def pull_from_api(
#                 cls, user_id: pd.Series, names: pd.Series, timestamps: pd.Series
#             ) -> pd.DataFrame:
#                 pass

#     assert (
#         str(e.value) == "on_demand must be defined with a parameter "
#         "expires_after of type Duration for eg: 30d."
#     )


def test_dataset_with_pipes():
    @meta(owner="test@test.com")
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @meta(owner="test@test.com")
    @dataset(index=True)
    class B:
        b1: int = field(key=True)
        t: datetime

    @meta(owner="test@test.com")
    @dataset
    class C:
        t: datetime

    def add_one(x: int):
        return x + 1

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDataset:
        a1: int = field(key=True)
        t: datetime

        @pipeline
        @includes(add_one)
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(b, how="left", left_on=["a1"], right_on=["b1"])

    view = InternalTestClient()
    view.add(ABCDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABCDataset",
        "version": 1,
        "dsschema": {
            "keys": {
                "fields": [{"name": "a1", "dtype": {"int_type": {}}}],
            },
            "values": {},
            "timestamp": "t",
        },
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {"a1": {}, "t": {}},
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # There is one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    expected_gen_code = """

def add_one(x: int):
    return x + 1


@pipeline
@includes(add_one)
@inputs(A, B)
def pipeline1(cls, a: Dataset, b: Dataset):
    return a.join(b, how="left", left_on=["a1"], right_on=["b1"])
"""
    assert expected_gen_code == pipeline_req.pycode.generated_code
    expected_source_code = """@pipeline
@includes(add_one)
@inputs(A, B)
def pipeline1(cls, a: Dataset, b: Dataset):
    return a.join(b, how="left", left_on=["a1"], right_on=["b1"])
"""
    assert expected_source_code == pipeline_req.pycode.source_code
    p = {
        "name": "pipeline1",
        "dataset_name": "ABCDataset",
        "signature": "pipeline1",
        "metadata": {},
        "input_dataset_names": ["A", "B"],
        "pycode": {},
        "dsVersion": 1,
    }
    pipeline_req.pycode.Clear()
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # There are 3 operators
    assert len(sync_request.operators) == 3
    operator_req = sync_request.operators[0]
    o = {
        "id": "B",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDataset",
        "dataset_ref": {
            "referring_dataset_name": "B",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[1]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDataset",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[2]
    o = {
        "id": "12a2088d8d7a0d265a7bd3f694fc81aa",
        "is_root": True,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDataset",
        "join": {
            "lhs_operand_id": "A",
            "rhs_dsref_operand_id": "B",
            "on": {"a1": "b1"},
            "how": 0,
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_dataset_with_pipes_bounds():
    @meta(owner="test@test.com")
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @meta(owner="test@test.com")
    @dataset(index=True)
    class B:
        b1: int = field(key=True)
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDatasetDefault:
        a1: int = field(key=True)
        t: datetime

        @pipeline
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(b, how="left", left_on=["a1"], right_on=["b1"])

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABDatasetLow:
        a1: int = field(key=True)
        t: datetime

        @pipeline
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(
                b,
                how="left",
                left_on=["a1"],
                right_on=["b1"],
                within=("1h", "0s"),
            )

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABDatasetHigh:
        a1: int = field(key=True)
        t: datetime

        @pipeline
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(
                b,
                how="left",
                left_on=["a1"],
                right_on=["b1"],
                within=("forever", "1d"),
            )

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABDataset:
        a1: int = field(key=True)
        t: datetime

        @pipeline
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(
                b,
                how="left",
                left_on=["a1"],
                right_on=["b1"],
                within=("3d", "1y"),
            )

    view = InternalTestClient()
    view.add(ABCDatasetDefault)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABCDatasetDefault",
        "dsschema": {
            "keys": {
                "fields": [{"name": "a1", "dtype": {"int_type": {}}}],
            },
            "values": {},
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {"a1": {}, "t": {}},
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # There is one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "signature": "pipeline1",
        "metadata": {},
        "input_dataset_names": ["A", "B"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # There are 3 operators
    assert len(sync_request.operators) == 3
    operator_req = sync_request.operators[0]
    o = {
        "id": "B",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "dataset_ref": {
            "referring_dataset_name": "B",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[1]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[2]
    o = {
        "id": "12a2088d8d7a0d265a7bd3f694fc81aa",
        "is_root": True,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "join": {
            "lhs_operand_id": "A",
            "rhs_dsref_operand_id": "B",
            "on": {"a1": "b1"},
            "how": 0,
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    # ----- ABCDatasetDefault -----

    view = InternalTestClient()
    view.add(ABCDatasetDefault)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABCDatasetDefault",
        "dsschema": {
            "keys": {
                "fields": [{"name": "a1", "dtype": {"int_type": {}}}],
            },
            "values": {},
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {"a1": {}, "t": {}},
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # There is one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "signature": "pipeline1",
        "metadata": {},
        "input_dataset_names": ["A", "B"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # There are 3 operators
    assert len(sync_request.operators) == 3
    operator_req = sync_request.operators[0]
    o = {
        "id": "B",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "dataset_ref": {
            "referring_dataset_name": "B",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[1]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[2]
    o = {
        "id": "12a2088d8d7a0d265a7bd3f694fc81aa",
        "is_root": True,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABCDatasetDefault",
        "join": {
            "lhs_operand_id": "A",
            "rhs_dsref_operand_id": "B",
            "on": {"a1": "b1"},
            "how": 0,
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    # ----- ABDatasetLow -----

    view = InternalTestClient()
    view.add(ABDatasetLow)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABDatasetLow",
        "dsschema": {
            "keys": {
                "fields": [{"name": "a1", "dtype": {"int_type": {}}}],
            },
            "values": {},
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {"a1": {}, "t": {}},
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # There is one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline1",
        "dataset_name": "ABDatasetLow",
        "signature": "pipeline1",
        "metadata": {},
        "input_dataset_names": ["A", "B"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # There are 3 operators
    assert len(sync_request.operators) == 3
    operator_req = sync_request.operators[0]
    o = {
        "id": "B",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDatasetLow",
        "dataset_ref": {
            "referring_dataset_name": "B",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[1]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDatasetLow",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[2]
    o = {
        "id": "12a2088d8d7a0d265a7bd3f694fc81aa",
        "is_root": True,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDatasetLow",
        "join": {
            "lhs_operand_id": "A",
            "rhs_dsref_operand_id": "B",
            "on": {"a1": "b1"},
            "within_low": "3600s",
            "how": 0,
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    # ----- ABDatasetHigh -----

    view = InternalTestClient()
    view.add(ABDatasetHigh)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABDatasetHigh",
        "dsschema": {
            "keys": {
                "fields": [{"name": "a1", "dtype": {"int_type": {}}}],
            },
            "values": {},
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {"a1": {}, "t": {}},
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # There is one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline1",
        "dataset_name": "ABDatasetHigh",
        "signature": "pipeline1",
        "metadata": {},
        "input_dataset_names": ["A", "B"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # There are 3 operators
    assert len(sync_request.operators) == 3
    operator_req = sync_request.operators[0]
    o = {
        "id": "B",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDatasetHigh",
        "dataset_ref": {
            "referring_dataset_name": "B",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[1]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDatasetHigh",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[2]
    o = {
        "id": "12a2088d8d7a0d265a7bd3f694fc81aa",
        "is_root": True,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDatasetHigh",
        "join": {
            "lhs_operand_id": "A",
            "rhs_dsref_operand_id": "B",
            "on": {"a1": "b1"},
            "how": 0,
            "within_high": "86400s",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    # ----- ABDataset -----

    view = InternalTestClient()
    view.add(ABDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABDataset",
        "dsschema": {
            "keys": {
                "fields": [{"name": "a1", "dtype": {"int_type": {}}}],
            },
            "values": {},
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {"a1": {}, "t": {}},
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # There is one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline1",
        "dataset_name": "ABDataset",
        "signature": "pipeline1",
        "metadata": {},
        "input_dataset_names": ["A", "B"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # There are 3 operators
    assert len(sync_request.operators) == 3
    operator_req = sync_request.operators[0]
    o = {
        "id": "B",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDataset",
        "dataset_ref": {
            "referring_dataset_name": "B",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[1]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDataset",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )
    operator_req = sync_request.operators[2]
    o = {
        "id": "12a2088d8d7a0d265a7bd3f694fc81aa",
        "is_root": True,
        "pipeline_name": "pipeline1",
        "dataset_name": "ABDataset",
        "join": {
            "lhs_operand_id": "A",
            "rhs_dsref_operand_id": "B",
            "on": {"a1": "b1"},
            "how": 0,
            "within_low": "259200s",
            "within_high": "31536000s",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_dataset_with_complex_pipe():
    @meta(owner="test@test.com")
    @dataset
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int
        num_merchant_fraudulent_transactions_7d: int
        median_transaction_amount: float

        @pipeline
        @inputs(Activity, UserInfoDataset)
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
                lambda df: df["action_type"] == "report_txn"
            )
            ds = filtered_ds.join(
                user_info,
                how="left",
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
            ds_deduped = ds_transform.dedup(by=["user_id", "merchant_id"])
            return ds_deduped.groupby("merchant_id").aggregate(
                [
                    Count(
                        window=Continuous("forever"),
                        into_field=str(
                            cls.num_merchant_fraudulent_transactions
                        ),
                    ),
                    Count(
                        window=Continuous("1w"),
                        into_field=str(
                            cls.num_merchant_fraudulent_transactions_7d
                        ),
                    ),
                    Quantile(
                        of="transaction_amount",
                        default=0.0,
                        p=0.5,
                        window=Continuous("1w"),
                        into_field=str(cls.median_transaction_amount),
                        approx=True,
                    ),
                ]
            )

    view = InternalTestClient()
    view.add(FraudReportAggregatedDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "FraudReportAggregatedDataset",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "merchant_id",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
                    {
                        "name": "num_merchant_fraudulent_transactions",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions_7d",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "median_transaction_amount",
                        "dtype": {"double_type": {}},
                    },
                ]
            },
            "timestamp": "timestamp",
        },
        "version": 1,
        "metadata": {"owner": "test@test.com"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "merchant_id": {},
            "num_merchant_fraudulent_transactions": {},
            "num_merchant_fraudulent_transactions_7d": {},
            "timestamp": {},
            "median_transaction_amount": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # Only one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "create_fraud_dataset",
        "dataset_name": "FraudReportAggregatedDataset",
        "signature": "create_fraud_dataset",
        "metadata": {},
        "input_dataset_names": ["Activity", "UserInfoDataset"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # 7 operators
    assert len(sync_request.operators) == 7
    operator_req = sync_request.operators[0]
    o = {
        "id": "UserInfoDataset",
        "is_root": False,
        "pipeline_name": "create_fraud_dataset",
        "dataset_name": "FraudReportAggregatedDataset",
        "dataset_ref": {
            "referring_dataset_name": "UserInfoDataset",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[1]
    o = {
        "id": "Activity",
        "is_root": False,
        "pipeline_name": "create_fraud_dataset",
        "dataset_name": "FraudReportAggregatedDataset",
        "dataset_ref": {
            "referring_dataset_name": "Activity",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[2])
    o = {
        "id": "101097826c6986ddb25ce924985d9217",
        "pipelineName": "create_fraud_dataset",
        "datasetName": "FraudReportAggregatedDataset",
        "filter": {"operandId": "Activity", "pycode": {}},
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[3]
    o = {
        "id": "4202e94cf2e47bf5bcc94fd57aee8d0f",
        "pipelineName": "create_fraud_dataset",
        "datasetName": "FraudReportAggregatedDataset",
        "join": {
            "lhsOperandId": "101097826c6986ddb25ce924985d9217",
            "rhsDsrefOperandId": "UserInfoDataset",
            "on": {"user_id": "user_id"},
            "how": 0,
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[4])
    o = {
        "id": "bfa10d216f843625785d24e6b9d890fb",
        "pipelineName": "create_fraud_dataset",
        "datasetName": "FraudReportAggregatedDataset",
        "transform": {
            "operandId": "4202e94cf2e47bf5bcc94fd57aee8d0f",
            "schema": {
                "user_id": {"intType": {}},
                "merchant_id": {"intType": {}},
                "timestamp": {"timestampType": {}},
                "transaction_amount": {"doubleType": {}},
            },
            "pycode": {},
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[5]
    o = {
        "id": "acd519ba5789e767383099d0561e07c8",
        "pipelineName": "create_fraud_dataset",
        "datasetName": "FraudReportAggregatedDataset",
        "dedup": {
            "operandId": "bfa10d216f843625785d24e6b9d890fb",
            "columns": ["user_id", "merchant_id"],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[6]
    o = {
        "id": "0b381d6b2444c390000402aaa4485a26",
        "isRoot": True,
        "pipelineName": "create_fraud_dataset",
        "datasetName": "FraudReportAggregatedDataset",
        "aggregate": {
            "operandId": "acd519ba5789e767383099d0561e07c8",
            "keys": ["merchant_id"],
            "specs": [
                {
                    "count": {
                        "name": "num_merchant_fraudulent_transactions",
                        "window": {"forever": {}},
                    }
                },
                {
                    "count": {
                        "name": "num_merchant_fraudulent_transactions_7d",
                        "window": {"sliding": {"duration": "604800s"}},
                    }
                },
                {
                    "quantile": {
                        "name": "median_transaction_amount",
                        "window": {"sliding": {"duration": "604800s"}},
                        "quantile": 0.5,
                        "default": 0.0,
                        "of": "transaction_amount",
                        "approx": True,
                    }
                },
            ],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_assign_column():
    @meta(owner="test@test.com")
    @dataset
    class A:
        a1: int = field(key=True)
        a2: int
        a3: str
        a4: float
        t: datetime

    @meta(owner="thaqib@fennel.ai")
    @dataset
    class B:
        a1: int = field(key=True)
        a2: str
        t: datetime

        @pipeline
        @inputs(A)
        def from_a(cls, a: Dataset):
            x = a.assign("a2", str, lambda df: df["a3"])
            return x.select("a1", "a2")

    view = InternalTestClient()
    view.add(B)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "B",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "a1",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
                    {
                        "name": "a2",
                        "dtype": {"string_type": {}},
                    }
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "thaqib@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "a1": {},
            "a2": {},
            "t": {},
        },
        "pycode": {},
    }

    dataset_req = sync_request.datasets[0]

    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[1])
    o = {
        "id": "666ab6a479657032e71942594b9d0fb4",
        "pipeline_name": "from_a",
        "dataset_name": "B",
        "assign": {
            "columnName": "a2",
            "operandId": "A",
            "outputType": {"stringType": {}},
            "pycode": {},
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_dropnull():
    @meta(owner="test@test.com")
    @dataset
    class A:
        a1: int = field(key=True)
        a2: Optional[int]
        a3: str
        a4: Optional[float]
        t: datetime

    @meta(owner="test@test.com")
    @dataset
    class B:
        a1: int = field(key=True)
        a2: int
        a3: str
        a4: float
        t: datetime

        @pipeline
        @inputs(A)
        def from_a(cls, a: Dataset):
            x = a.dropnull("a2", "a4")
            return x

    view = InternalTestClient()
    view.add(B)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "B",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "a1",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
                    {
                        "name": "a2",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "a3",
                        "dtype": {"string_type": {}},
                    },
                    {
                        "name": "a4",
                        "dtype": {"double_type": {}},
                    },
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "test@test.com"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "a1": {},
            "a2": {},
            "a3": {},
            "a4": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "from_a",
        "dataset_name": "B",
        "signature": "from_a",
        "metadata": {},
        "input_dataset_names": ["A"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    assert len(sync_request.operators) == 2
    o = {
        "id": "A",
        "pipeline_name": "from_a",
        "dataset_name": "B",
        "dataset_ref": {"referring_dataset_name": "A"},
        "ds_version": 1,
    }
    operator_req = sync_request.operators[0]
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    o = {
        "id": "be210d6a1d0ed8ab544d737b629a1c8d",
        "isRoot": True,
        "pipelineName": "from_a",
        "datasetName": "B",
        "dropnull": {"operandId": "A", "columns": ["a2", "a4"]},
        "ds_version": 1,
    }

    operator_req = sync_request.operators[1]
    expected_operator_request = ParseDict(o, ds_proto.Operator())

    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_select_and_rename_column():
    @meta(owner="test@test.com")
    @dataset
    class A:
        a1: int = field(key=True)
        a2: int
        a3: str
        a4: float
        t: datetime

    @meta(owner="thaqib@fennel.ai")
    @dataset
    class B:
        b1: int = field(key=True)
        a2: int
        t: datetime

        @pipeline
        @inputs(A)
        def from_a(cls, a: Dataset):
            x = a.rename({"a1": "b1"})
            return x.select("b1", "a2")

    view = InternalTestClient()
    view.add(B)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "B",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "b1",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
                    {
                        "name": "a2",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "thaqib@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "b1": {},
            "a2": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "from_a",
        "dataset_name": "B",
        "signature": "from_a",
        "metadata": {},
        "input_dataset_names": ["A"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    assert len(sync_request.operators) == 3
    o = {
        "id": "A",
        "pipeline_name": "from_a",
        "dataset_name": "B",
        "dataset_ref": {"referring_dataset_name": "A"},
        "ds_version": 1,
    }
    operator_req = sync_request.operators[0]
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    o = {
        "datasetName": "B",
        "id": "10340ca369826992acc29dc84b073c18",
        "pipelineName": "from_a",
        "rename": {"columnMap": {"a1": "b1"}, "operandId": "A"},
        "ds_version": 1,
    }

    operator_req = sync_request.operators[1]
    expected_operator_request = ParseDict(o, ds_proto.Operator())

    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    o = {
        "datasetName": "B",
        "drop": {
            "dropcols": ["a3", "a4"],
            "operandId": "10340ca369826992acc29dc84b073c18",
        },
        "id": "0d52839b6fb94cde94dea24334ad9bce",
        "isRoot": True,
        "pipelineName": "from_a",
        "ds_version": 1,
    }

    operator_req = sync_request.operators[2]
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_union_datasets():
    @dataset
    class A:
        a1: int
        t: datetime

    @dataset
    class B:
        b1: int
        t: datetime

    @meta(owner="test@test.com")
    @dataset
    class ABCDataset:
        a1: int
        t: datetime

        @pipeline
        @inputs(A)
        def pipeline2_diamond(cls, a: Dataset):
            b = a.transform(lambda df: df)
            c = a.transform(lambda df: df * 2)
            d = b + c
            e = d.transform(lambda df: df * 3)
            f = d.transform(lambda df: df * 4)
            return e + f

    view = InternalTestClient()
    view.add(ABCDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "ABCDataset",
        "dsschema": {
            "keys": {},
            "values": {
                "fields": [
                    {
                        "name": "a1",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "test@test.com"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "a1": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "signature": "pipeline2_diamond",
        "metadata": {},
        "input_dataset_names": ["A"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    assert len(sync_request.operators) == 7
    operator_req = sync_request.operators[0]
    o = {
        "id": "A",
        "is_root": False,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "dataset_ref": {
            "referring_dataset_name": "A",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[1])
    o = {
        "id": "c11a7a6052cdbb1759969dd10613ac8b",
        "is_root": False,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "transform": {"operand_id": "A", "schema": {}, "pycode": {}},
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[2])
    o = {
        "id": "b1f19f0df67793dfec442938232b07c4",
        "is_root": False,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "transform": {"operand_id": "A", "schema": {}, "pycode": {}},
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[3]
    o = {
        "id": "3e00aad7fe8a3f2c35b3abeb42540705",
        "is_root": False,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "union": {
            "operand_ids": [
                "c11a7a6052cdbb1759969dd10613ac8b",
                "b1f19f0df67793dfec442938232b07c4",
            ],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[4])
    o = {
        "id": "bbea68029557c9f85a5f7fa8f92d632b",
        "is_root": False,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "transform": {
            "operand_id": "3e00aad7fe8a3f2c35b3abeb42540705",
            "schema": {},
            "pycode": {},
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[5])
    o = {
        "id": "95a98aebceb48a64d9b2a8a7001d10df",
        "is_root": False,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "transform": {
            "operand_id": "3e00aad7fe8a3f2c35b3abeb42540705",
            "schema": {},
            "pycode": {},
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[6]
    o = {
        "id": "281385065b983e434ee8dd13c934cc08",
        "is_root": True,
        "pipeline_name": "pipeline2_diamond",
        "dataset_name": "ABCDataset",
        "union": {
            "operand_ids": [
                "bbea68029557c9f85a5f7fa8f92d632b",
                "95a98aebceb48a64d9b2a8a7001d10df",
            ],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_first_operator():
    @meta(owner="abhay@fennel.ai")
    @dataset
    class RatingActivity:
        userid: int
        movie: str
        rating: float
        t: datetime

    @meta(owner="abhay@fennel.ai")
    @dataset
    class FirstMovieSeen:
        userid: int = field(key=True)
        rating: float
        movie: str
        t: datetime

        @pipeline
        @inputs(RatingActivity)
        def pipeline_first_movie_seen(cls, rating: Dataset):
            return rating.groupby("userid").first()

    view = InternalTestClient()
    view.add(FirstMovieSeen)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "FirstMovieSeen",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "userid",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
                    {
                        "name": "rating",
                        "dtype": {"double_type": {}},
                    },
                    {
                        "name": "movie",
                        "dtype": {"string_type": {}},
                    },
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "abhay@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "userid": {},
            "movie": {},
            "rating": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # Only one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline_first_movie_seen",
        "dataset_name": "FirstMovieSeen",
        "signature": "pipeline_first_movie_seen",
        "metadata": {},
        "input_dataset_names": ["RatingActivity"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # 1 operators
    assert len(sync_request.operators) == 2
    operator_req = sync_request.operators[0]
    o = {
        "id": "RatingActivity",
        "is_root": False,
        "pipeline_name": "pipeline_first_movie_seen",
        "dataset_name": "FirstMovieSeen",
        "dataset_ref": {
            "referring_dataset_name": "RatingActivity",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[1]
    o = {
        "id": "883e720f07ee6c1ecf924ca31416b8c3",
        "is_root": True,
        "pipelineName": "pipeline_first_movie_seen",
        "datasetName": "FirstMovieSeen",
        "first": {"operandId": "RatingActivity", "by": ["userid"]},
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_last_operator():
    @meta(owner="aditya@fennel.ai")
    @dataset
    class RatingActivity:
        userid: int
        movie: str
        rating: float
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class LastMovieSeen:
        userid: int = field(key=True)
        rating: float
        movie: str
        t: datetime

        @pipeline
        @inputs(RatingActivity)
        def pipeline_last_movie_seen(cls, rating: Dataset):
            return rating.groupby("userid").latest()

    view = InternalTestClient()
    view.add(LastMovieSeen)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "LastMovieSeen",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "userid",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
                    {
                        "name": "rating",
                        "dtype": {"double_type": {}},
                    },
                    {
                        "name": "movie",
                        "dtype": {"string_type": {}},
                    },
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "userid": {},
            "movie": {},
            "rating": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # Only one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline_last_movie_seen",
        "dataset_name": "LastMovieSeen",
        "signature": "pipeline_last_movie_seen",
        "metadata": {},
        "input_dataset_names": ["RatingActivity"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # 1 operators
    assert len(sync_request.operators) == 2
    operator_req = sync_request.operators[0]
    o = {
        "id": "RatingActivity",
        "is_root": False,
        "pipeline_name": "pipeline_last_movie_seen",
        "dataset_name": "LastMovieSeen",
        "dataset_ref": {
            "referring_dataset_name": "RatingActivity",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[1]
    o = {
        "id": "883e720f07ee6c1ecf924ca31416b8c3",
        "is_root": True,
        "pipelineName": "pipeline_last_movie_seen",
        "datasetName": "LastMovieSeen",
        "latest": {"operandId": "RatingActivity", "by": ["userid"]},
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


@meta(owner="e2@company.com")
@dataset
class Document:
    doc_id: int
    body: str
    title: str
    owner: str
    origin: str
    creation_timestamp: datetime


def get_content_features(df: pd.DataFrame) -> pd.DataFrame:
    pass


def test_search_dataset():
    @meta(owner="aditya@fennel.ai")
    @dataset
    class DocumentContentDataset:
        doc_id: int
        bert_embedding: Embedding[128]
        fast_text_embedding: Embedding[256]
        num_words: int
        num_stop_words: int
        top_10_unique_words: List[str]
        creation_timestamp: datetime

        @pipeline
        @inputs(Document)
        def content_features(cls, ds: Dataset):
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

    @meta(owner="abhay@fennel.ai")
    @dataset
    class DocumentWordDataset:
        doc_id: int
        bert_embedding: Embedding[128]
        fast_text_embedding: Embedding[256]
        num_words: int
        num_stop_words: int
        top_10_unique_words: str
        creation_timestamp: datetime

        @pipeline
        @inputs(DocumentContentDataset)
        def unique_words(cls, ds: Dataset):
            schema = ds.schema()
            schema["top_10_unique_words"] = str
            return ds.explode(columns=["top_10_unique_words"]).transform(
                lambda df: df, schema
            )

    view = InternalTestClient()
    view.add(DocumentContentDataset)  # type: ignore
    view.add(DocumentWordDataset)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 2

    content_dataset_req = sync_request.datasets[0]
    d = {
        "name": "DocumentContentDataset",
        "dsschema": {
            "keys": {"fields": []},
            "values": {
                "fields": [
                    {
                        "name": "doc_id",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "bert_embedding",
                        "dtype": {"embedding_type": {"embedding_size": 128}},
                    },
                    {
                        "name": "fast_text_embedding",
                        "dtype": {"embedding_type": {"embedding_size": 256}},
                    },
                    {
                        "name": "num_words",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "num_stop_words",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "top_10_unique_words",
                        "dtype": {"array_type": {"of": {"string_type": {}}}},
                    },
                ]
            },
            "timestamp": "creation_timestamp",
        },
        "version": 1,
        "metadata": {"owner": "aditya@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "doc_id": {},
            "bert_embedding": {},
            "fast_text_embedding": {},
            "num_words": {},
            "num_stop_words": {},
            "top_10_unique_words": {},
            "creation_timestamp": {},
        },
        "pycode": {},
    }
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    content_dataset_req.pycode.Clear()
    assert content_dataset_req == expected_dataset_request, error_message(
        content_dataset_req, expected_dataset_request
    )

    word_dataset_req = sync_request.datasets[1]
    d = {
        "name": "DocumentWordDataset",
        "dsschema": {
            "keys": {"fields": []},
            "values": {
                "fields": [
                    {
                        "name": "doc_id",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "bert_embedding",
                        "dtype": {"embedding_type": {"embedding_size": 128}},
                    },
                    {
                        "name": "fast_text_embedding",
                        "dtype": {"embedding_type": {"embedding_size": 256}},
                    },
                    {
                        "name": "num_words",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "num_stop_words",
                        "dtype": {"int_type": {}},
                    },
                    {
                        "name": "top_10_unique_words",
                        "dtype": {"string_type": {}},
                    },
                ]
            },
            "timestamp": "creation_timestamp",
        },
        "version": 1,
        "metadata": {"owner": "abhay@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "doc_id": {},
            "bert_embedding": {},
            "fast_text_embedding": {},
            "num_words": {},
            "num_stop_words": {},
            "top_10_unique_words": {},
            "creation_timestamp": {},
        },
        "pycode": {},
    }
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    word_dataset_req.pycode.Clear()
    assert word_dataset_req == expected_dataset_request, error_message(
        word_dataset_req, expected_dataset_request
    )

    # pipelines
    assert len(sync_request.pipelines) == 2

    content_pipeline_req = sync_request.pipelines[0]
    content_pipeline_req.pycode.Clear()
    p = {
        "name": "content_features",
        "dataset_name": "DocumentContentDataset",
        "signature": "content_features",
        "metadata": {},
        "input_dataset_names": ["Document"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert content_pipeline_req == expected_pipeline_request, error_message(
        content_pipeline_req, expected_pipeline_request
    )

    word_pipeline_req = sync_request.pipelines[1]
    word_pipeline_req.pycode.Clear()
    p = {
        "name": "unique_words",
        "dataset_name": "DocumentWordDataset",
        "signature": "unique_words",
        "metadata": {},
        "input_dataset_names": ["DocumentContentDataset"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert word_pipeline_req == expected_pipeline_request, error_message(
        word_pipeline_req, expected_pipeline_request
    )

    # operators
    assert len(sync_request.operators) == 5
    operator_req = sync_request.operators[0]
    o = {
        "id": "Document",
        "is_root": False,
        "pipeline_name": "content_features",
        "dataset_name": "DocumentContentDataset",
        "dataset_ref": {
            "referring_dataset_name": "Document",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[1])
    o = {
        "id": "ad1a76eb67071a839ac8ec856e90c97c",
        "is_root": True,
        "pipeline_name": "content_features",
        "dataset_name": "DocumentContentDataset",
        "transform": {
            "operand_id": "Document",
            "schema": {
                "num_words": {"int_type": {}},
                "num_stop_words": {"int_type": {}},
                "creation_timestamp": {"timestamp_type": {}},
                "fast_text_embedding": {
                    "embedding_type": {"embedding_size": 256}
                },
                "bert_embedding": {"embedding_type": {"embedding_size": 128}},
                "doc_id": {"int_type": {}},
                "top_10_unique_words": {
                    "array_type": {"of": {"string_type": {}}}
                },
            },
            "pycode": {},
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[2]
    o = {
        "id": "DocumentContentDataset",
        "is_root": False,
        "pipeline_name": "unique_words",
        "dataset_name": "DocumentWordDataset",
        "dataset_ref": {
            "referring_dataset_name": "DocumentContentDataset",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[3]
    o = {
        "id": "961d4107a251ef6ef344d2b09f1a03f3",
        "is_root": False,
        "pipeline_name": "unique_words",
        "dataset_name": "DocumentWordDataset",
        "explode": {
            "operand_id": "DocumentContentDataset",
            "columns": ["top_10_unique_words"],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = erase_operator_pycode(sync_request.operators[4])
    o = {
        "id": "197b05deae8b6acd0b86b8153f636634",
        "is_root": True,
        "pipeline_name": "unique_words",
        "dataset_name": "DocumentWordDataset",
        "transform": {
            "operand_id": "961d4107a251ef6ef344d2b09f1a03f3",
            "schema": {
                "num_words": {"int_type": {}},
                "num_stop_words": {"int_type": {}},
                "creation_timestamp": {"timestamp_type": {}},
                "fast_text_embedding": {
                    "embedding_type": {"embedding_size": 256}
                },
                "bert_embedding": {"embedding_type": {"embedding_size": 128}},
                "doc_id": {"int_type": {}},
                "top_10_unique_words": {"string_type": {}},
            },
            "pycode": {},
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_auto_schema_generation():
    @meta(owner="data-eng-oncall@fennel.ai")
    @dataset
    class FraudActivityDataset:
        timestamp: datetime
        user_id: int
        merchant_id: int
        transaction_amount: float

        @pipeline
        @inputs(Activity)
        def create_fraud_dataset(cls, activity: Dataset):
            def extract_info(df: pd.DataFrame) -> pd.DataFrame:
                df_json = df["metadata"].apply(json.loads).apply(pd.Series)
                df = pd.concat([df_json, df[["user_id", "timestamp"]]], axis=1)
                df["transaction_amount"] = df["transaction_amount"] / 100
                return df[
                    [
                        "merchant_id",
                        "transaction_amount",
                        "user_id",
                        "timestamp",
                    ]
                ]

            assert activity.schema() == {
                "action_type": pd.Float64Dtype,
                "timestamp": datetime,
                "amount": Optional[pd.Float64Dtype],
                "user_id": pd.Int64Dtype,
            }

            filtered_ds = activity.filter(
                lambda df: df["action_type"] == "report"
            )

            assert filtered_ds.schema() == {
                "action_type": pd.Float64Dtype,
                "amount": Optional[pd.Float64Dtype],
                "timestamp": datetime,
                "user_id": pd.Int64Dtype,
            }

            x = filtered_ds.transform(
                extract_info,
                schema={
                    "transaction_amount": pd.Float64Dtype,
                    "merchant_id": pd.Int64Dtype,
                    "user_id": pd.Int64Dtype,
                    "timestamp": datetime,
                },
            )

            assert x.schema() == {
                "merchant_id": pd.Int64Dtype,
                "transaction_amount": pd.Float64Dtype,
                "user_id": pd.Int64Dtype,
                "timestamp": datetime,
            }

            assign_ds = activity.assign(
                "transaction_amount", int, lambda df: df["user_id"] * 2
            )
            assert assign_ds.schema() == {
                "action_type": pd.Float64Dtype,
                "timestamp": datetime,
                "transaction_amount": pd.Int64Dtype,
                "user_id": pd.Int64Dtype,
                "amount": Optional[pd.Float64Dtype],
            }

            assign_ds_str = activity.assign(
                "user_id", str, lambda df: str(df["user_id"])
            )
            assert assign_ds_str.schema() == {
                "action_type": pd.Float64Dtype,
                "timestamp": datetime,
                "user_id": pd.StringDtype,
                "amount": Optional[pd.Float64Dtype],
            }

            return x


def test_pipeline_with_env_selector():
    kafka = Kafka.get(name="my_kafka")

    @meta(owner="test@test.com")
    @source(kafka.topic("orders"), disorder="1h", cdc="upsert")
    @dataset
    class A:
        a1: int = field(key=True)
        t: datetime

    @meta(owner="test@test.com")
    @source(kafka.topic("orders2"), disorder="1h", cdc="upsert")
    @dataset(index=True)
    class B:
        b1: int = field(key=True)
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDatasetDefault:
        a1: int = field(key=True)
        t: datetime

        @pipeline(env="prod")
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(b, how="left", left_on=["a1"], right_on=["b1"])

        @pipeline(env="staging")
        @inputs(A, B)
        def pipeline2(cls, a: Dataset, b: Dataset):
            return a.join(b, how="inner", left_on=["a1"], right_on=["b1"])

        @pipeline(env="staging")
        @inputs(A, B)
        def pipeline3(cls, a: Dataset, b: Dataset):
            return a.join(b, how="inner", left_on=["a1"], right_on=["b1"])

    view = InternalTestClient()
    view.add(A)  # type: ignore
    view.add(B)  # type: ignore
    view.add(ABCDatasetDefault)  # type: ignore
    with pytest.raises(ValueError) as e:
        _ = view._get_sync_request_proto()
    assert (
        str(e.value)
        == "Pipeline : `pipeline3` mapped to Tier : staging which has more than one pipeline. Please specify only one."
    )

    with pytest.raises(ValueError) as e:
        _ = view._get_sync_request_proto(env=["prod"])
    assert str(e.value) == "Expected env to be a string, got ['prod']"
    sync_request = view._get_sync_request_proto(env="prod")
    pipelines = sync_request.pipelines
    assert len(pipelines) == 1


def test_dataset_with_str_window_aggregate():
    @meta(owner="test@test.com")
    @dataset
    class UserAggregatesDataset:
        gender: str = field(key=True)
        timestamp: datetime = field(timestamp=True)
        count: int
        sum_age: int
        stddev_age: float

        @pipeline
        @inputs(UserInfoDataset)
        def create_aggregated_dataset(cls, user_info: Dataset):
            return user_info.groupby("gender").aggregate(
                Count(window=Continuous("forever"), into_field="count"),
                Sum(
                    of="age",
                    window=Continuous("forever"),
                    into_field="sum_age",
                ),
                Stddev(
                    of="age",
                    window=Continuous("forever"),
                    into_field="stddev_age",
                ),
            )

    view = InternalTestClient()
    view.add(UserAggregatesDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "UserAggregatesDataset",
        "metadata": {"owner": "test@test.com"},
        "dsschema": {
            "keys": {
                "fields": [{"name": "gender", "dtype": {"stringType": {}}}]
            },
            "values": {
                "fields": [
                    {"name": "count", "dtype": {"intType": {}}},
                    {"name": "sum_age", "dtype": {"intType": {}}},
                    {"name": "stddev_age", "dtype": {"doubleType": {}}},
                ]
            },
            "timestamp": "timestamp",
        },
        "version": 1,
        "history": "63072000s",
        "retention": "63072000s",
        "fieldMetadata": {
            "sum_age": {},
            "stddev_age": {},
            "count": {},
            "gender": {},
            "timestamp": {},
        },
        "pycode": {},
    }
    # Ignoring schema validation since they are bytes and not human readable
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


def test_window_operator():
    @meta(owner="nitin@fennel.ai")
    @dataset
    class PageViewEvent:
        user_id: str
        page_id: str
        t: datetime

    @meta(owner="nitin@fennel.ai")
    @dataset
    class Sessions:
        user_id: str = field(key=True)
        window: Window = field(key=True)
        t: datetime

        @pipeline
        @inputs(PageViewEvent)
        def pipeline_window(cls, app_event: Dataset):
            return app_event.groupby(
                "user_id", window=Session("10m")
            ).aggregate()

    view = InternalTestClient()
    view.add(Sessions)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "Sessions",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"stringType": {}},
                    },
                    {
                        "name": "window",
                        "dtype": {
                            "structType": {
                                "name": "Window",
                                "fields": [
                                    {
                                        "name": "begin",
                                        "dtype": {"timestampType": {}},
                                    },
                                    {
                                        "name": "end",
                                        "dtype": {"timestampType": {}},
                                    },
                                ],
                            }
                        },
                    },
                ]
            },
            "values": {},
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "nitin@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "user_id": {},
            "window": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # Only one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline_window",
        "dataset_name": "Sessions",
        "signature": "pipeline_window",
        "metadata": {},
        "input_dataset_names": ["PageViewEvent"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # 1 operators
    assert len(sync_request.operators) == 2
    operator_req = sync_request.operators[0]
    o = {
        "id": "PageViewEvent",
        "is_root": False,
        "pipeline_name": "pipeline_window",
        "dataset_name": "Sessions",
        "dataset_ref": {
            "referring_dataset_name": "PageViewEvent",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[1]
    o = {
        "id": "a4f0c87fb58dbccaaa2a115386ea5762",
        "is_root": True,
        "pipelineName": "pipeline_window",
        "datasetName": "Sessions",
        "window": {
            "field": "window",
            "windowType": {"session": {"gap": "600s"}},
            "operandId": "PageViewEvent",
            "by": ["user_id"],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_window_operator_with_aggregation():
    @meta(owner="nitin@fennel.ai")
    @dataset
    class PageViewEvent:
        user_id: str
        page_id: str
        t: datetime

    @meta(owner="nitin@fennel.ai")
    @dataset
    class Sessions:
        user_id: str = field(key=True)
        avg_session_secs: float
        t: datetime

        @pipeline
        @inputs(PageViewEvent)
        def pipeline_window(cls, app_event: Dataset):
            sessions = (
                app_event.groupby("user_id", window=Session("10m"))
                .aggregate()
                .assign(
                    "duration_secs",
                    int,
                    lambda df: (df["window"].end - df["window"].begin).secs(),
                )
                .groupby("user_id")
                .aggregate(
                    Average(
                        of="duration_secs",
                        window=Continuous("forever"),
                        into_field="avg_session_secs",
                    )
                )
            )
            return sessions

    view = InternalTestClient()
    view.add(Sessions)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "Sessions",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"stringType": {}},
                    },
                ]
            },
            "values": {
                "fields": [
                    {"name": "avg_session_secs", "dtype": {"double_type": {}}}
                ]
            },
            "timestamp": "t",
        },
        "version": 1,
        "metadata": {"owner": "nitin@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "user_id": {},
            "avg_session_secs": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )

    # Only one pipeline
    assert len(sync_request.pipelines) == 1
    pipeline_req = sync_request.pipelines[0]
    pipeline_req.pycode.Clear()
    p = {
        "name": "pipeline_window",
        "dataset_name": "Sessions",
        "signature": "pipeline_window",
        "metadata": {},
        "input_dataset_names": ["PageViewEvent"],
        "pycode": {},
        "ds_version": 1,
    }
    expected_pipeline_request = ParseDict(p, ds_proto.Pipeline())
    assert pipeline_req == expected_pipeline_request, error_message(
        pipeline_req, expected_pipeline_request
    )

    # 1 operators
    assert len(sync_request.operators) == 4
    operator_req = sync_request.operators[0]
    o = {
        "id": "PageViewEvent",
        "is_root": False,
        "pipeline_name": "pipeline_window",
        "dataset_name": "Sessions",
        "dataset_ref": {
            "referring_dataset_name": "PageViewEvent",
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[1]
    o = {
        "id": "a4f0c87fb58dbccaaa2a115386ea5762",
        "is_root": False,
        "pipelineName": "pipeline_window",
        "datasetName": "Sessions",
        "window": {
            "field": "window",
            "windowType": {"session": {"gap": "600s"}},
            "operandId": "PageViewEvent",
            "by": ["user_id"],
        },
        "ds_version": 1,
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[2]
    o = {
        "id": "9a3812227a7dbdee819d143c82e23c05",
        "is_root": False,
        "pipelineName": "pipeline_window",
        "datasetName": "Sessions",
        "assign": {
            "columnName": "duration_secs",
            "operandId": "a4f0c87fb58dbccaaa2a115386ea5762",
            "outputType": {"intType": {}},
            "pycode": {},
        },
        "ds_version": 1,
    }
    operator_req.assign.pycode.Clear()
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_erase_key():
    @meta(owner="nitin@fennel.ai")
    @dataset
    class PageViewEvent:
        user_id: str
        page_id: str
        t: datetime

    @meta(owner="nitin@fennel.ai")
    @dataset
    class Sessions:
        user_id: str = field(key=True, erase_key=True)
        avg_session_secs: float
        t: datetime

        @pipeline
        @inputs(PageViewEvent)
        def pipeline_window(cls, app_event: Dataset):
            sessions = (
                app_event.groupby("user_id", window=Session("10m"))
                .aggregate()
                .assign(
                    "duration_secs",
                    int,
                    lambda df: (df["window"].end - df["window"].begin).secs(),
                )
                .groupby("user_id")
                .aggregate(
                    Average(
                        of="duration_secs",
                        window=Continuous("forever"),
                        into_field="avg_session_secs",
                    )
                )
            )
            return sessions

    view = InternalTestClient()
    view.add(Sessions)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "name": "Sessions",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"stringType": {}},
                    },
                ]
            },
            "values": {
                "fields": [
                    {"name": "avg_session_secs", "dtype": {"double_type": {}}}
                ]
            },
            "timestamp": "t",
            "erase_keys": ["user_id"],
        },
        "version": 1,
        "metadata": {"owner": "nitin@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "user_id": {},
            "avg_session_secs": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[0]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


def test_emit_strategy():
    @meta(owner="nitin@fennel.ai")
    @dataset
    class A1:
        user_id: str
        page_id: str
        event_id: str
        t: datetime

    @meta(owner="nitin@fennel.ai")
    @dataset
    class A2:
        user_id: str = field(key=True)
        page_id: str = field(key=True)
        count: int
        t: datetime

        @pipeline
        @inputs(A1)
        def pipeline_window(cls, event: Dataset):
            return event.groupby("user_id", "page_id").aggregate(
                count=Count(window=Continuous("1h")), emit="final"
            )

    view = InternalTestClient()
    view.add(A1)  # type: ignore
    view.add(A2)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 2
    d = {
        "name": "A2",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"stringType": {}},
                    },
                    {
                        "name": "page_id",
                        "dtype": {"stringType": {}},
                    },
                ]
            },
            "values": {
                "fields": [{"name": "count", "dtype": {"int_type": {}}}]
            },
            "timestamp": "t",
            "erase_keys": [],
        },
        "version": 1,
        "metadata": {"owner": "nitin@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "user_id": {},
            "page_id": {},
            "count": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[1]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )


def test_aggregation_emit_final():
    @meta(owner="nitin@fennel.ai")
    @dataset
    class A1:
        user_id: str
        page_id: str
        event_id: str
        t: datetime

    @meta(owner="nitin@fennel.ai")
    @dataset
    class A2:
        user_id: str = field(key=True)
        count: int
        t: datetime

        @pipeline
        @inputs(A1)
        def pipeline_window(cls, event: Dataset):
            return (
                event.groupby("user_id", "page_id")
                .aggregate(count=Count(window=Continuous("1h")), emit="final")
                .groupby("user_id")
                .aggregate(count=Count(window=Continuous("1h")))
            )

    view = InternalTestClient()
    view.add(A1)  # type: ignore
    view.add(A2)  # type: ignore
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 2
    d = {
        "name": "A2",
        "dsschema": {
            "keys": {
                "fields": [
                    {
                        "name": "user_id",
                        "dtype": {"stringType": {}},
                    },
                ]
            },
            "values": {
                "fields": [{"name": "count", "dtype": {"int_type": {}}}]
            },
            "timestamp": "t",
            "erase_keys": [],
        },
        "version": 1,
        "metadata": {"owner": "nitin@fennel.ai"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "user_id": {},
            "count": {},
            "t": {},
        },
        "pycode": {},
    }
    dataset_req = sync_request.datasets[1]
    dataset_req.pycode.Clear()
    expected_dataset_request = ParseDict(d, ds_proto.CoreDataset())
    assert dataset_req == expected_dataset_request, error_message(
        dataset_req, expected_dataset_request
    )
