import json
from datetime import datetime, timedelta

import pandas as pd
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional, List

import fennel.gen.dataset_pb2 as ds_proto
from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.gen.services_pb2 import SyncRequest
from fennel.lib.aggregate import Count
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import Embedding, inputs
from fennel.lib.window import Window
from fennel.sources import source, Webhook
from fennel.test_lib import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
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


def test_simple_dataset():
    assert UserInfoDataset._history == timedelta(days=730)
    view = InternalTestClient()
    view.add(UserInfoDataset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "datasets": [
            {
                "name": "UserInfoDataset",
                "metadata": {"owner": "test@test.com"},
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
                    "age": {},
                    "name": {},
                    "account_creation_date": {},
                    "country": {},
                    "user_id": {},
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
                            "webhook": {"name": "fennel_webhook"},
                        },
                        "endpoint": "UserInfoDataset",
                    }
                },
                "dataset": "UserInfoDataset",
                "lateness": "3600s",
            }
        ],
        "extdbs": [
            {"name": "fennel_webhook", "webhook": {"name": "fennel_webhook"}}
        ],
    }
    # Ignoring schema validation since they are bytes and not human readable
    expected_sync_request = ParseDict(d, SyncRequest())
    sync_request.datasets[0].pycode.Clear()
    assert sync_request == expected_sync_request, error_message(
        sync_request, expected_sync_request
    )


@source(webhook.endpoint("Activity"))
@meta(owner="test@test.com")
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
                "metadata": {"owner": "test@test.com"},
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
                            "webhook": {"name": "fennel_webhook"},
                        },
                        "endpoint": "Activity",
                    }
                },
                "dataset": "Activity",
                "lateness": "3600s",
            }
        ],
        "extdbs": [
            {"name": "fennel_webhook", "webhook": {"name": "fennel_webhook"}}
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
@source(webhook.endpoint("DealerDataset"))
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
                            "webhook": {"name": "fennel_webhook"},
                        },
                        "endpoint": "DealerDataset",
                    }
                },
                "dataset": "Dealer",
                "lateness": "3600s",
            }
        ],
        "extdbs": [
            {"name": "fennel_webhook", "webhook": {"name": "fennel_webhook"}}
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
#             cls, ts: pd.Series[datetime], user_id: pd.Series[int], names: pd.Series[str]
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
    @dataset
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

        @pipeline(version=1)
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


@pipeline(version=1)
@includes(add_one)
@inputs(A, B)
def pipeline1(cls, a: Dataset, b: Dataset):
    return a.join(b, how="left", left_on=["a1"], right_on=["b1"])
"""
    assert expected_gen_code == pipeline_req.pycode.generated_code
    expected_source_code = """@pipeline(version=1)
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
        "version": 1,
        "active": True,
        "pycode": {},
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
    @dataset
    class B:
        b1: int = field(key=True)
        t: datetime

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABCDatasetDefault:
        a1: int = field(key=True)
        t: datetime

        @pipeline(version=1)
        @inputs(A, B)
        def pipeline1(cls, a: Dataset, b: Dataset):
            return a.join(b, how="left", left_on=["a1"], right_on=["b1"])

    @meta(owner="aditya@fennel.ai")
    @dataset
    class ABDatasetLow:
        a1: int = field(key=True)
        t: datetime

        @pipeline(version=1)
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

        @pipeline(version=1)
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

        @pipeline(version=1)
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
        "version": 1,
        "active": True,
        "pycode": {},
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
        "version": 1,
        "active": True,
        "pycode": {},
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
        "version": 1,
        "active": True,
        "pycode": {},
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
        "version": 1,
        "active": True,
        "pycode": {},
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
        "version": 1,
        "active": True,
        "pycode": {},
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

        @pipeline(version=1)
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
                ]
            },
            "timestamp": "timestamp",
        },
        "metadata": {"owner": "test@test.com"},
        "history": "63072000s",
        "retention": "63072000s",
        "field_metadata": {
            "merchant_id": {},
            "num_merchant_fraudulent_transactions": {},
            "num_merchant_fraudulent_transactions_7d": {},
            "timestamp": {},
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
        "version": 1,
        "active": True,
        "pycode": {},
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
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )

    operator_req = sync_request.operators[6]
    o = {
        "id": "d7d235e0397bf22f09131d7a2650343e",
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
            ],
        },
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
    )


def test_delete_and_rename_column():
    @dataset
    class A:
        a1: int = field(key=True)
        a2: int
        a3: str
        t: datetime

    @dataset
    class B:
        b1: int = field(key=True)
        t: datetime

        @pipeline(version=1)
        @inputs(A)
        def from_a(cls, a: Dataset):
            x = a.rename({"a1": "b1"})
            return x.drop(["a2", "a3"])


def test_union_datasets():
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

        @pipeline(version=1)
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
            "keys": {
                "fields": [
                    {
                        "name": "a1",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {},
            "timestamp": "t",
        },
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
        "version": 1,
        "input_dataset_names": ["A"],
        "active": True,
        "pycode": {},
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
    }
    expected_operator_request = ParseDict(o, ds_proto.Operator())
    assert operator_req == expected_operator_request, error_message(
        operator_req, expected_operator_request
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


def test_search_dataset():
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

        @pipeline(version=1)
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
        doc_id: int = field(key=True)
        bert_embedding: Embedding[128]
        fast_text_embedding: Embedding[256]
        num_words: int
        num_stop_words: int
        top_10_unique_words: str
        creation_timestamp: datetime

        @pipeline(version=1)
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
            "keys": {
                "fields": [
                    {
                        "name": "doc_id",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
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
            "keys": {
                "fields": [
                    {
                        "name": "doc_id",
                        "dtype": {"int_type": {}},
                    }
                ]
            },
            "values": {
                "fields": [
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
        "version": 1,
        "input_dataset_names": ["Document"],
        "active": True,
        "pycode": {},
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
        "version": 1,
        "input_dataset_names": ["DocumentContentDataset"],
        "active": True,
        "pycode": {},
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

        @pipeline(version=1)
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
                "action_type": float,
                "timestamp": datetime,
                "amount": Optional[float],
                "user_id": int,
            }

            filtered_ds = activity.filter(
                lambda df: df["action_type"] == "report"
            )

            assert filtered_ds.schema() == {
                "action_type": float,
                "amount": Optional[float],
                "timestamp": datetime,
                "user_id": int,
            }

            x = filtered_ds.transform(
                extract_info,
                schema={
                    "transaction_amount": float,
                    "merchant_id": int,
                    "user_id": int,
                    "timestamp": datetime,
                },
            )

            assert x.schema() == {
                "merchant_id": int,
                "transaction_amount": float,
                "user_id": int,
                "timestamp": datetime,
            }
            return x
