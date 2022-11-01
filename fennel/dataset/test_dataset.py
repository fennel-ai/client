import pandas as pd
import requests
from datetime import datetime, timedelta
from google.protobuf.json_format import ParseDict
from typing import Optional

from fennel.dataset import Count, dataset, depends_on, field, Sum, aggregate, \
    Window
from fennel.gen.services_pb2 import SyncRequest
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


def test_DatasetWithRetention(grpc_stub):
    @dataset(retention="4m")
    class Activity:
        user_id: int
        action_type: float
        amount: Optional[float]
        timestamp: datetime

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


@dataset
class FraudReport:
    merchant_id: int = field(key=True)
    user_id: int
    user_age: int
    transaction_amount: float
    timestamp: datetime


def test_AggregatedDataset(grpc_stub):
    @dataset
    @aggregate(FraudReport)
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int = Count(window=Window())
        num_merchant_fraudulent_transactions_7d: int = Count(
            window=Window("1w"))
        total_amount_transacted: int = Sum(window=Window(),
            value=FraudReport.transaction_amount)

    assert FraudReportAggregatedDataset._max_staleness == timedelta(days=30)
    assert FraudReportAggregatedDataset._retention == timedelta(days=730)
    view = InternalTestView(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "FraudReportAggregatedDataset",
                "fields": [
                    {
                        "name": "merchant_id",
                        "isKey": True,
                    },
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions",
                        "aggregation": {
                            "type": "COUNT",
                            "window_spec": {
                                "foreverWindow": True,
                            }
                        }
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions_7d",
                        "aggregation": {
                            "type": "COUNT",
                            "window_spec": {
                                "window": {
                                    "start": 604800000000
                                }
                            }
                        }
                    },
                    {
                        "name": "total_amount_transacted",
                        "aggregation": {
                            "type": "SUM",
                            "window_spec": {
                                "foreverWindow": True,
                            },
                            "value_field": "transaction_amount"
                        }
                    }
                ],
                "retention": 63072000000000,
                "maxStaleness": 2592000000000,
                "mode": "pandas",
                "isAggregated": True,
                "baseDataset": "FraudReport",
            }
        ]
    }

    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request


def test_AggregatedDatasetWithRetention(grpc_stub):
    @dataset(retention="1y")
    @aggregate(FraudReport)
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int = Count(window=Window())
        num_merchant_fraudulent_transactions_7d: int = Count(
            window=Window("1w"))
        total_amount_transacted: int = Sum(window=Window(),
            value=FraudReport.transaction_amount)

    assert FraudReportAggregatedDataset._max_staleness == timedelta(days=30)
    assert FraudReportAggregatedDataset._retention == timedelta(days=365)
    view = InternalTestView(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "FraudReportAggregatedDataset",
                "fields": [
                    {
                        "name": "merchant_id",
                        "isKey": True,
                    },
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions",
                        "aggregation": {
                            "type": "COUNT",
                            "window_spec": {
                                "foreverWindow": True,
                            }
                        }
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions_7d",
                        "aggregation": {
                            "type": "COUNT",
                            "window_spec": {
                                "window": {
                                    "start": 604800000000
                                }
                            }
                        }
                    },
                    {
                        "name": "total_amount_transacted",
                        "aggregation": {
                            "type": "SUM",
                            "window_spec": {
                                "foreverWindow": True,
                            },
                            "value_field": "transaction_amount"
                        }
                    }
                ],
                "retention": 31536000000000,
                "maxStaleness": 2592000000000,
                "mode": "pandas",
                "isAggregated": True,
                "baseDataset": "FraudReport",
            }
        ]
    }

    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request


def test_AggregatedDatasetOrderReversed(grpc_stub):
    @aggregate(FraudReport)
    @dataset(retention="1y")
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int = Count(window=Window(
            start=None))
        num_merchant_fraudulent_transactions_7d: int = Count(
            window=Window("1w"))
        total_amount_transacted: int = Sum(window=Window(),
            value=FraudReport.transaction_amount)

    assert FraudReportAggregatedDataset._max_staleness == timedelta(days=30)
    assert FraudReportAggregatedDataset._retention == timedelta(days=365)
    view = InternalTestView(grpc_stub)
    view.add(FraudReportAggregatedDataset)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
                "name": "FraudReportAggregatedDataset",
                "fields": [
                    {
                        "name": "merchant_id",
                        "isKey": True,
                    },
                    {
                        "name": "timestamp",
                        "isTimestamp": True,
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions",
                        "aggregation": {
                            "type": "COUNT",
                            "window_spec": {
                                "foreverWindow": True,
                            }
                        }
                    },
                    {
                        "name": "num_merchant_fraudulent_transactions_7d",
                        "aggregation": {
                            "type": "COUNT",
                            "window_spec": {
                                "window": {
                                    "start": 604800000000
                                }
                            }
                        }
                    },
                    {
                        "name": "total_amount_transacted",
                        "aggregation": {
                            "type": "SUM",
                            "window_spec": {
                                "foreverWindow": True,
                            },
                            "value_field": "transaction_amount"
                        }
                    }
                ],
                "retention": 31536000000000,
                "maxStaleness": 2592000000000,
                "mode": "pandas",
                "isAggregated": True,
                "baseDataset": "FraudReport",
            }
        ]
    }

    # Ignoring schema validation since they are bytes and not human readable
    sync_request.dataset_requests[0].schema = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request


def test_DatasetWithPull(grpc_stub):
    API_ENDPOINT_URL = 'http://transunion.com/v1/credit_score'

    @dataset(retention="1y", max_staleness='7d')
    class UserCreditScore:
        user_id: int = field(key=True)
        credit_score: float
        timestamp: datetime

        @staticmethod
        @depends_on(UserInfoDataset, FraudReport)
        def pull(df: pd.DataFrame) -> pd.DataFrame:
            user_info_df = UserInfoDataset.lookup(user_id=df['userid'])
            user_list = df['user_id'].tolist()
            names = user_info_df['user'].tolist()
            resp = requests.get(API_ENDPOINT_URL,
                json={"users": user_list, "names": names})
            if resp.status_code != 200:
                return df
            results = resp.json()['results']
            df['credit_score'] = pd.Series(results)
            return df

    assert UserCreditScore._max_staleness == timedelta(days=7)
    assert UserCreditScore._retention == timedelta(days=365)
    view = InternalTestView(grpc_stub)
    view.add(UserCreditScore)
    sync_request = view.to_proto()
    assert len(sync_request.dataset_requests) == 1
    d = {
        "datasetRequests": [
            {
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
                    "functionSourceCode": """        @staticmethod
        @depends_on(UserInfoDataset, FraudReport)
        def pull(df: pd.DataFrame) -> pd.DataFrame:
            user_info_df = UserInfoDataset.lookup(user_id=df['userid'])
            user_list = df['user_id'].tolist()
            names = user_info_df['user'].tolist()
            resp = requests.get(API_ENDPOINT_URL,
                json={"users": user_list, "names": names})
            if resp.status_code != 200:
                return df
            results = resp.json()['results']
            df['credit_score'] = pd.Series(results)
            return df\n""",
                    "dependsOnDatasets": ["UserInfoDataset", "FraudReport"],
                }
            }
        ]
    }

    # Ignoring schema validation since they are bytes and not human-readable
    sync_request.dataset_requests[0].schema = b""
    sync_request.dataset_requests[0].pull_lookup.function = b""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request
