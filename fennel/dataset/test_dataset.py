import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from google.protobuf.json_format import ParseDict
from typing import Optional

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
                    "functionSourceCode": "",
                }
            }
        ]
    }

    # Ignoring schema validation since they are bytes and not human-readable
    sync_request.dataset_requests[0].schema = b""
    sync_request.dataset_requests[0].pull_lookup.function = b""
    sync_request.dataset_requests[0].pull_lookup.function_source_code = ""
    expected_sync_request = ParseDict(d, SyncRequest())
    assert sync_request == expected_sync_request


def test_DatasetWithPipe(grpc_stub):
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
