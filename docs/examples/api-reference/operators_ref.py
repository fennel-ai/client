import json
from datetime import datetime
from typing import Optional

import pandas as pd

from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field, pipeline, Dataset, Sum, Count
from fennel.dtypes import Continuous, Window, Session
from fennel.lib import inputs

webhook = Webhook(name="fennel_webhook")
__owner__ = "aditya@fennel.ai"


@source(webhook.endpoint("Activity"), disorder="14d", cdc="append")
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@dataset(index=True)
class MerchantCategory:
    merchant: int = field(key=True)
    category: str
    timestamp: datetime


@dataset
class UserTransactions:
    user_id: int
    merchant_id: int
    transaction_amount: float
    metadata: str
    timestamp: datetime

    @pipeline
    @inputs(Activity)
    def create_user_transactions(cls, activity: Dataset):
        # docsnip dropnull
        dropnull_amounts = activity.dropnull("amount")
        # /docsnip

        # docsnip transform
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df = pd.concat([df_json, df[["user_id", "timestamp"]]], axis=1)
            df["transaction_amount"] = df["transaction_amount"] / 100
            df["metadata"] = "some_metadata"
            return df[
                [
                    "merchant_id",
                    "transaction_amount",
                    "user_id",
                    "timestamp",
                    "metadata",
                ]
            ]

        transformed_ds = dropnull_amounts.transform(
            extract_info,
            schema={
                "transaction_amount": float,
                "merchant_id": int,
                "user_id": int,
                "timestamp": datetime,
                "metadata": str,
            },
        )
        # /docsnip
        return transformed_ds


@dataset
class UserTransactionsV2:
    user_id: int
    merchant_id: int
    transaction_amount: float
    transaction_amount_sq: float
    user_id_str: str
    metadata: str
    timestamp: datetime

    @pipeline
    @inputs(UserTransactions)
    def create_user_transactions(cls, user_transactions: Dataset):
        # docsnip assign
        assign_ds = user_transactions.assign(
            "transaction_amount_sq",
            float,
            lambda df: df["transaction_amount"] ** 2,
        ).assign("user_id_str", str, lambda df: df["user_id"].astype(str))
        # /docsnip
        return assign_ds


@dataset
class UserFirstAction:
    user_id: int = field(key=True)
    transaction_amount: float
    timestamp: datetime

    @pipeline
    @inputs(UserTransactions)
    def create_user_first_action_category(cls, txns: Dataset):
        # docsnip first
        first_txns = txns.groupby("user_id").first()
        return first_txns.drop("merchant_id", "metadata")
        # /docsnip


@dataset
class FraudActivityDataset:
    merchant_category: str = field(key=True)
    txn_sum: float
    txn_count: int
    timestamp: datetime

    @pipeline
    @inputs(UserTransactions, MerchantCategory)
    def create_fraud_dataset(cls, txns: Dataset, merchant_category: Dataset):
        # docsnip rename
        renamed_ds = txns.rename(
            {
                "transaction_amount": "txn_amount",
                "merchant_id": "merchant",
            }
        )
        # /docsnip

        # docsnip drop
        dropped_ds = renamed_ds.drop(["user_id"])
        # /docsnip

        # docsnip select
        select_ds = dropped_ds.select("txn_amount", "merchant")
        # /docsnip

        # docsnip join
        joined_ds = select_ds.join(
            merchant_category,
            how="left",
            on=["merchant"],
            within=("forever", "60s"),
        )
        # /docsnip

        joined_ds = joined_ds.rename({"category": "merchant_category"})

        # docsnip filter
        joined_ds = joined_ds.filter(
            lambda df: df["merchant_category"] is not None
        )
        # /docsnip

        cur_schema = joined_ds.schema()
        cur_schema["merchant_category"] = str
        joined_ds = joined_ds.transform(lambda x: x, cur_schema)

        # docsnip aggregate
        aggregated_ds = joined_ds.groupby("merchant_category").aggregate(
            txn_sum=Sum(of="txn_amount", window=Continuous("1h")),
            txn_count=Count(window=Continuous("1h")),
        )
        # /docsnip
        return aggregated_ds


@dataset
class ActivitySession:
    user_id: int = field(key=True)
    window: Window = field(key=True)
    timestamp: datetime

    @pipeline
    @inputs(Activity)
    def create_sessions_dataset(cls, activity: Dataset):
        # docsnip window
        sessions = activity.groupby(
            "user_id", window=Session("60m")
        ).aggregate()
        # /docsnip
        return sessions
