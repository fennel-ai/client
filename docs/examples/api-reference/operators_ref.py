import json
from datetime import datetime
from typing import Optional

import pandas as pd

from fennel.datasets import dataset, field
from fennel.datasets import pipeline, Dataset
from fennel.lib.aggregate import Sum, Count
from fennel.lib.includes import includes
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs
from fennel.lib.window import Window
from fennel.sources import source, Webhook

webhook = Webhook(name="fennel_webhook")


@meta(owner="aditya@fennel.ai")
@source(webhook.endpoint("Activity"))
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="aditya@fennel.ai")
@dataset
class MerchantCategory:
    merchant: int = field(key=True)
    category: str
    timestamp: datetime


@meta(owner="abhay@fennel.ai")
@dataset
class UserTransactions:
    user_id: int
    merchant_id: str
    transaction_amount: float
    timestamp: datetime

    @pipeline(version=1)
    @inputs(Activity)
    def create_user_transactions(cls, activity: Dataset):
        # docsnip transform
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df = pd.concat([df_json, df[["user_id", "timestamp"]]], axis=1)
            df["transaction_amount"] = df["transaction_amount"] / 100
            return df[
                ["merchant_id", "transaction_amount", "user_id", "timestamp"]
            ]

        transformed_ds = activity.transform(
            extract_info,
            schema={
                "transaction_amount": float,
                "merchant_id": int,
                "user_id": int,
                "timestamp": datetime,
            },
        )
        # /docsnip
        return transformed_ds


@meta(owner="abhay@fennel.ai")
@dataset
class UserFirstActionAmount:
    user_id: int = field(key=True)
    transaction_amount: float
    timestamp: datetime

    @pipeline(version=1)
    @inputs(UserTransactions)
    def create_user_first_action_category(cls, txns: UserTransactions):
        # docsnip first
        first_txns = txns.groupby("user_id").first()
        # /docsnip
        return first_txns


@meta(owner="aditya@fennel.ai")
@dataset
class FraudActivityDataset:
    merchant_category: str = field(key=True)
    txn_sum: float
    txn_count: int
    timestamp: datetime

    @pipeline(version=1)
    @inputs(UserTransactions, MerchantCategory)
    def create_fraud_dataset(
        cls, txns: UserTransactions, merchant_category: Dataset
    ):
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

        # docsnip join
        joined_ds = dropped_ds.join(
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
            [
                Sum(of="txn_amount", window=Window("1h"), into_field="txn_sum"),
                Count(window=Window("1h"), into_field="txn_count"),
            ]
        )
        # /docsnip
        return aggregated_ds
