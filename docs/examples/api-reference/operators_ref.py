import json
from datetime import datetime

import pandas as pd
from typing import Optional

from fennel.datasets import dataset, field
from fennel.datasets import pipeline, Dataset
from fennel.lib.aggregate import Sum, Count
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


@meta(owner="aditya@fennel.ai")
@dataset
class FraudActivityDataset:
    merchant_category: str = field(key=True)
    txn_sum: float
    txn_count: int
    timestamp: datetime

    @pipeline(version=1)
    @inputs(Activity, MerchantCategory)
    def create_fraud_dataset(cls, activity: Dataset, merchant_category: Dataset):
        # docsnip transform
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_json = df["metadata"].apply(json.loads).apply(pd.Series)
            df = pd.concat([df_json, df[["user_id", "timestamp"]]], axis=1)
            df["transaction_amount"] = df["transaction_amount"] / 100
            return df[["merchant_id", "transaction_amount", "user_id", "timestamp"]]

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

        # docsnip rename
        renamed_ds = transformed_ds.rename(
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
        joined_ds = dropped_ds.left_join(merchant_category, on=["merchant"], within=("forever", "60s"))
        # /docsnip

        joined_ds = joined_ds.rename({"category": "merchant_category"})

        # docsnip filter
        joined_ds = joined_ds.filter(lambda df: df["merchant_category"] is not None)
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
