from datetime import datetime

from typing import Optional

from fennel.datasets import dataset
from fennel.datasets import pipeline, Dataset
from fennel.lib import meta, inputs
from fennel.connectors import source, Webhook

webhook = Webhook(name="fennel_webhook")


@meta(owner="aditya@fennel.ai")
@source(webhook.endpoint("Activity"), disorder="14d", cdc="append")
@dataset(history="4m")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@meta(owner="aditya@fennel.ai")
@dataset
class TxnActivityDataset:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime

    @pipeline
    @inputs(Activity)
    def create_fraud_dataset(
        cls,
        activity: Dataset,
    ):
        # docsnip schema_debug
        filtered_activity = activity.filter(lambda x: x.action_type == "txn")
        print(filtered_activity.schema())
        # /docsnip
        return filtered_activity
