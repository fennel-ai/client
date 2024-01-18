"""
This file contains all the required datasets for linking different payment ids to a driver_id.
"""
from datetime import datetime

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.lib.schema import inputs
from fennel.sources import Webhook, source

__owner__ = "eng@app.com"

webhook = Webhook(name="app_webhook")


@dataset
@source(webhook.endpoint("PaymentAccountSrcDS"), tier="local")
class PaymentAccountSrcDS:
    customer_id: str = field(key=True)
    id: int
    created: datetime


@dataset
@source(
    webhook.endpoint("PaymentAccountAssociationSrcDS"),
    tier="local",
)
class PaymentAccountAssociationSrcDS:
    payment_account_id: int = field(key=True)
    account_id: int
    created: datetime


@dataset
@source(webhook.endpoint("AccountSrcDS"), tier="local")
class AccountSrcDS:
    primary_driver_id: int
    id: int = field(key=True)
    created: datetime


@dataset
class PaymentIdentifierDS:
    """
    This dataset maps driver_id to several ids from multiple tables.
    """

    driver_id: int
    customer_id: str = field(key=True)
    account_id: int
    id: int
    created: datetime

    @pipeline(version=1)
    @inputs(
        PaymentAccountSrcDS,
        PaymentAccountAssociationSrcDS,
        AccountSrcDS,
    )
    def payment_identifier(
        cls,
        payment_account: Dataset,
        paa: Dataset,
        account: Dataset,
    ):
        return (
            payment_account.join(
                paa,
                left_on=["id"],
                right_on=["payment_account_id"],
                how="inner",
            )
            .join(
                account,
                left_on=["account_id"],
                right_on=["id"],
                how="inner",
                within=("forever", "1d"),
            )
            .rename({"primary_driver_id": "driver_id"})
        )
