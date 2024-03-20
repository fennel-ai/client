"""
This file contains all the required datasets for driver payment features.
"""

from datetime import datetime
from typing import Optional

import pandas as pd
from fraud.datasets.payment_ids import PaymentIdentifierDS

from fennel.datasets import (
    Count,
    Min,
    Max,
    Sum,
    dataset,
    field,
    pipeline,
    Dataset,
    index,
)
from fennel.lib import inputs
from fennel.sources import Webhook, source

__owner__ = "eng@app.com"

webhook = Webhook(name="app_webhook")


@source(
    webhook.endpoint("ChargesDS"), disorder="14d", cdc="append", tier="local"
)
@dataset
class ChargesDS:
    customer: str = field(key=True)
    risk_score: float
    amount: float
    amount_refunded: float
    reservation_id: Optional[int]
    created: datetime = field(timestamp=True)


@index
@dataset
class TransactionsDS:
    driver_id: int = field(key=True)
    min_radar_score: float
    max_radar_score: float
    created: datetime = field(timestamp=True)

    @pipeline
    @inputs(ChargesDS, PaymentIdentifierDS)
    def transactions(cls, charges: Dataset, payment_identifier: Dataset):
        return (
            charges.join(
                payment_identifier,
                left_on=["customer"],
                right_on=["customer_id"],
                how="inner",
            )
            .groupby("driver_id")
            .aggregate(
                Min(
                    of="risk_score",
                    window="forever",
                    default=0.0,
                    into_field="min_radar_score",
                ),
                Max(
                    of="risk_score",
                    window="forever",
                    default=0.0,
                    into_field="max_radar_score",
                ),
            )
        )


@dataset
@source(
    webhook.endpoint("PaymentEventDS"),
    disorder="14d",
    cdc="append",
    tier="local",
)
class PaymentEventDS:
    customer_id: str
    payment_provider: str
    result: str
    postal_code: str
    payment_method: str
    invoice_id: Optional[int]
    debit_card: Optional[int]
    created: datetime


@index
@dataset
class LastPaymentDS:
    driver_id: int = field(key=True)
    payment_provider: str
    result: str
    postal_code: str
    invoice_id: Optional[int]
    payment_method: str
    is_debit_card: bool
    created: datetime = field(timestamp=True)

    @pipeline
    @inputs(PaymentEventDS, PaymentIdentifierDS)
    def last_payment(cls, payment_event: Dataset, payment_identifier: Dataset):
        return (
            payment_event.join(
                payment_identifier,
                on=["customer_id"],
                how="inner",
            )
            .assign(
                "is_debit_card",
                bool,
                lambda df: df["debit_card"].apply(
                    lambda x: True if pd.notna(x) else False
                ),
            )
            .drop("debit_card", "customer_id", "account_id", "id")
            .groupby("driver_id")
            .first()
        )


@index
@dataset
class PaymentDS:
    driver_id: int = field(key=True)
    num_postal_codes: int
    num_failed_payment_verification_attempts: int
    created: datetime = field(timestamp=True)

    @pipeline
    @inputs(PaymentEventDS, PaymentIdentifierDS)
    def payment(cls, payment_event: Dataset, payment_identifier: Dataset):
        return (
            payment_event.join(
                payment_identifier,
                on=["customer_id"],
                how="inner",
            )
            .drop("account_id", "id")
            .assign(
                "result_val",
                int,
                lambda df: (df["result"] != "SUCCESS").astype(int),
            )
            .groupby("driver_id")
            .aggregate(
                Count(
                    of="postal_code",
                    window="forever",
                    into_field="num_postal_codes",
                    unique=True,
                    approx=True,
                ),
                Sum(
                    of="result_val",
                    window="forever",
                    into_field="num_failed_payment_verification_attempts",
                ),
            )
        )
