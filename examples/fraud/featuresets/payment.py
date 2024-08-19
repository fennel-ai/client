from fraud.datasets.payment import (
    PaymentDS,
    TransactionsDS,
    LastPaymentDS,
)
from fraud.featuresets.request import Request

from fennel.featuresets import featureset, feature

__owner__ = "eng@app.com"


@featureset
class PaymentFS:
    driver_id: int = feature(Request.driver_id)
    num_postal_codes: int = feature(PaymentDS.num_postal_codes, default=0)
    num_failed_payment_verification_attempts: int = feature(
        PaymentDS.num_failed_payment_verification_attempts, default=0
    )
    payment_type: str = feature(LastPaymentDS.payment_method, default="UNKNOWN")
    is_debit_card: bool = feature(LastPaymentDS.is_debit_card, default=False)
    max_radar_score: float = feature(
        TransactionsDS.max_radar_score, default=0.0
    )
    min_radar_score: float = feature(
        TransactionsDS.min_radar_score, default=0.0
    )
