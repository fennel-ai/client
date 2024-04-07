from fraud.datasets.payment import (
    PaymentDS,
    TransactionsDS,
    LastPaymentDS,
)
from fraud.featuresets.request import Request

from fennel.featuresets import featureset, feature as F

__owner__ = "eng@app.com"


@featureset
class PaymentFS:
    driver_id: int = F(Request.driver_id)
    num_postal_codes: int = F(PaymentDS.num_postal_codes, default=0)
    num_failed_payment_verification_attempts: int = F(
        PaymentDS.num_failed_payment_verification_attempts, default=0
    )
    payment_type: str = F(LastPaymentDS.payment_method, default="UNKNOWN")
    is_debit_card: bool = F(LastPaymentDS.is_debit_card, default=False)
    max_radar_score: float = F(TransactionsDS.max_radar_score, default=0.0)
    min_radar_score: float = F(TransactionsDS.min_radar_score, default=0.0)
