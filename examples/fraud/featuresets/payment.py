from fraud.datasets.payment import (
    PaymentDS,
    TransactionsDS,
    LastPaymentDS,
)
from fraud.featuresets.driver import Request

from fennel.featuresets import featureset, feature

__owner__ = "eng@app.com"


@featureset
class PaymentFS:
    driver_id: int = feature(id=1).extract(feature=Request.driver_id)
    num_postal_codes: int = feature(id=2).extract(
        field=PaymentDS.num_postal_codes, default=0
    )
    num_failed_payment_verification_attempts: int = feature(id=3).extract(
        field=PaymentDS.num_failed_payment_verification_attempts, default=0
    )
    payment_type: str = feature(id=4).extract(
        field=LastPaymentDS.payment_method, default="UNKNOWN"
    )
    is_debit_card: bool = feature(id=5).extract(
        field=LastPaymentDS.is_debit_card, default=False
    )
    max_radar_score: float = feature(id=6).extract(
        field=TransactionsDS.max_radar_score, default=0
    )
    min_radar_score: float = feature(id=7).extract(
        field=TransactionsDS.min_radar_score, default=0
    )
