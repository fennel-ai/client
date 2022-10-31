import pytest
from datetime import datetime
from typing import Optional

from fennel.dataset import Count, dataset, field, Sum
from fennel.test_lib import *


def test_MultipleDateTime(grpc_stub):
    with pytest.raises(ValueError) as e:
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
            timestamp: datetime

    _ = InternalTestView(grpc_stub)
    assert str(e.value) == "Multiple timestamp fields are not supported."


def test_InvalidRetentionWindow(grpc_stub):
    with pytest.raises(TypeError) as e:
        @dataset(retention=324)
        class Activity:
            user_id: int
            action_type: float
            amount: Optional[float]
            timestamp: datetime
    assert str(
        e.value) == "duration 324 must be a specified as a string for eg. " \
                    "1d/2m/3y."


def test_InvalidAggregatedDataset(grpc_stub):
    @dataset
    class FraudReport:
        merchant_id: int = field(key=True)
        user_id: int
        user_age: int
        transaction_amount: float
        timestamp: datetime

    @dataset.aggregate(FraudReport)
    class FraudReportAggregatedDataset:
        merchant_id: int = field(key=True)
        timestamp: datetime
        num_merchant_fraudulent_transactions: int = Count(window="forever",
            value="transaction_amount")
        num_merchant_fraudulent_transactions_7d: int = Count(window="1w")
        total_amount_transacted: int = Sum(window="forever",
            value=FraudReport.transaction_amount)
