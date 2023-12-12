from datetime import datetime
from fennel.datasets import dataset

# docsnip module_ownership

__owner__ = "luke@fennel.ai"


@dataset
class User:
    uid: int
    country: str
    age: float
    signup_at: datetime


@dataset
class Transaction:
    uid: int
    amount: float
    payment_country: str
    merchant_id: int
    timestamp: datetime


# /docsnip
