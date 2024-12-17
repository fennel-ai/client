from fennel.featuresets import featureset, feature as F
from fennel.lib import meta
from fennel.lib.schema import struct
from typing import Optional, List
from datetime import datetime
from fennel.expr import col, var, lit


__owner__ = "aditya@fennel.ai"


@struct
class CreditCategory:
    detailed: str
    primary: str


@struct
class Transaction:
    transaction_id: str
    account_id: str
    amount: float

    account_owner: Optional[str]
    check_number: Optional[str]
    credit_category: Optional[CreditCategory]
    date: Optional[datetime]
    date_posted: Optional[datetime]
    date_transacted: Optional[datetime]
    iso_currency_code: Optional[str]
    merchant_name: Optional[str]
    original_description: Optional[str]
    unofficial_currency_code: Optional[str]


@featureset
class PlaidReport:
    payload: str
    transactions: List[Transaction] = F(
        col("payload")
        .str.json_extract("$.report.items[0].accounts[0].transactions")
        .fillnull("[]")
        .str.parse(List[Transaction]),
        version=3,
    )

    num_transactions: int = F(col("transactions").list.len())

    ecommerce_txns: List[Transaction] = F(
        col("transactions").list.filter(
            "t",
            lit("amazon|walmart|target|bestbuy").str.contains(
                var("t").struct.get("merchant_name").fillnull("Unknown")
            ),
        ),
        version=1,
    )
    num_ecomerce_txns: int = F(col("ecommerce_txns").list.len())
    any_ecomerce_txn: bool = F(col("ecommerce_txns").list.len() > 0)
    total_ecomerce_spend: float = F(
        col("ecommerce_txns").list.map("t", var("t").struct.get("amount")).sum()
    )

    atm_txns: List[Transaction] = F(
        col("transactions").list.filter(
            "t",
            var("t")
            .struct.get("credit_category")
            .struct.get("primary")
            .fillnull("Unknown")
            == lit("ATM"),
        ),
        version=1,
    )
    num_atm_txns: int = F(col("atm_txns").list.len())
    any_atm_txn: bool = F(col("atm_txns").list.len() > 0)
    total_atm_spend: float = F(
        col("atm_txns").list.map("t", var("t").struct.get("amount")).sum()
    )

    retail_txns: List[Transaction] = F(
        col("transactions").list.filter(
            "t",
            var("t")
            .struct.get("credit_category")
            .struct.get("primary")
            .fillnull("Unknown")
            == lit("RETAIL"),
        ),
        version=1,
    )
    num_retail_txns: int = F(col("retail_txns").list.len())
    any_retail_txn: bool = F(col("retail_txns").list.len() > 0)
    total_retail_spend: float = F(
        col("retail_txns").list.map("t", var("t").struct.get("amount")).sum()
    )
