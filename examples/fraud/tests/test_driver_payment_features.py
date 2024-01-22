import pandas as pd
from fraud.datasets.payment import (
    ChargesDS,
    TransactionsDS,
    PaymentEventDS,
    LastPaymentDS,
    PaymentDS,
)
from fraud.datasets.payment_ids import (
    PaymentAccountSrcDS,
    PaymentAccountAssociationSrcDS,
    AccountSrcDS,
    PaymentIdentifierDS,
)
from fraud.featuresets.driver import Request
from fraud.featuresets.payment import PaymentFS

from fennel.test_lib import mock


def log_payment_identifier_datasets(client):
    df = (
        pd.read_csv("examples/fraud/data/payment/payment_account.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(id=lambda x: x["id"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="PaymentAccountSrcDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv(
            "examples/fraud/data/payment/payment_account_association.csv"
        )
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(
            payment_account_id=lambda x: x["payment_account_id"].astype(int)
        )
        .assign(account_id=lambda x: x["account_id"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook",
        endpoint="PaymentAccountAssociationSrcDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/payment/account.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(primary_driver_id=lambda x: x["primary_driver_id"].astype(int))
        .assign(id=lambda x: x["id"].astype(int))
    )
    log_response = client.log(
        webhook="app_webhook", endpoint="AccountSrcDS", df=df
    )
    assert log_response.status_code == 200, log_response.json()


@mock
def test_payment_identifier(client):
    """This test all tests if the mapping of different account ids is correct"""
    sync_response = client.sync(
        datasets=[
            PaymentAccountSrcDS,
            PaymentAccountAssociationSrcDS,
            AccountSrcDS,
            PaymentIdentifierDS,
        ]
    )
    assert sync_response.status_code == 200

    log_payment_identifier_datasets(client)

    df = client.get_dataset_df("PaymentIdentifierDS")
    assert df.shape == (10, 5)
    # Assert that the first 5 rows are correct
    df = df[:5]
    assert df["driver_id"].to_list() == [
        6982917,
        23152354,
        23315855,
        23348774,
        23404126,
    ]
    assert df["customer_id"].to_list() == [
        "cus_L1AhSE9FhwqLU8",
        "cus_L30RTG6xHqyCts",
        "cus_L6r9fDEkC3hyjG",
        "cus_L7YJPGFFZVkLeU",
        "cus_L8fOEPxIrocWAc",
    ]
    assert df["account_id"].to_list() == [
        6983139,
        23152574,
        23316075,
        23348994,
        23404346,
    ]


@mock
def test_min_max_radar_score(client):
    """This test tests if the min and max radar score are correct"""
    sync_response = client.sync(
        datasets=[
            ChargesDS,
            PaymentAccountSrcDS,
            PaymentAccountAssociationSrcDS,
            AccountSrcDS,
            PaymentIdentifierDS,
            TransactionsDS,
        ]
    )
    assert sync_response.status_code == 200
    log_payment_identifier_datasets(client)

    df = (
        pd.read_csv("examples/fraud/data/payment/charges.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(risk_score=lambda x: x["risk_score"].astype(float))
        .assign(amount=lambda x: x["amount"].astype(float))
        .assign(amount_refunded=lambda x: x["amount_refunded"].astype(float))
        .assign(reservation_id=lambda x: x["reservation_id"].astype("Int64"))
    )

    log_response = client.log(
        webhook="app_webhook",
        endpoint="ChargesDS",
        df=df,
    )
    assert log_response.status_code == 200, log_response.json()

    df = client.get_dataset_df("TransactionsDS")
    assert df.shape[0] == 3


@mock
def test_last_payment(client):
    """This test tests if the last payment amount is correct"""
    sync_response = client.sync(
        datasets=[
            ChargesDS,
            PaymentAccountSrcDS,
            PaymentAccountAssociationSrcDS,
            AccountSrcDS,
            PaymentIdentifierDS,
            PaymentEventDS,
            TransactionsDS,
            LastPaymentDS,
        ]
    )
    assert sync_response.status_code == 200
    log_payment_identifier_datasets(client)

    df = (
        pd.read_csv("examples/fraud/data/payment/payment_event.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(invoice_id=lambda x: x["invoice_id"].astype("Int64"))
        .assign(debit_card=lambda x: x["debit_card"].astype("bool"))
        .assign(postal_code="123")
    )

    log_response = client.log(
        webhook="app_webhook", endpoint="PaymentEventDS", df=df
    )

    assert log_response.status_code == 200, log_response.json()

    df = client.get_dataset_df("LastPaymentDS")
    assert df.shape == (8, 8)


@mock
def test_payment(client):
    sync_response = client.sync(
        datasets=[
            ChargesDS,
            PaymentAccountSrcDS,
            PaymentAccountAssociationSrcDS,
            AccountSrcDS,
            PaymentIdentifierDS,
            PaymentEventDS,
            TransactionsDS,
            PaymentDS,
            LastPaymentDS,
        ],
        featuresets=[PaymentFS, Request],
    )
    assert sync_response.status_code == 200
    log_payment_identifier_datasets(client)

    df = (
        pd.read_csv("examples/fraud/data/payment/payment_event.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(invoice_id=lambda x: x["invoice_id"].astype("Int64"))
        .assign(debit_card=lambda x: x["debit_card"].astype("Int64"))
        .assign(postal_code="123")
    )
    log_response = client.log(
        webhook="app_webhook", endpoint="PaymentEventDS", df=df
    )
    assert log_response.status_code == 200, log_response.json()

    df = (
        pd.read_csv("examples/fraud/data/payment/charges.csv")
        .assign(
            created=lambda x: pd.to_datetime(x["created"]).apply(
                lambda y: y.tz_localize(None)
            )
        )
        .assign(risk_score=lambda x: x["risk_score"].astype(float))
        .assign(amount=lambda x: x["amount"].astype(float))
        .assign(amount_refunded=lambda x: x["amount_refunded"].astype(float))
        .assign(reservation_id=lambda x: x["reservation_id"].astype("Int64"))
    )

    log_response = client.log(
        webhook="app_webhook",
        endpoint="ChargesDS",
        df=df,
    )

    assert log_response.status_code == 200, log_response.json()

    feature_df = client.extract_features(
        input_feature_list=[
            "Request.driver_id",
            "Request.reservation_id",
        ],
        input_dataframe=pd.DataFrame(
            {
                "Request.driver_id": [
                    23315855,
                    23348774,
                ],
                "Request.reservation_id": [13176875, 13398944],
            }
        ),
        output_feature_list=[PaymentFS],
    )
    assert feature_df.shape == (2, 7)
    assert feature_df["PaymentFS.driver_id"].to_list() == [23315855, 23348774]
    assert feature_df["PaymentFS.num_postal_codes"].to_list() == [1, 1]
    assert feature_df[
        "PaymentFS.num_failed_payment_verification_attempts"
    ].to_list() == [2, 11]
    assert feature_df["PaymentFS.payment_type"].to_list() == ["CARD", "CARD"]
    assert feature_df["PaymentFS.is_debit_card"].to_list() == [True, True]
    assert feature_df["PaymentFS.max_radar_score"].to_list() == [0.0, 5.0]
