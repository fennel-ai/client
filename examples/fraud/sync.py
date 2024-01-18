import argparse

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
from fraud.datasets.sourced import (
    EventTrackerDS,
    DriverLicenseCountryDS,
    VehicleSummaryDS,
    RentCarCheckoutEventDS,
    DriverDS,
    DriverCreditScoreDS,
)
from fraud.datasets.vehicle import (
    LocationDS,
    LocationDS2,
    LocationToNewMarketArea,
    IdToMarketAreaDS,
    VehicleSummary,
    MarketAreaDS,
)
from fraud.datasets.velocity import (
    ReservationDS,
    NumCompletedTripsDS,
    CancelledTripsDS,
    LoginEventsDS,
    LoginsLastDayDS,
    BookingFlowCheckoutPageDS,
    CheckoutPagesLastDayDS,
    ReservationSummaryDS,
    PastApprovedDS,
)
from fraud.featuresets.all_features import FraudModel
from fraud.featuresets.driver import (
    Request,
    AgeFS,
    CreditScoreFS,
    ReservationLevelFS,
)
from fraud.featuresets.payment import PaymentFS
from fraud.featuresets.vehicle import VehicleFS
from fraud.featuresets.velocity import DriverVelocityFS

from fennel.client import Client


def main(url, preview):
    print(f"URL: {url}")
    print(f"Preview Mode: {'ON' if preview else 'OFF'}")

    # Your synchronization logic goes here
    # Use the URL and preview values as needed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync Script")
    # URL parameter (string)
    parser.add_argument("url", type=str, help="URL to sync with")
    # Tier selector
    parser.add_argument("tier", type=str, help="Tier to sync with")
    # Token - optional because for local runs, we don't need it
    parser.add_argument(
        "--token", type=str, help="Token to use", required=False
    )
    # Preview parameter (boolean)
    parser.add_argument(
        "--preview", action="store_true", help="Run in preview mode"
    )
    args = parser.parse_args()
    print("Starting client  with url: " + args.url)
    client = Client(args.url, token=args.token)
    client.sync(
        datasets=[
            PaymentAccountSrcDS,
            PaymentAccountAssociationSrcDS,
            AccountSrcDS,
            PaymentIdentifierDS,
            ChargesDS,
            TransactionsDS,
            PaymentEventDS,
            LastPaymentDS,
            PaymentDS,
            EventTrackerDS,
            DriverLicenseCountryDS,
            VehicleSummaryDS,
            RentCarCheckoutEventDS,
            DriverDS,
            DriverCreditScoreDS,
            LocationDS,
            LocationDS2,
            LocationToNewMarketArea,
            IdToMarketAreaDS,
            VehicleSummary,
            MarketAreaDS,
            ReservationDS,
            NumCompletedTripsDS,
            CancelledTripsDS,
            LoginEventsDS,
            LoginsLastDayDS,
            BookingFlowCheckoutPageDS,
            CheckoutPagesLastDayDS,
            ReservationSummaryDS,
            PastApprovedDS,
        ],
        featuresets=[
            Request,
            AgeFS,
            CreditScoreFS,
            FraudModel,
            PaymentFS,
            DriverVelocityFS,
            ReservationLevelFS,
            VehicleFS,
        ],
        preview=args.preview,
        tier=args.tier,
    )
