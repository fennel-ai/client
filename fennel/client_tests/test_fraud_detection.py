from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.aggregate import Sum
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.lib.window import Window
from fennel.sources import source, Webhook
from fennel.test_lib import mock

webhook = Webhook(name="fennel_webhook")


@meta(owner="henry@fennel.ai")
@source(webhook.endpoint("CreditCardTransactions"))
@dataset
class CreditCardTransactions:
    trans_num: str = field(key=True)  # Id
    trans_date_trans_time: datetime = field(
        timestamp=True
    )  # Temporal aspect of our data
    cc_num: int
    merchant: str
    category: str
    amt: float
    first: str
    last: str
    gender: str
    street: str
    city: str
    state: str
    zip: int
    lat: float
    long: float
    city_pop: int
    job: str
    dob: str  # string since mutliple timestamps not supported in a singular dataset it said
    unix_time: int
    merch_lat: float
    merch_long: float
    is_fraud: int


@meta(owner="henry@fennel.ai")
@source(webhook.endpoint("Regions"))
@dataset
class Regions:
    created_at: datetime
    state: str
    region: str


@meta(owner="henry@fennel.ai")
@dataset
class UserTransactionSums:
    cc_num: int = field(key=True)  # needs to be key for groubpby
    trans_date_trans_time: datetime = field(timestamp=True)
    sum_amt_1d: float
    sum_amt_7d: float

    @pipeline(version=1)
    @inputs(CreditCardTransactions)
    def first_pipeline(cls, transactions: Dataset):
        return transactions.groupby("cc_num").aggregate(
            [
                Sum(of="amt", window=Window("1d"), into_field="sum_amt_1d"),
                Sum(of="amt", window=Window("7d"), into_field="sum_amt_7d"),
            ]
        )


@meta(owner="henry@fennel.ai")
@featureset
class UserTransactionSumsFeatures:
    cc_num: int = feature(id=1)
    sum_amt_1d: float = feature(id=2)
    sum_amt_7d: float = feature(id=3)

    # If come from different featuresets have to include full path x.y
    @extractor(depends_on=[UserTransactionSums])
    @inputs(cc_num)
    @outputs(sum_amt_1d, sum_amt_7d)
    def my_extractor(cls, ts: pd.Series, cc_nums: pd.Series):
        df, found = UserTransactionSums.lookup(ts, cc_num=cc_nums)  # type: ignore

        # Fill any Na values with 0 as this means there were no transactions and the sum was 0
        df["sum_amt_1d"] = df["sum_amt_1d"].fillna(0).astype(float)
        df["sum_amt_7d"] = df["sum_amt_7d"].fillna(0).astype(float)
        return df[["sum_amt_1d", "sum_amt_7d"]]


@mock
def test_fraud_detection_pipeline(client):
    states_to_regions = {
        "WA": "West",
        "OR": "West",
        "CA": "West",
        "NV": "West",
        "ID": "West",
        "MT": "West",
        "WY": "West",
        "UT": "West",
        "CO": "West",
        "AK": "West",
        "HI": "West",
        "ME": "Northeast",
        "VT": "Northeast",
        "NY": "Northeast",
        "NH": "Northeast",
        "MA": "Northeast",
        "RI": "Northeast",
        "CT": "Northeast",
        "NJ": "Northeast",
        "PA": "Northeast",
        "ND": "Midwest",
        "SD": "Midwest",
        "NE": "Midwest",
        "KS": "Midwest",
        "MN": "Midwest",
        "IA": "Midwest",
        "MO": "Midwest",
        "WI": "Midwest",
        "IL": "Midwest",
        "MI": "Midwest",
        "IN": "Midwest",
        "OH": "Midwest",
        "WV": "South",
        "DC": "South",
        "MD": "South",
        "VA": "South",
        "KY": "South",
        "TN": "South",
        "NC": "South",
        "MS": "South",
        "AR": "South",
        "LA": "South",
        "AL": "South",
        "GA": "South",
        "SC": "South",
        "FL": "South",
        "DE": "South",
        "AZ": "Southwest",
        "NM": "Southwest",
        "OK": "Southwest",
        "TX": "Southwest",
    }

    region_to_state = pd.DataFrame.from_dict(states_to_regions, orient="index")
    region_to_state["state"] = region_to_state.index
    region_to_state = region_to_state.rename(columns={0: "region"}).reset_index(
        drop=True
    )
    region_to_state.insert(0, "created_at", datetime.now())
    # Upload transaction_data dataframe to the Transactions dataset on the mock client
    transaction_data_sample = pd.read_csv(
        "fennel/client_tests/data/fraud_sample.csv"
    )

    # Convert our transaction times to datetimes from objects
    transaction_data_sample["trans_date_trans_time"] = pd.to_datetime(
        transaction_data_sample["trans_date_trans_time"]
    )
    transaction_data_sample["cc_num"] = transaction_data_sample[
        "cc_num"
    ].astype(int)
    client.sync(
        datasets=[CreditCardTransactions, Regions, UserTransactionSums],
        featuresets=[UserTransactionSumsFeatures],
    )

    response = client.log(
        "fennel_webhook", "CreditCardTransactions", transaction_data_sample
    )
    assert response.status_code == 200
    # Upload region_to_state dataframe to the Regions dataset on the mock client
    response = client.log("fennel_webhook", "Regions", region_to_state)
    assert response.status_code == 200

    assert len(client.aggregated_datasets["UserTransactionSums"]) == 2
    lookup_dataframe = transaction_data_sample[["cc_num"]].rename(
        columns={"cc_num": "UserTransactionSumsFeatures.cc_num"}
    )
    # Add a random row to the lookup dataframe to test that it is ignored
    lookup_dataframe.loc[lookup_dataframe.shape[0]] = {
        "UserTransactionSumsFeatures.cc_num": 99
    }

    df = client.extract_features(
        input_feature_list=[UserTransactionSumsFeatures.cc_num],
        # Input from featureset,
        output_feature_list=[
            UserTransactionSumsFeatures.cc_num,
            UserTransactionSumsFeatures.sum_amt_1d,
            UserTransactionSumsFeatures.sum_amt_7d,
        ],
        input_dataframe=lookup_dataframe,
    )
    assert df["UserTransactionSumsFeatures.sum_amt_1d"].sum() == 0
    assert df["UserTransactionSumsFeatures.sum_amt_7d"].sum() == 0
