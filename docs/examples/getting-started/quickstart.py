from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional

import pandas as pd
import json
from fennel.test_lib import mock_client

# docsnip quickstart
from fennel.datasets import dataset, field, pipeline, Dataset
from fennel.featuresets import (
    feature,
    Feature,
    featureset,
    extractor,
    depends_on,
)
from fennel.lib.aggregate import Count
from fennel.lib.metadata import meta
from fennel.lib.window import Window
from fennel.lib.schema import oneof, DataFrame, Series
from fennel.sources import source, Postgres, Snowflake
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
)

# Step 1: define connectors to your data sources. Here
# it is done in the UI (so that credentials don't have to be
# put in code) and referred by the names given to them
postgres = Postgres.get(name="my_rdbms")
warehouse = Snowflake.get(name="my_warehouse")


# Step 2: define a few datasets - each dataset has some
# typed fields, an owner, and can optionally be given tags
# datasets can be sourced from sources defined in step 1
@dataset
@source(postgres.table("merchant_info", cursor="last_modified"), every="1m")
@meta(owner="aditya@fennel.ai")
class MerchantInfo:
    merchant_id: int = field(key=True)
    merchant_category: str
    city: str
    merchant_num_employees: int
    created_on: datetime
    last_modified: datetime = field(timestamp=True)


@dataset
@source(postgres.table("user_info", cursor="last_modified"), every="1m")
@meta(owner="nikhil@fennel.ai", tags=["PII"])
class UserInfo:
    user_id: int = field(key=True)
    name: str
    gender: oneof(str, ["male", "female"])
    dob: str
    age: int
    country: Optional[str]
    last_modified: datetime = field(timestamp=True)

    @expectations
    def get_expectations(cls):
        return [
            expect_column_values_to_be_between(
                column=str(cls.age), min_value=13, max_value=100, mostly=0.95
            )
        ]


# Here Fennel brings data from warehouse. This way, Fennel
# let you bring data from disparate systems in the same plane
# which makes it easier to work across many data sysetms
@dataset
@source(warehouse.table("user_activity", cursor="timestamp"), every="15m")
@meta(owner="luke@fennel.ai")
class Activity:
    user_id: int
    action_type: str
    amount: Optional[float]
    metadata: str
    timestamp: datetime


@dataset
@meta(owner="laura@fennel.ai")
class FraudReportAggregateByCity:
    merchant_id: int = field(key=True)
    city: str = field(key=True)
    timestamp: datetime
    num_merchant_city_fraud_transactions: int
    num_merchant_city_fraud_transactions_7d: int

    # Fennel lets you write pipelines that operate on one or more
    # datasets - you get SQL like power but with arbitrary Python.
    # and the same pipeline code works for batch & streaming data alike!
    @pipeline(id=1)
    def create_fraud_dataset(
        cls, activity: Dataset[Activity], merchant_info: Dataset[MerchantInfo]
    ):
        def extract_info(df: pd.DataFrame) -> pd.DataFrame:
            df_metadata_dict = df["metadata"].apply(json.loads).apply(pd.Series)
            df["transaction_amount"] = df_metadata_dict["transaction_amt"]
            df["merchant_id"] = df_metadata_dict["merchant_id"]
            return df[
                ["merchant_id", "transaction_amount", "user_id", "timestamp"]
            ]

        filtered = activity.filter(lambda r: r["action_type"] == "report_txn")
        extracted = filtered.transform(
            extract_info,
            schema={
                "transaction_amount": float,
                "merchant_id": int,
                "user_id": int,
                "timestamp": datetime,
            },
        )
        joined = extracted.left_join(merchant_info, on=["merchant_id"])
        transformed = joined.transform(
            lambda df: df.fillna({"city": "unknown"}),
            schema={
                "merchant_id": int,
                "transaction_amount": float,
                "timestamp": datetime,
                "city": str,
            },
        )
        return transformed.groupby("merchant_id", "city").aggregate(
            [
                Count(
                    window=Window("forever"),
                    into_field="num_merchant_city_fraud_transactions",
                ),
                Count(
                    window=Window("1w"),
                    into_field="num_merchant_city_fraud_transactions_7d",
                ),
            ]
        )


# --------------------- Featuresets ---------------------#


# Step 3: define some featuresets - each featureset
# is a collection of logically related features. Features
# can be added/removed from featuresets following a tagging
# mechanism similar to protobufs and each feature itself is
# immutable once created
@featureset
@meta(owner="anti-fraud-team@fennel.ai")
class Merchant:
    merchant_id: int = feature(id=1)
    merchant_category: str = feature(id=2)
    city: str = feature(id=3)
    merchant_age: int = feature(id=4)
    merchant_num_employees: int = feature(id=5)

    # Fennel lets you specify code that knows how to
    # extract one or more features of a featureset
    @extractor
    @depends_on(MerchantInfo)
    def get_merchant_info(
        cls, ts: Series[datetime], merchant_id: Series[merchant_id]
    ) -> Series[merchant_age]:
        df, _found = MerchantInfo.lookup(ts, merchant_id=merchant_id)
        df["current_timestamp"] = ts
        df["merchant_age"] = df.apply(
            lambda x: (x["current_timestamp"] - x["created_on"]).days, axis=1
        )
        return df[["merchant_age"]]

    @extractor
    @depends_on(MerchantInfo)
    def get_merchant_features(
        cls, ts: Series[datetime], merchant_id: Series[merchant_id]
    ) -> DataFrame[merchant_category, city, merchant_num_employees]:
        df, found = MerchantInfo.lookup(ts, merchant_id=merchant_id)
        df.fillna(
            {
                "merchant_category": "unknown",
                "city": -1,
                "merchant_num_employees": 0,
            },
            inplace=True,
        )
        return df[["merchant_category", "city", "merchant_num_employees"]]


@featureset
@meta(owner="abhay@fennel.ai")
class MerchantBehaviorFeatures:
    num_merchant_city_fraud_transactions: int = feature(id=1)
    num_merchant_city_fraud_transactions_7d: int = feature(id=2)
    fradulent_transaction_ratio: float = feature(id=3)

    @extractor
    @depends_on(FraudReportAggregateByCity)
    def get_merchant_fraud_features(
        cls,
        ts: Series[datetime],
        merchant_id: Series[Merchant.merchant_id],
        city: Series[Merchant.city],
    ) -> DataFrame[
        num_merchant_city_fraud_transactions,
        num_merchant_city_fraud_transactions_7d,
        fradulent_transaction_ratio,
    ]:
        df, _found = FraudReportAggregateByCity.lookup(
            ts, merchant_id=merchant_id, city=city
        )
        df["fradulent_transaction_ratio"] = df.apply(
            lambda x: x.num_merchant_city_fraud_transactions_7d
            / x.num_merchant_city_fraud_transactions,
            axis=1,
        )
        return df[
            [
                "num_merchant_city_fraud_transactions_7d",
                "num_merchant_city_fraud_transactions",
                "fradulent_transaction_ratio",
            ]
        ]
# /docsnip


@mock_client
def test_fraud_detection_pipeline(client):
    client.sync(
        datasets=[
            MerchantInfo,
            UserInfo,
            Activity,
            FraudReportAggregateByCity,
        ],
        featuresets=[
            Merchant,
            MerchantBehaviorFeatures,
        ],
    )
    now = datetime.now()
    data = [
        {
            "merchant_id": 1,
            "merchant_category": "food",
            "city": "NY",
            "merchant_num_employees": 100,
            "created_on": now - timedelta(days=10),
            "last_modified": now,
        },
        {
            "merchant_id": 2,
            "merchant_category": "retail",
            "city": "SF",
            "merchant_num_employees": 500,
            "created_on": now - timedelta(days=30),
            "last_modified": now,
        },
        {
            "merchant_id": 3,
            "merchant_category": "retail",
            "city": "NY",
            "merchant_num_employees": 300,
            "created_on": now - timedelta(days=20),
            "last_modified": now,
        },
    ]
    res = client.log("MerchantInfo", pd.DataFrame(data))
    assert res.status_code == 200, res.json()

    data = [
        {
            "user_id": 111,
            "dob": "1990-11-01",
            "age": 32,
            "country": "US",
            "last_modified": now,
            "name": "John",
            "gender": "male",
        },
        {
            "user_id": 222,
            "dob": "1992-01-04",
            "age": 31,
            "country": "India",
            "last_modified": now,
            "name": "Raj",
            "gender": "male",
        },
        {
            "user_id": 331,
            "dob": "1994-03-01",
            "age": 29,
            "country": "US",
            "last_modified": now,
            "gender": "female",
            "name": "Laura",
        },
    ]
    res = client.log("UserInfo", pd.DataFrame(data))
    assert res.status_code == 200, res.json()

    data = [
        {
            "user_id": 111,
            "action_type": "report_txn",
            "amount": 100,
            "metadata": json.dumps({"transaction_amt": 100, "merchant_id": 1}),
            "timestamp": now,
        },
        {
            "user_id": 222,
            "action_type": "report_txn",
            "amount": 100,
            "metadata": json.dumps({"transaction_amt": 100, "merchant_id": 2}),
            "timestamp": now,
        },
        {
            "user_id": 331,
            "action_type": "report_txn",
            "amount": 100,
            "metadata": json.dumps({"transaction_amt": 100, "merchant_id": 3}),
            "timestamp": now,
        },
        {
            "user_id": 111,
            "action_type": "report_txn",
            "amount": 100,
            "metadata": json.dumps({"transaction_amt": 100, "merchant_id": 1}),
            "timestamp": now,
        },
    ]
    res = client.log("Activity", pd.DataFrame(data))
    assert res.status_code == 200, res.json()

    feature_df = client.extract_features(
        output_feature_list=[MerchantBehaviorFeatures, Merchant.merchant_age],
        input_feature_list=[Merchant.merchant_id],
        input_dataframe=pd.DataFrame({"Merchant.merchant_id": [1, 2, 3]}),
    )
    assert feature_df.shape == (3, 4)
    assert feature_df["Merchant.merchant_age"].tolist() == [
        10,
        30,
        20,
    ]
    assert feature_df[
        "MerchantBehaviorFeatures.num_merchant_city_fraud_transactions"
    ].tolist() == [
        2,
        1,
        1,
    ]
