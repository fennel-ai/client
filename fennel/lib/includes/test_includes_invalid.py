from datetime import datetime
from typing import Optional

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import includes, meta, inputs, outputs
from fennel.sources import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="append")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    timestamp: datetime = field(timestamp=True)
    country: str


def square(x: int) -> int:
    return x**2


def cube(x: int) -> int:
    return x**3


@includes(square)
def power_4(x: int) -> int:
    return square(square(x))


@meta(owner="test@test.com")
@featureset
class UserInfoExtractor:
    userid: int = feature(id=1)
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_power_four: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor(depends_on=[UserInfoDataset])
    @inputs(userid)
    @outputs(age, age_power_four, age_cubed, is_name_common)
    def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
        df, _ = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
        df[str(cls.userid)] = user_id
        df[str(cls.age_power_four)] = power_4(df["age"])
        df[str(cls.age_cubed)] = cube(df["age"])
        df[str(cls.is_name_common)] = df["name"].isin(["John", "Mary", "Bob"])
        return df[
            [
                str(cls.age),
                str(cls.age_power_four),
                str(cls.age_cubed),
                str(cls.is_name_common),
            ]
        ]


def power_4_alt(x: int) -> int:
    return x**4


@pytest.mark.integration
@mock
def test_simple_invalid_extractor(client):
    client.commit(
        message="some msg",
        datasets=[UserInfoDataset],
        featuresets=[UserInfoExtractor],
    )
    now = datetime.utcnow()
    data = [
        [18232, "John", 32, "USA", now],
        [18234, "Monica", 24, "Chile", now],
    ]
    columns = ["user_id", "name", "age", "country", "timestamp"]
    df = pd.DataFrame(data, columns=columns)
    response = client.log("fennel_webhook", "UserInfoDataset", df)
    assert response.status_code == requests.codes.OK, response.json()
    if client.is_integration_client():
        client.sleep()

    with pytest.raises(Exception) as e:
        client.query(
            outputs=[UserInfoExtractor],
            inputs=[UserInfoExtractor.userid],
            input_dataframe=pd.DataFrame(
                {"UserInfoExtractor.userid": [18232, 18234]}
            ),
        )
    if client.is_integration_client():
        assert "Extractor UserInfoExtractor.get_user_info failed" in str(
            e.value
        )
        assert "name 'power_4' is not defined" in str(e.value)
    else:
        str(e.value) == "Extractor `get_user_info` in `UserInfoExtractor` "
        "failed to run with error: name 'power_4' is not defined. "


# This test is only an integration test because it requires a backend
# to store state in.
@pytest.mark.integration
@mock
def test_invalid_code_changes(client):
    def commit():
        @meta(owner="test@test.com")
        @featureset
        class UserInfoExtractorInvalid:
            userid: int = feature(id=1)
            age: int = feature(id=4).meta(
                owner="aditya@fennel.ai"
            )  # type: ignore
            age_power_four: int = feature(id=5)
            age_cubed: int = feature(id=6)
            is_name_common: bool = feature(id=7)

            @extractor(depends_on=[UserInfoDataset])
            @includes(power_4, cube)
            @inputs(userid)
            @outputs(age, age_power_four, age_cubed, is_name_common)
            def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
                df, _ = UserInfoDataset.lookup(  # type: ignore
                    ts, user_id=user_id
                )
                df[str(cls.userid)] = user_id
                df[str(cls.age_power_four)] = power_4(df["age"])
                df[str(cls.age_cubed)] = cube(df["age"])
                df[str(cls.is_name_common)] = df["name"].isin(
                    ["John", "Mary", "Bob"]
                )
                return df[
                    [
                        str(cls.age),
                        str(cls.age_power_four),
                        str(cls.age_cubed),
                        str(cls.is_name_common),
                    ]
                ]

        client.commit(
            message="some msg",
            datasets=[UserInfoDataset],
            featuresets=[UserInfoExtractorInvalid],
        )

    def failed_sync_with_new_include():
        @meta(owner="test@test.com")
        @featureset
        class UserInfoExtractorInvalid:
            userid: int = feature(id=1)
            age: int = feature(id=4).meta(
                owner="aditya@fennel.ai"
            )  # type: ignore
            age_power_four: int = feature(id=5)
            age_cubed: int = feature(id=6)
            is_name_common: bool = feature(id=7)

            @extractor(depends_on=[UserInfoDataset])
            @includes(power_4_alt, cube)
            @inputs(userid)
            @outputs(age, age_power_four, age_cubed, is_name_common)
            def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
                df, _ = UserInfoDataset.lookup(  # type: ignore
                    ts, user_id=user_id
                )
                df[str(cls.userid)] = user_id
                df[str(cls.age_power_four)] = power_4_alt(df["age"])
                df[str(cls.age_cubed)] = cube(df["age"])
                df[str(cls.is_name_common)] = df["name"].isin(
                    ["John", "Mary", "Bob"]
                )
                return df[
                    [
                        str(cls.age),
                        str(cls.age_power_four),
                        str(cls.age_cubed),
                        str(cls.is_name_common),
                    ]
                ]

        with pytest.raises(Exception) as e:
            client.commit(
                message="some msg",
                datasets=[UserInfoDataset],
                featuresets=[UserInfoExtractorInvalid],
            )
        assert (
            "cannot update code of extractor UserInfoExtractorInvalid.get_user_info"
            in str(e.value)
        )

    def successful_sync_with_new_feature():
        @meta(owner="test@test.com")
        @featureset
        class UserInfoExtractorInvalid:
            userid: int = feature(id=1)
            age: int = feature(id=4).meta(
                owner="aditya@fennel.ai"
            )  # type: ignore
            age_power_four: int = feature(id=5)
            age_cubed: int = feature(id=6)
            is_name_common: bool = feature(id=7)
            another_feature: int = feature(id=8)

            @extractor(depends_on=[UserInfoDataset], version=1)
            @includes(power_4, cube)
            @inputs(userid)
            @outputs(age, age_power_four, age_cubed, is_name_common)
            def get_user_info(cls, ts: pd.Series, user_id: pd.Series):
                df, _ = UserInfoDataset.lookup(  # type: ignore
                    ts, user_id=user_id
                )
                df[str(cls.userid)] = user_id
                df[str(cls.age_power_four)] = power_4(df["age"])
                df[str(cls.age_cubed)] = cube(df["age"])
                df[str(cls.is_name_common)] = df["name"].isin(
                    ["John", "Mary", "Bob"]
                )
                return df[
                    [
                        str(cls.age),
                        str(cls.age_power_four),
                        str(cls.age_cubed),
                        str(cls.is_name_common),
                    ]
                ]

        client.commit(
            message="some msg",
            datasets=[UserInfoDataset],
            featuresets=[UserInfoExtractorInvalid],
        )

    if client.is_integration_client():
        commit()
        failed_sync_with_new_include()
        successful_sync_with_new_feature()
