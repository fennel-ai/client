from datetime import datetime
from typing import Optional

import pandas as pd
import pytest
from google.protobuf.json_format import ParseDict  # type: ignore

from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature as F
from fennel.lib import meta, inputs, outputs
from fennel.testing import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    gender: str
    # Users date of birth
    dob: str
    age: int
    account_creation_date: datetime
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


@meta(owner="test@test.com")
@featureset
class User:
    id: int
    age: float


def test_invalid_multiple_extracts():
    # Tests a derived and manual extractor for the same feature
    with pytest.raises(TypeError) as e:

        @meta(owner="user@xyz.ai")
        @featureset
        class UserInfo3:
            user_id: int = F(User.id)
            age: int = F(
                UserInfoDataset.age,
                default=0,
            )

            @extractor(deps=[UserInfoDataset])
            @inputs(user_id)
            @outputs(age)
            def get_age(cls, ts: pd.Series, user_id: pd.Series):
                df = UserInfoDataset.lookup(ts, user_id=user_id)  # type: ignore
                return df.fillna(0)

        view = InternalTestClient()
        view.add(User)
        view.add(UserInfo3)
        view._get_sync_request_proto()
    assert (
        str(e.value)
        == "Feature `age` is extracted by multiple extractors including `get_age` in featureset `UserInfo3`."
    )


def test_invalid_missing_fields():
    # no field nor feature
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo4:
            user_id: int = F(User.id)
            age: int = F(default=0)

    assert (
        str(e.value)
        == 'Please specify a reference to a field of a dataset to use "default" param'
    )

    # missing dataset key in current featureset
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo6:
            age: int = F(UserInfoDataset.age, default=0)

    assert (
        str(e.value)
        == "Key field `user_id` for dataset `UserInfoDataset` not found in provider `UserInfo6` for feature: `age` auto generated extractor"
    )
