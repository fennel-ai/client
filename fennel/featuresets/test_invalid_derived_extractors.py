import pandas as pd
import pytest
from datetime import datetime
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import meta, inputs, outputs
from fennel.sources import source, Webhook

from fennel.testing import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="append")
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
    timestamp: datetime = field(timestamp=True)


@meta(owner="test@test.com")
@featureset
class User:
    id: int = feature(id=1)
    age: float = feature(id=2)


def test_invalid_multiple_extracts():
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo1:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = (
                feature(id=2)
                .extract(
                    field=UserInfoDataset.age,
                    default=0,
                )
                .extract(feature=User.age)
            )

    assert str(e.value) == "extract() can only be called once for feature id=2"

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo2:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = (
                feature(id=2)
                .extract(
                    field=UserInfoDataset.age,
                    default=0,
                )
                .meta(owner="zaki@fennel.ai")
                .extract(feature=User.age)
            )

    assert str(e.value) == "extract() can only be called once for feature id=2"

    # Tests a derived and manual extractor for the same feature
    with pytest.raises(TypeError) as e:

        @meta(owner="user@xyz.ai")
        @featureset
        class UserInfo3:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                default=0,
            )

            @extractor(depends_on=[UserInfoDataset])
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
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo4:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = feature(id=2).extract(
                default=0,
            )

    assert (
        str(e.value)
        == "Exactly one of field or feature must be specified to extract feature id=2"
    )

    # both field and feature specified
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo5:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age, default=0, feature=User.age
            )

    assert (
        str(e.value)
        == "Exactly one of field or feature must be specified to extract feature id=2"
    )

    # missing dataset key in current featureset
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo6:
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                default=0,
            )

    assert (
        str(e.value)
        == "Key field `user_id` for dataset `UserInfoDataset` not found in provider `UserInfo6` for feature: `age` auto generated extractor"
    )

    # missing dataset key in provider
    with pytest.raises(ValueError) as e:

        @featureset
        class UserRequest:
            name: str = feature(id=1)

        @featureset
        class UserInfo8:
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                provider=UserRequest,
                default=0,
            )

    assert (
        str(e.value)
        == "Dataset key `user_id` not found in provider `UserRequest` for extractor `_fennel_lookup_age`"
    )
