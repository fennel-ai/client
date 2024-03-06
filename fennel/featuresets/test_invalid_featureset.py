from datetime import datetime
from typing import Optional, List
import sys

import pandas as pd
import pytest

from fennel import meta
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import inputs, outputs
from fennel.sources import source, Webhook

# noinspection PyUnresolvedReferences
from fennel.testing import *

__owner__ = "data@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="append")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    gender: str
    # Users date of birth
    dob: str
    age: int
    ids: List[int]
    account_creation_date: datetime
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


@meta(owner="aditya@fennel.ai")
@featureset
class User:
    id: int = feature(id=1)
    age: float = feature(id=2)


def test_featureset_as_input():
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfoInvalid:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)

            @extractor(depends_on=[UserInfoDataset])
            @inputs(User)
            @outputs(userid, home_geoid)
            def get_user_info1(cls, ts: pd.Series, user: pd.Series):
                pass

    assert (
        str(e.value)
        == "Parameter `User` is not a feature, but a `<class 'fennel.featuresets.featureset.Featureset'>`, and hence not supported as an input for the extractor `get_user_info1`"
    )


def test_complex_featureset():
    with pytest.raises(TypeError) as e:

        @meta(owner="aditya@fennel.ai")
        @featureset
        class UserInfo:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            # The users gender among male/female/non-binary
            gender: str = feature(id=3)
            age: int = feature(id=4).meta(owner="aditya@fennel.ai")
            income: int = feature(id=5)

            @extractor(depends_on=[UserInfoDataset])
            @inputs(User.id)
            @outputs(userid, home_geoid)
            def get_user_info1(cls, ts: pd.Series, user_id: pd.Series):
                pass

            @extractor(depends_on=[UserInfoDataset])
            @inputs(User.id)
            @outputs(gender, age)
            def get_user_info2(cls, ts: pd.Series, user_id: pd.Series):
                pass

            @extractor
            @inputs(User.id)
            @outputs(gender)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

        view = InternalTestClient()
        view.add(User)
        view.add(UserInfo)
        view._get_sync_request_proto()
    assert (
        str(e.value)
        == "Feature `gender` is extracted by multiple extractors including `get_user_info3` in featureset `UserInfo`."
    )


def test_extract_anoather_featureset():
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            # The users gender among male/female/non-binary
            gender: str = feature(id=3)
            age: int = feature(id=4).meta(owner="aditya@fennel.ai")
            income: int = feature(id=5)

            @extractor
            @inputs(User.id)
            @outputs(User.age)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

    assert (
        str(e.value) == "Extractors can only extract a feature defined in "
        "the same featureset, found (User.age,)."
    )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo2:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            # The users gender among male/female/non-binary
            gender: str = feature(id=3)
            age: int = feature(id=4).meta(owner="aditya@fennel.ai")
            income: int = feature(id=5)

            @extractor
            @inputs(User.id)
            @outputs(User)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

    assert (
        str(e.value)
        == "Extractor `get_user_info3` can only return a set of features, but found type <class 'fennel.featuresets.featureset.Featureset'> in output annotation."
    )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo3:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            # The users gender among male/female/non-binary
            gender: str = feature(id=3)
            age: int = feature(id=4).meta(owner="aditya@fennel.ai")
            income: int = feature(id=5)

            @extractor
            @inputs(User.id)
            @outputs(User.age, User.id)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

    assert (
        str(e.value) == "Extractors can only extract a feature defined in "
        "the same featureset, found (User.age, User.id)."
    )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo4:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            # The users gender among male/female/non-binary
            gender: str = feature(id=3)
            age: int = feature(id=4).meta(owner="aditya@fennel.ai")
            income: int = feature(id=5)

            @extractor(version="2")
            @inputs(User.id)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

    assert str(e.value) == "version for extractor must be an int."


def test_missing_id():
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo:
            userid: int = feature()
            home_geoid: int = feature(id=2)

    assert (
        str(e.value) == "feature() missing 1 required positional argument: 'id'"
    )


def test_duplicate_id():
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=1)

    assert (
        str(e.value)
        == "Feature `age` has a duplicate id `1` in featureset `UserInfo`."
    )


def test_deprecated_id():
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=3).meta(deprecated=True)
            credit_score: int = feature(id=3)

    assert (
        str(e.value) == "Feature `credit_score` has a duplicate id `3` in "
        "featureset `UserInfo`."
    )


def test_invalid_featureset():
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            extractors: List[int] = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=3).meta(deprecated=True)
            credit_score: int = feature(id=3)

    assert (
        str(e.value)
        == "Feature `extractors` in `UserInfo` has a reserved name `extractors`."
    )


@featureset
class UserInfo2:
    user_id: int = feature(id=1)
    home_geoid: int = feature(id=2)
    age: int = feature(id=3).extract(field=UserInfoDataset.age, default=0)  # type: ignore
    credit_score: int = feature(id=4)


@mock
def test_tp(client):
    client.commit(
        message="msg", datasets=[UserInfoDataset], featuresets=[UserInfo2]
    )


@mock
def test_invalid_autogenerated_extractors(client):
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo:
            user_id: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=3).extract(field=UserInfoDataset.age)
            credit_score: int = feature(id=4)

    # age should be optional[int]
    if sys.version_info < (3, 9):
        assert (
            str(e.value)
            == "Feature `UserInfo.age` has type `<class 'int'>` but expectected type `typing.Union[int, NoneType]`"
        )
    else:
        assert (
            str(e.value)
            == "Feature `UserInfo.age` has type `<class 'int'>` but expectected type `typing.Optional[int]`"
        )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo1:
            user_id: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: float = feature(id=3).extract(
                field=UserInfoDataset.age, default=0
            )
            credit_score: int = feature(id=4)

    # age should be int
    assert (
        str(e.value)
        == "Feature `UserInfo1.age` has type `<class 'float'>` but expected type `<class 'int'>`."
    )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo2:
            home_geoid: int = feature(id=2)
            age: int = feature(id=3)
            credit_score: int = feature(id=4)

        @featureset
        class UserInfo2:
            user_id: int = feature(id=1)
            home_geoid: float = feature(id=2).extract(
                feature=UserInfo2.home_geoid
            )
            age: int = feature(id=3)
            credit_score: int = feature(id=4)

    # home_geoid should be int
    assert (
        str(e.value)
        == "Feature `UserInfo2.home_geoid` has type `<class 'float'>` but the extractor aliasing `UserInfo2.home_geoid` has input type `<class 'int'>`."
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo3:
            user_id: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=4).extract(
                field=UserInfoDataset.age, default=0.0
            )
            credit_score: int = feature(id=5)

    # default value for age should be 0
    assert (
        str(e.value)
        == "Default value `0.0` for feature `UserInfo3.age` has incorrect default value: Expected type int, got <class 'float'> for value 0.0"
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo4:
            user_id: int = feature(id=1)
            home_geoid: int = feature(id=2)
            country: str = feature(id=4).extract(
                field=UserInfoDataset.country, default=0.0
            )
            credit_score: int = feature(id=5)

    # default value for age should be 0
    assert (
        str(e.value)
        == "Default value `0.0` for feature `UserInfo4.country` has incorrect default value: Expected type str, got <class 'float'> for value 0.0"
    )
