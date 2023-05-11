from datetime import datetime
from typing import Optional, List

import pandas as pd
import pytest

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.schema import inputs, outputs

# noinspection PyUnresolvedReferences
from fennel.test_lib import *


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
        == "Parameter `User` is not a feature of but a `<class 'fennel.featuresets.featureset.Featureset'>`. Please note that Featuresets are mutable and hence not supported."
    )


def test_complex_featureset():
    with pytest.raises(TypeError) as e:

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

    assert (
        str(e.value) == "Feature `gender` is extracted by multiple extractors."
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
