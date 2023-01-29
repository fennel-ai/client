from datetime import datetime
from typing import Optional, List

import pytest

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, depends_on, feature
from fennel.lib.schema import Series, DataFrame

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


def test_complex_featureset(grpc_stub):
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
            @depends_on(UserInfoDataset)
            def get_user_info1(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ) -> DataFrame[userid, home_geoid]:
                pass

            @extractor
            @depends_on(UserInfoDataset)
            def get_user_info2(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ) -> DataFrame[gender, age]:
                pass

            @extractor
            def get_user_info3(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ) -> Series[gender]:
                pass

    assert (
        str(e.value) == "Feature UserInfo.gender is extracted by multiple "
        "extractors"
    )


def test_extract_anoather_featureset(grpc_stub):
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
            def get_user_info3(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ) -> Series[User.age]:
                pass

    assert (
        str(e.value) == "Extractors can only extract a feature defined in "
        "the same featureset, found User.age"
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
            def get_user_info3(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ) -> DataFrame[User]:
                pass

    assert (
        str(e.value) == "Extractors can only return a Series[feature] or "
        "a DataFrame[<list of features defined in this "
        "Featureset>]."
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
            def get_user_info3(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ) -> DataFrame[User.age, User.id]:
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
            def get_user_info3(
                cls, ts: Series[datetime], user_id: Series[User.id]
            ):
                pass

    assert str(e.value) == "version for extractor must be an int."


def test_missing_id(grpc_stub):
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo:
            userid: int = feature()
            home_geoid: int = feature(id=2)

    assert (
        str(e.value) == "feature() missing 1 required positional argument: 'id'"
    )


def test_duplicate_id(grpc_stub):
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=1)

    assert str(e.value) == "Feature age has a duplicate id 1"


def test_deprecated_id(grpc_stub):
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            userid: int = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=3).meta(deprecated=True)
            credit_score: int = feature(id=3)

    assert str(e.value) == "Feature credit_score has a duplicate id 3"


def test_invalid_featureset(grpc_stub):
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            extractors: List[int] = feature(id=1)
            home_geoid: int = feature(id=2)
            age: int = feature(id=3).meta(deprecated=True)
            credit_score: int = feature(id=3)

    assert (
        str(e.value)
        == "Feature extractors in UserInfo has a reserved name extractors."
    )
