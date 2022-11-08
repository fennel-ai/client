from datetime import datetime
from typing import Optional, Tuple

import pandas as pd
import pytest

from fennel.dataset import dataset
from fennel.featureset import featureset, extractor, depends_on
from fennel.lib.field import field
# noinspection PyUnresolvedReferences
from fennel.test_lib import *


# Don't remove this line. It's used by the test runner to find the tests.


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
    id: int
    age: float


def test_ComplexFeatureSet(grpc_stub):
    with pytest.raises(TypeError) as e:
        @featureset
        class UserInfo:
            userid: int
            home_geoid: int

        @featureset
        class UserInfo:
            userid: int
            home_geoid: int
            # The users gender among male/female/non-binary
            gender: str
            age: int = field(owner="aditya@fennel.ai")
            income: int

            @extractor
            @depends_on(UserInfoDataset)
            def get_user_info1(ts: pd.Series, user_id: User.id) -> Tuple[
                "userid", "home_geoid"]:
                pass

            @extractor
            @depends_on(UserInfoDataset)
            def get_user_info2(ts: pd.Series, user_id: User.id) -> Tuple[
                "gender", "age"]:
                pass

            @extractor
            def get_user_info3(ts: pd.Series, user_id: User.id) -> Tuple[
                "gender"]:
                pass
    assert str(e.value) == "Feature gender is extracted by multiple extractors"
