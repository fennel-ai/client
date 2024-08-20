import sys
from datetime import datetime
from typing import Optional, List

import pandas as pd
import pytest

from fennel import meta
from fennel.connectors import source, Webhook
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature as F
from fennel.lib import inputs, outputs
from fennel.expr import col

# noinspection PyUnresolvedReferences
from fennel.testing import *

__owner__ = "data@fennel.ai"
webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
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
    id: int
    age: float


def test_featureset_as_input():
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfoInvalid:
            userid: int
            home_geoid: int

            @extractor(deps=[UserInfoDataset])
            @inputs(User)
            @outputs("userid", "home_geoid")
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
            userid: int
            home_geoid: int
            # The users gender among male/female/non-binary
            gender: str
            age: int = F().meta(owner="aditya@fennel.ai")
            income: int

            @extractor(deps=[UserInfoDataset])
            @inputs(User.id)
            @outputs("userid", "home_geoid")
            def get_user_info1(cls, ts: pd.Series, user_id: pd.Series):
                pass

            @extractor(deps=[UserInfoDataset])
            @inputs(User.id)
            @outputs("gender", "age")
            def get_user_info2(cls, ts: pd.Series, user_id: pd.Series):
                pass

            @extractor
            @inputs(User.id)
            @outputs("gender")
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
            userid: int
            home_geoid: int
            # The users gender among male/female/non-binary
            gender: str
            age: int = F().meta(owner="aditya@fennel.ai")
            income: int

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
            userid: int
            home_geoid: int
            # The users gender among male/female/non-binary
            gender: str
            age: int = F().meta(owner="aditya@fennel.ai")
            income: int

            @extractor
            @inputs(User.id)
            @outputs(User)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

    assert (
        str(e.value)
        == "Extractor `get_user_info3` can only return a set of features or strings, but found type <class 'fennel.featuresets.featureset.Featureset'> in output annotation."
    )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo3:
            userid: int
            home_geoid: int
            # The users gender among male/female/non-binary
            gender: str
            age: int = F().meta(owner="aditya@fennel.ai")
            income: int

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
            userid: int
            home_geoid: int
            # The users gender among male/female/non-binary
            gender: str
            age: int = F().meta(owner="aditya@fennel.ai")
            income: int

            @extractor(version="2")
            @inputs(User.id)
            def get_user_info3(cls, ts: pd.Series, user_id: pd.Series):
                pass

    assert str(e.value) == "version for extractor must be an int."


def test_invalid_featureset():
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo:
            extractors: List[int]
            home_geoid: int
            age: int = F().meta(deprecated=True)
            credit_score: int

    assert (
        str(e.value)
        == "Feature `extractors` in `UserInfo` has a reserved name `extractors`."
    )


@featureset
class UserInfo2:
    user_id: int
    home_geoid: int
    age: int = F(UserInfoDataset.age, default=0)  # type: ignore
    credit_score: int


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
            user_id: int
            home_geoid: int
            age: int = F(UserInfoDataset.age)
            credit_score: int

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
            user_id: int
            home_geoid: int
            age: float = F(UserInfoDataset.age, default=0)
            credit_score: int

    # age should be int
    assert (
        str(e.value)
        == "Feature `UserInfo1.age` has type `<class 'float'>` but expected type `<class 'int'>`."
    )

    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo2:
            home_geoid: int
            age: int
            credit_score: int

        @featureset
        class UserInfo2:
            user_id: int
            home_geoid: float = F(UserInfo2.home_geoid)
            age: int
            credit_score: int

    # home_geoid should be int
    assert (
        str(e.value)
        == "Feature `UserInfo2.home_geoid` has type `<class 'float'>` but the extractor aliasing `UserInfo2.home_geoid` has input type `<class 'int'>`."
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo3:
            user_id: int
            home_geoid: int
            age: int = F(UserInfoDataset.age, default=0.0)
            credit_score: int

    # default value for age should be 0
    assert (
        str(e.value)
        == "Default value `0.0` for feature `UserInfo3.age` has incorrect default value: Expected type int, got <class 'float'> for value 0.0"
    )

    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo4:
            user_id: int
            home_geoid: int
            country: str = F(UserInfoDataset.country, default=0.0)
            credit_score: int

    # default value for age should be 0
    assert (
        str(e.value)
        == "Default value `0.0` for feature `UserInfo4.country` has incorrect default value: Expected type str, got <class 'float'> for value 0.0"
    )


@mock
def test_invalid_extractors(client):
    with pytest.raises(ValueError) as e:
        """
        Testing failure when choose an invalid string in inputs of an extractor
        """

        @featureset
        class Featureset1:
            user_id: int
            home_geoid: int
            country: str
            credit_score: int

            @extractor
            @inputs("user_ids")
            @outputs("home_geoid")
            def my_extractor(cls, ts: pd.Series, user_ids: pd.Series):
                return pd.Series([0] * user_ids.shape[0], name="home_geoid")

    # default value for age should be 0
    assert (
        str(e.value)
        == "When using strings in 'inputs' for an extractor, one can only choose from "
        "the features defined in the current featureset. Please choose an input from : "
        "['user_id', 'home_geoid', 'country', 'credit_score'] found : `user_ids` in "
        "extractor : `my_extractor`."
    )

    with pytest.raises(ValueError) as e:
        """
        Testing failure when choose an invalid string in outputs of an extractor
        """

        @featureset
        class Featureset2:
            user_id: int
            home_geoid: int
            country: str
            credit_score: int

            @extractor
            @inputs("user_ids")
            @outputs("home_geoids")
            def my_extractor(cls, ts: pd.Series, user_ids: pd.Series):
                return pd.Series([0] * user_ids.shape[0], name="home_geoid")

    # default value for age should be 0
    assert (
        str(e.value)
        == "When using strings in 'outputs' for an extractor, one can only choose from "
        "the features defined in the current featureset. Please choose an output from : "
        "['user_id', 'home_geoid', 'country', 'credit_score'] found : `home_geoids` in "
        "extractor : `my_extractor`."
    )


@mock
def test_invalid_alias_feature(client):
    @featureset
    class A:
        user_id: Optional[int]
        home_geoid: int
        credit_score: int

    with pytest.raises(TypeError) as e:

        @featureset
        class B:
            user_id: int = F(A.user_id, default=0)
            home_geoid: int
            credit_score: int

    assert (
        str(e.value)
        == "'Please specify a reference to a field of a dataset to use \"default\" param', found arg: `user_id` and default: `0`"
    )


@mock
def test_invalid_expr_feature(client):

    # Using a feature that is not defined in the featureset
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo3:
            user_id: int
            home_geoid: int
            age: int = F(UserInfoDataset.age, default=0)
            age_squared: int = F(col("Age") * col("Age"))
            credit_score: int

    assert (
        str(e.value)
        == "extractor for 'age_squared' refers to feature col('Age') not present in 'UserInfo3'; 'col' can only reference features from the same featureset"
    )

    # Using default value for an expression feature
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo4:
            user_id: int
            home_geoid: int
            age: int = F(UserInfoDataset.age, default=0)
            age_squared: int = F(col("age") * col("age"), default=0)
            credit_score: int

    assert (
        str(e.value)
        == "error in expression based extractor 'col('age') * col('age')'; can not set default value for expressions, maybe use fillnull instead?"
    )

    # Incorrect type for an expression feature
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo5:
            user_id: int
            home_geoid: int
            age: int = F(UserInfoDataset.age, default=0)
            age_squared: str = F(col("age") * col("age"))
            credit_score: int

    assert (
        str(e.value)
        == "expression 'col('age') * col('age')' for feature 'age_squared' is of type 'str' not 'int'"
    )

    # Using dataset field in expression feature
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo6:
            user_id: int
            home_geoid: int
            age: int = F(UserInfoDataset.age, default=0)
            age_squared: int = F(
                col("UserInfoDataset.age") * col("UserInfoDataset.age")
            )
            credit_score: int

    assert (
        str(e.value)
        == "extractor for 'age_squared' refers to feature col('UserInfoDataset.age') not present in 'UserInfo6'; 'col' can only reference features from the same featureset"
    )
