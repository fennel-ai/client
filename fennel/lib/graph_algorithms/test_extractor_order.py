import pandas as pd

from fennel.featuresets import featureset, extractor, feature
from fennel.lib.graph_algorithms import get_extractor_order
from fennel.lib.schema import inputs, outputs


@featureset
class A:
    a1: int = feature(id=1)
    a2: int = feature(id=2)
    root: int = feature(id=3)

    @extractor
    @inputs(root)
    def a1_a2(cls, ts: pd.Series, root: pd.Series):
        pass


@featureset
class B:
    b1: int = feature(id=1)
    b2: int = feature(id=2)

    @extractor
    @inputs(A.a1, A.a2)
    def b1_b2(cls, ts: pd.Series, a1: pd.Series, a2: pd.Series):
        pass


def test_simple_extractor_path():
    extractors = A.extractors + B.extractors
    extractors = get_extractor_order([A.root], [B.b1, B.b2], extractors)
    extractors_to_run = [e.name for e in extractors]
    assert extractors_to_run == ["a1_a2", "b1_b2"]

    # If A.a1 is part of input we should still run A.a1_a2 to get A.a2
    extractors = A.extractors + B.extractors
    extractors = get_extractor_order([A.root, A.a1], [B.b1, B.b2], extractors)
    extractors_to_run = [e.name for e in extractors]
    assert extractors_to_run == ["a1_a2", "b1_b2"]

    # If A.a1 & A.a2 is part of input we should only run B.b1_b2
    extractors = A.extractors + B.extractors
    extractors = get_extractor_order(
        [A.root, A.a1, A.a2], [B.b1, B.b2], extractors
    )
    extractors_to_run = [e.name for e in extractors]
    assert extractors_to_run == ["b1_b2"]


@featureset
class C:
    c1: int = feature(id=1)
    c2: int = feature(id=2)
    c3: int = feature(id=3)
    c4: int = feature(id=4)

    @extractor
    @inputs(A.root)
    @outputs(c1)
    def c1_from_root(cls, ts: pd.Series, a1: pd.Series):
        pass

    @extractor
    @inputs(c1)
    @outputs(c2, c3, c4)
    def from_c1(cls, ts: pd.Series, c1: pd.Series):
        pass


def test_complex_extractor_path():
    extractors = A.extractors + B.extractors + C.extractors
    extractors = get_extractor_order([A.root], [B.b1, B.b2, C.c2], extractors)
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 4
    assert extractors_to_run == [
        "a1_a2",
        "c1_from_root",
        "from_c1",
        "b1_b2",
    ]

    extractors = A.extractors + B.extractors + C.extractors
    extractors = get_extractor_order(
        [A.root, C.c1], [B.b1, B.b2, C.c2], extractors
    )
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 3
    assert extractors_to_run == ["a1_a2", "from_c1", "b1_b2"]


@featureset
class UserInfo:
    userid: int = feature(id=1)
    name: str = feature(id=2)
    country_geoid: int = feature(id=3)
    # The users gender among male/female/non-binary
    age: int = feature(id=4).meta(owner="aditya@fennel.ai")  # type: ignore
    age_squared: int = feature(id=5)
    age_cubed: int = feature(id=6)
    is_name_common: bool = feature(id=7)

    @extractor
    @inputs(userid)
    @outputs(age, name)
    def get_user_age_and_name(cls, ts: pd.Series, user_id: pd.Series):
        pass

    @extractor
    @inputs(age, name)
    @outputs(age_squared, age_cubed, is_name_common)
    def get_age_and_name_features(
        cls, ts: pd.Series, user_age: pd.Series, name: pd.Series
    ):
        pass

    @extractor
    @inputs(userid)
    @outputs(country_geoid)
    def get_country_geoid(cls, ts: pd.Series, user_id: pd.Series):
        pass


def test_age_feature_extraction():
    extractors = get_extractor_order(
        [UserInfo.userid], [UserInfo], UserInfo.extractors
    )
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 3
    assert set(extractors_to_run[0:2]) == set(
        {"get_user_age_and_name", "get_country_geoid"}
    )
    assert extractors_to_run[2] == "get_age_and_name_features"


@featureset
class UserInfoTransformedFeatures:
    age_power_four: int = feature(id=1)
    is_name_common: bool = feature(id=2)

    @extractor
    @inputs(UserInfo.age, UserInfo.is_name_common)
    def get_user_transformed_features(
        cls, ts: pd.Series, age: pd.Series, is_name_common: pd.Series
    ):
        age_power_four = age**4
        return pd.DataFrame(
            {
                "age_power_four": age_power_four,
                "is_name_common": is_name_common,
            }
        )


def test_age_feature_extraction_complex():
    extractors = get_extractor_order(
        [UserInfo.userid],
        [
            UserInfoTransformedFeatures.age_power_four,
            UserInfoTransformedFeatures.is_name_common,
        ],
        UserInfo.extractors + UserInfoTransformedFeatures.extractors,
    )
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 3
    assert extractors_to_run[0] == "get_user_age_and_name"
    assert extractors_to_run[1] == "get_age_and_name_features"
    assert extractors_to_run[2] == "get_user_transformed_features"
