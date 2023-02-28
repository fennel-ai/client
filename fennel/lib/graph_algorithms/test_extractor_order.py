from datetime import datetime

import pandas as pd

from fennel.featuresets import featureset, extractor, feature
from fennel.lib.graph_algorithms import get_extractor_order
from fennel.lib.schema import Series, DataFrame


@featureset
class A:
    a1: int = feature(id=1)
    a2: int = feature(id=2)
    root: int = feature(id=3)

    @extractor
    def a1_a2(
        cls, ts: Series[datetime], root: Series[root]
    ) -> DataFrame[a1, a2]:
        pass


@featureset
class B:
    b1: int = feature(id=1)
    b2: int = feature(id=2)

    @extractor
    def b1_b2(
        cls, ts: Series[datetime], a1: Series[A.a1], a2: Series[A.a2]
    ) -> DataFrame[b1, b2]:
        pass


def test_simple_extractor_path():
    extractors = A.extractors + B.extractors
    extractors = get_extractor_order([A.root], [B.b1, B.b2], extractors)
    extractors_to_run = [e.name for e in extractors]
    assert extractors_to_run == ["A.a1_a2", "B.b1_b2"]

    # If A.a1 is part of input we should still run A.a1_a2 to get A.a2
    extractors = A.extractors + B.extractors
    extractors = get_extractor_order([A.root, A.a1], [B.b1, B.b2], extractors)
    extractors_to_run = [e.name for e in extractors]
    assert extractors_to_run == ["A.a1_a2", "B.b1_b2"]

    # If A.a1 & A.a2 is part of input we should only run B.b1_b2
    extractors = A.extractors + B.extractors
    extractors = get_extractor_order(
        [A.root, A.a1, A.a2], [B.b1, B.b2], extractors
    )
    extractors_to_run = [e.name for e in extractors]
    assert extractors_to_run == ["B.b1_b2"]


@featureset
class C:
    c1: int = feature(id=1)
    c2: int = feature(id=2)
    c3: int = feature(id=3)
    c4: int = feature(id=4)

    @extractor
    def c1_from_root(
        cls, ts: Series[datetime], a1: Series[A.root]
    ) -> Series[c1]:
        pass

    @extractor
    def from_c1(
        cls, ts: Series[datetime], c1: Series[c1]
    ) -> DataFrame[c2, c3, c4]:
        pass


def test_complex_extractor_path():
    extractors = A.extractors + B.extractors + C.extractors
    extractors = get_extractor_order([A.root], [B.b1, B.b2, C.c2], extractors)
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 4
    assert extractors_to_run == [
        "C.c1_from_root",
        "C.from_c1",
        "A.a1_a2",
        "B.b1_b2",
    ]

    extractors = A.extractors + B.extractors + C.extractors
    extractors = get_extractor_order(
        [A.root, C.c1], [B.b1, B.b2, C.c2], extractors
    )
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 3
    assert extractors_to_run == ["C.from_c1", "A.a1_a2", "B.b1_b2"]


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
    def get_user_age_and_name(
        cls, ts: Series[datetime], user_id: Series[userid]
    ) -> DataFrame[age, name]:
        pass

    @extractor
    def get_age_and_name_features(
        cls, ts: Series[datetime], user_age: Series[age], name: Series[name]
    ) -> DataFrame[age_squared, age_cubed, is_name_common]:
        pass

    @extractor
    def get_country_geoid(
        cls, ts: Series[datetime], user_id: Series[userid]
    ) -> Series[country_geoid]:
        pass


def test_age_feature_extraction():
    extractors = get_extractor_order(
        [UserInfo.userid], [UserInfo], UserInfo.extractors
    )
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 3
    assert set(extractors_to_run[0:2]) == set(
        {"UserInfo.get_user_age_and_name", "UserInfo.get_country_geoid"}
    )
    assert extractors_to_run[2] == "UserInfo.get_age_and_name_features"


@featureset
class UserInfoTransformedFeatures:
    age_power_four: int = feature(id=1)
    is_name_common: bool = feature(id=2)

    @extractor
    @extractor
    def get_user_transformed_features(
        cls,
        ts: Series[datetime],
        user_features: DataFrame[UserInfo.age, UserInfo.is_name_common],
    ):
        age = user_features[repr(UserInfo.age)]
        is_name_common = user_features[repr(UserInfo.is_name_common)]
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
            DataFrame[
                UserInfoTransformedFeatures.age_power_four,
                UserInfoTransformedFeatures.is_name_common,
            ]
        ],
        UserInfo.extractors + UserInfoTransformedFeatures.extractors,
    )
    extractors_to_run = [e.name for e in extractors]
    assert len(extractors_to_run) == 3
    assert extractors_to_run[0] == "UserInfo.get_user_age_and_name"
    assert extractors_to_run[1] == "UserInfo.get_age_and_name_features"
    assert (
        extractors_to_run[2]
        == "UserInfoTransformedFeatures.get_user_transformed_features"
    )
