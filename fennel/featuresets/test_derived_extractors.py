import json
import pandas as pd
import pytest
from datetime import datetime
from google.protobuf.json_format import ParseDict  # type: ignore
from typing import Optional

import fennel.gen.featureset_pb2 as fs_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs, struct
from fennel.sources import source, Webhook

from fennel.test_lib import *

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
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


def test_valid_derived_extractors():
    @struct
    class AgeGroup:
        young: bool
        senior: bool
        group: str

    @meta(owner="test@test.com")
    @featureset
    class UserInfo:
        # alias feature
        user_id: int = feature(id=1).extract(feature=User.id)
        # lookup derived feature
        gender: str = feature(id=2).extract(
            field=UserInfoDataset.gender,
            default="unspecified",
            depends_on=[UserInfoDataset],
        )
        # lookup with meta
        age_years: float = (
            feature(id=3)
            .extract(
                field=UserInfoDataset.age,
                default=0,
                depends_on=[UserInfoDataset],
            )
            .meta(owner="zaki@fennel.ai")
        )
        # deprecated feature
        dob: str = (
            feature(id=4)
            .extract(
                field=UserInfoDataset.dob,
                default="unspecified",
                depends_on=[UserInfoDataset],
            )
            .meta(deprecated=True)
        )
        # depends on derived feature
        age_group: AgeGroup = feature(id=5)

        @extractor(depends_on=[UserInfoDataset])
        @inputs(age_years)
        @outputs(age_group)
        def get_age_group(cls, ts: pd.Series, age: pd.Series):
            def age_to_group(x):
                if x < 18:
                    return AgeGroup(young=True, senior=False, group="young")
                if x > 65:
                    return AgeGroup(young=False, senior=True, group="senior")
                return AgeGroup(young=False, senior=False, group="adult")

            return age.map(lambda x: age_to_group(x))

    @meta(owner="test@test.com")
    @featureset
    class AgeInfo:
        # alias a feature that has an explicit extractor
        age_group: AgeGroup = feature(id=1).extract(feature=UserInfo.age_group)
        # alias a feature that has a derived extractor
        age: float = feature(id=2).extract(feature=UserInfo.age_years)

    view = InternalTestClient()
    view.add(UserInfoDataset)
    view.add(UserInfo)
    view.add(AgeInfo)
    view.add(User)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.feature_sets) == 3
    assert len(sync_request.extractors) == 7
    assert len(sync_request.features) == 9

    # featuresets
    def test_fs(fs_request, expected_dict):
        expected_fs_request = ParseDict(
            expected_dict, fs_proto.CoreFeatureset()
        )
        # Clear the pycode field in featureset_request as it is not deterministic
        fs_request.pycode.Clear()
        assert fs_request == expected_fs_request, error_message(
            fs_request, expected_fs_request
        )

    f = {
        "name": "UserInfo",
        "metadata": {"owner": "test@test.com"},
        "pycode": {"source_code": ""},
    }
    test_fs(sync_request.feature_sets[0], f)
    f["name"] = "AgeInfo"
    test_fs(sync_request.feature_sets[1], f)

    # features
    def test_feature(actual_feature, expected_dict):
        expected_feature = ParseDict(expected_dict, fs_proto.Feature())
        assert actual_feature == expected_feature, error_message(
            actual_feature, expected_feature
        )

    age_group_struct_type = {
        "name": "AgeGroup",
        "fields": [
            {"name": "young", "dtype": {"bool_type": {}}},
            {"name": "senior", "dtype": {"bool_type": {}}},
            {"name": "group", "dtype": {"string_type": {}}},
        ],
    }

    # The comments above the feature declarations are captured as metadata
    expected_features = [
        {
            "id": 1,
            "name": "user_id",
            "dtype": {"int_type": {}},
            "metadata": {"description": "alias feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 2,
            "name": "gender",
            "dtype": {"string_type": {}},
            "metadata": {"description": "lookup derived feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 3,
            "name": "age_years",
            "dtype": {"double_type": {}},
            "metadata": {
                "owner": "zaki@fennel.ai",
                "description": "lookup with meta",
            },
            "feature_set_name": "UserInfo",
        },
        {
            "id": 4,
            "name": "dob",
            "dtype": {"string_type": {}},
            "metadata": {
                "deprecated": True,
                "description": "deprecated feature",
            },
            "feature_set_name": "UserInfo",
        },
        {
            "id": 5,
            "name": "age_group",
            "dtype": {"struct_type": age_group_struct_type},
            "metadata": {"description": "depends on derived feature"},
            "feature_set_name": "UserInfo",
        },
        {
            "id": 1,
            "name": "age_group",
            "dtype": {"struct_type": age_group_struct_type},
            "metadata": {
                "description": "alias a feature that has an explicit extractor"
            },
            "feature_set_name": "AgeInfo",
        },
        {
            "id": 2,
            "name": "age",
            "dtype": {"double_type": {}},
            "metadata": {
                "description": "alias a feature that has a derived extractor"
            },
            "feature_set_name": "AgeInfo",
        },
        {
            "id": 1,
            "name": "id",
            "dtype": {"int_type": {}},
            "metadata": {},
            "feature_set_name": "User",
        },
        {
            "id": 2,
            "name": "age",
            "dtype": {"double_type": {}},
            "metadata": {},
            "feature_set_name": "User",
        },
    ]

    for i, f in enumerate(sync_request.features):
        test_feature(f, expected_features[i])

    def test_extractor(actual, expected):
        if actual.extractor_type == fs_proto.PY_FUNC:
            actual = erase_extractor_pycode(actual)
        expected_extractor = ParseDict(expected, fs_proto.Extractor())
        assert actual == expected_extractor, error_message(
            actual, expected_extractor
        )

    expected_extractors = [
        {
            "name": "alias_id",
            "datasets": [],
            "inputs": [{"feature": {"feature_set_name": "User", "name": "id"}}],
            "features": ["user_id"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.ALIAS,
            "dataset_info": None,
        },
        {
            "name": "lookup_gender",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["gender"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "dataset_info": {
                "field": {"name": "gender", "dtype": {"string_type": {}}},
                "defaultValue": json.dumps("unspecified"),
            },
        },
        {
            "name": "lookup_age",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["age_years"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "dataset_info": {
                "field": {"name": "age", "dtype": {"int_type": {}}},
                "defaultValue": json.dumps(0),
            },
        },
        {
            "name": "lookup_dob",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {"feature": {"feature_set_name": "UserInfo", "name": "user_id"}}
            ],
            "features": ["dob"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.LOOKUP,
            "dataset_info": {
                "field": {"name": "dob", "dtype": {"string_type": {}}},
                "defaultValue": json.dumps("unspecified"),
            },
        },
        {
            "name": "get_age_group",
            "datasets": ["UserInfoDataset"],
            "inputs": [
                {
                    "feature": {
                        "feature_set_name": "UserInfo",
                        "name": "age_years",
                    }
                }
            ],
            "features": ["age_group"],
            "metadata": {},
            "version": 0,
            "pycode": {"source_code": ""},
            "feature_set_name": "UserInfo",
            "extractor_type": fs_proto.PY_FUNC,
            "dataset_info": None,
        },
        {
            "name": "alias_age_group",
            "datasets": [],
            "inputs": [
                {
                    "feature": {
                        "feature_set_name": "UserInfo",
                        "name": "age_group",
                    }
                }
            ],
            "features": ["age_group"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "AgeInfo",
            "extractor_type": fs_proto.ALIAS,
            "dataset_info": None,
        },
        {
            "name": "alias_age_years",
            "datasets": [],
            "inputs": [
                {
                    "feature": {
                        "feature_set_name": "UserInfo",
                        "name": "age_years",
                    }
                }
            ],
            "features": ["age"],
            "metadata": {},
            "version": 0,
            "pycode": None,
            "feature_set_name": "AgeInfo",
            "extractor_type": fs_proto.ALIAS,
            "dataset_info": None,
        },
    ]
    for i, e in enumerate(sync_request.extractors):
        test_extractor(e, expected_extractors[i])


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
                    depends_on=[UserInfoDataset],
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
                    depends_on=[UserInfoDataset],
                )
                .meta(owner="zaki@fennel.ai")
                .extract(feature=User.age)
            )

    assert str(e.value) == "extract() can only be called once for feature id=2"

    # Tests a derived and manual extractor for the same feature
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo3:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                default=0,
                depends_on=[UserInfoDataset],
            )

            @extractor(depends_on=[UserInfoDataset])
            @inputs(user_id)
            @outputs(age)
            def get_age(cls, ts: pd.Series, user_id: pd.Series):
                df = UserInfoDataset.lookup(ts, user_id=user_id) # type: ignore
                return df.fillna(0)

    assert str(e.value) == "Feature `age` is extracted by multiple extractors."


def test_invalid_missing_fields():
    @meta(owner="test@test.com")
    @dataset
    class AnotherDataset:
        foo: int = field(key=True)
        ts: datetime = field(timestamp=True)

    # no field nor feature
    with pytest.raises(TypeError) as e:

        @featureset
        class UserInfo4:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = feature(id=2).extract(
                default=0, depends_on=[UserInfoDataset]
            )

    assert (
        str(e.value)
        == "Either field or feature must be specified to extract feature id=2"
    )

    # missing dataset in depends_on
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo5:
            user_id: int = feature(id=1).extract(feature=User.id)
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                default=0,
                depends_on=[AnotherDataset],
            )

    assert (
        str(e.value)
        == "Dataset UserInfoDataset not found in depends_on list for extractor lookup_age"
    )

    # missing dataset key in current featureset
    with pytest.raises(ValueError) as e:

        @featureset
        class UserInfo6:
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                default=0,
                depends_on=[UserInfoDataset],
            )

    assert (
        str(e.value)
        == "Dataset key user_id not found in provider UserInfo6 for extractor lookup_age"
    )

    # missing dataset in depends_on, with input provider
    with pytest.raises(ValueError) as e:

        @featureset
        class UserRequest:
            name: str = feature(id=1)

        @featureset
        class UserInfo7:
            age: int = feature(id=2).extract(
                field=UserInfoDataset.age,
                provider=UserRequest,
                default=0,
                depends_on=[AnotherDataset],
            )

    assert (
        str(e.value)
        == "Dataset UserInfoDataset not found in depends_on list for extractor lookup_age"
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
                depends_on=[UserInfoDataset],
            )

    assert (
        str(e.value)
        == "Dataset key user_id not found in provider UserRequest for extractor lookup_age"
    )
