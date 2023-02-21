import json
import unittest
from datetime import datetime
from typing import Optional

import pytest
import requests
from google.protobuf.json_format import ParseDict  # type: ignore

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature
from fennel.lib.expectations import expectations
from fennel.lib.metadata import meta
from fennel.lib.schema import oneof
from fennel.test_lib import *


@meta(owner="test@test.com")
@dataset
class UserInfoDS:
    user_id: int = field(key=True)
    name: str = field()
    age: Optional[int]
    country: Optional[str]
    gender: oneof(str, ["male", "female"])  # type: ignore
    timestamp: datetime = field(timestamp=True)

    @expectations
    def dataset_expectations(cls, suite):
        suite.expect_column_values_to_be_between(
            column=str(cls.age), min_value=0, max_value=100
        )
        suite.expect_column_values_to_be_in_set(
            column=str(cls.gender), value_set=["male", "female"], mostly=0.9
        )
        suite.expect_column_values_to_not_be_null(
            column=str(cls.country), mostly=0.9
        )
        return suite


def test_dataset_expectation_creation(grpc_stub):
    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDS)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.dataset_requests) == 1
    d = (
        '{"data_asset_type": null, "meta": {"great_expectations_version": '
        '"0.15.46"}, "expectation_suite_name": '
        '"dataset_UserInfoDS_expectations", "expectations": [{'
        '"expectation_type": "expect_column_values_to_be_between", "kwargs": '
        '{"column": "age", "min_value": 0, "max_value": 100}, "meta": {}}, '
        '{"expectation_type": "expect_column_values_to_be_in_set", "kwargs": '
        '{"column": "gender", "value_set": ["male", "female"], "mostly": '
        '0.9}, "meta": {}}, {"expectation_type": '
        '"expect_column_values_to_not_be_null", "kwargs": {"column": '
        '"country", "mostly": 0.9}, "meta": {}}], "ge_cloud_id": null} '
    )
    expected_config = json.loads(d)
    act_config = json.loads(
        sync_request.dataset_requests[0].expectations.json_expectation_config
    )
    assert expected_config == act_config


class TestExpectationCreation(unittest.TestCase):
    @pytest.mark.integration
    @mock_client
    def test_dataset_expectation_creation_integration(self, client):
        response = client.sync(datasets=[UserInfoDS])
        assert response.status_code == requests.codes.OK, response.json()


def test_featureset_expectation_creation(grpc_stub):
    @meta(owner="test@test.com")
    @featureset
    class UserInfoFeatureset:
        user_id: int = feature(id=1)
        name: str = feature(id=2)
        age: int = feature(id=3)
        country: str = feature(id=4)
        gender: oneof(str, ["male", "female"]) = feature(id=5)

        @expectations
        def featureset_expectations(cls, suite):
            suite.expect_column_values_to_be_between(
                column=str(cls.age), min_value=0, max_value=100
            )
            suite.expect_column_values_to_be_in_set(
                column=str(cls.gender), value_set=["male", "female"], mostly=0.9
            )
            suite.expect_column_values_to_not_be_null(
                column=str(cls.country), mostly=0.9
            )
            return suite

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoFeatureset)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.featureset_requests) == 1
    d = (
        '{"data_asset_type": null, "meta": {"great_expectations_version": '
        '"0.15.46"}, "expectation_suite_name": '
        '"featureset_UserInfoFeatureset_expectations", "expectations": [{'
        '"expectation_type": "expect_column_values_to_be_between", "kwargs": '
        '{"column": "age", "min_value": 0, "max_value": 100}, "meta": {}}, '
        '{"expectation_type": "expect_column_values_to_be_in_set", "kwargs": '
        '{"column": "gender", "value_set": ["male", "female"], "mostly": '
        '0.9}, "meta": {}}, {"expectation_type": '
        '"expect_column_values_to_not_be_null", "kwargs": {"column": '
        '"country", "mostly": 0.9}, "meta": {}}], "ge_cloud_id": null} '
    )
    expected_config = json.loads(d)
    act_config = json.loads(
        sync_request.featureset_requests[0].expectations.json_expectation_config
    )
    assert expected_config == act_config


def test_dataset_invalid_expectation_creation():
    with pytest.raises(Exception) as e:
        @meta(owner="test@test.com")
        @dataset
        class UserInfoDSInvalid:
            user_id: int = field(key=True)
            name: str = field()
            age: Optional[int]
            country: Optional[str]
            gender: oneof(str, ["male", "female"])
            timestamp: datetime = field(timestamp=True)

            @expectations
            def dataset_expectations(cls, suite):
                suite.expect_column_values_to_be_between_random(
                    column=str(cls.age), min_value=0, max_value=100
                )
                suite.expect_column_values_to_be_in_set(
                    column=str(cls.gender),
                    value_set=["male", "female"],
                    mostly=0.9,
                )
                suite.expect_column_values_to_not_be_null(
                    column=str(cls.country), mostly=0.9
                )
                return suite

    assert (
            str(e.value)
            == "expect_column_values_to_be_between_random not found"
    )

    with pytest.raises(Exception) as e:
        @meta(owner="test@test.com")
        @dataset
        class UserInfoDSInvalid2:
            user_id: int = field(key=True)
            name: str = field()
            age: Optional[int]
            country: Optional[str]
            gender: oneof(str, ["male", "female"])
            timestamp: datetime = field(timestamp=True)

            @expectations
            def dataset_expectations(cls, suite):
                suite.expect_column_values_to_be_between(
                    column=str(cls.age), minimum_value=0, max_value=100
                )
                suite.expect_column_values_to_be_in_set(
                    column=str(cls.gender),
                    value_set=["male", "female"],
                    mostly=0.9,
                )
                suite.expect_column_values_to_not_be_null(
                    column=str(cls.country), mostly=0.9
                )
                return suite

    assert (
            str(e.value)
            == "expect_column_values_to_be_between_random not found"
    )
