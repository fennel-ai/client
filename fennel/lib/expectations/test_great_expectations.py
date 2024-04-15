import unittest
from datetime import datetime
from typing import Optional

import pytest
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel._vendor.requests as requests
import fennel.gen.expectations_pb2 as exp_proto
from fennel.datasets import dataset, field
from fennel.lib import (
    meta,
    expectations,
    expect_column_values_to_be_between,
    expect_column_values_to_be_in_set,
    expect_column_values_to_not_be_null,
    expect_column_pair_values_A_to_be_greater_than_B,
)
from fennel.dtypes import oneof
from fennel.connectors import source, Webhook
from fennel.testing import *

webhook = Webhook(name="fennel_webhook")


def test_dataset_expectation_creation():
    @meta(owner="test@test.com")
    @source(webhook.endpoint("UserInfoDS"), disorder="14d", cdc="upsert")
    @dataset
    class UserInfoDS:
        user_id: int = field(key=True)
        name: str = field()
        age: Optional[int]
        country: Optional[str]
        gender: oneof(str, ["male", "female"])  # type: ignore
        timestamp: datetime = field(timestamp=True)

        @expectations(version=34)
        def dataset_expectations(cls):
            return [
                expect_column_values_to_be_between(
                    column=str(cls.age), min_value=0, max_value=100
                ),
                expect_column_values_to_be_in_set(
                    column=str(cls.gender),
                    value_set=["male", "female"],
                    mostly=0.9,
                ),
                expect_column_values_to_not_be_null(
                    column=str(cls.country), mostly=0.9
                ),
                expect_column_pair_values_A_to_be_greater_than_B(
                    column_A=str(cls.age), column_B=str(cls.user_id)
                ),
            ]

    view = InternalTestClient()
    view.add(UserInfoDS)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    d = {
        "entityName": "UserInfoDS",
        "suite": "dataset_UserInfoDS_expectations",
        "version": 34,
        "expectations": [
            {
                "expectationType": "expect_column_values_to_be_between",
                "expectationKwargs": '{"column": "age", "min_value": 0, "max_value": 100, "strict_min": false, "strict_max": false, "parse_strings_as_datetimes": false, "include_config": true}',
            },
            {
                "expectationType": "expect_column_values_to_be_in_set",
                "expectationKwargs": '{"column": "gender", "value_set": ["male", "female"], "mostly": 0.9, "include_config": true}',
            },
            {
                "expectationType": "expect_column_values_to_not_be_null",
                "expectationKwargs": '{"column": "country", "mostly": 0.9, "include_config": true}',
            },
            {
                "expectationType": "expect_column_pair_values_a_to_be_greater_than_b",
                "expectationKwargs": '{"column_A": "age", "column_B": "user_id", "parse_strings_as_datetimes": false, "ignore_row_if": "both_values_are_missing", "include_config": true}',
            },
        ],
        "entity_version": 1,
    }
    expected_exp_request = ParseDict(d, exp_proto.Expectations())
    act_config = sync_request.expectations[0]
    assert act_config == expected_exp_request, error_message(
        act_config, expected_exp_request
    )


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDS"), disorder="14d", cdc="upsert")
@dataset
class UserInfoDS:
    user_id: int = field(key=True)
    name: str = field()
    age: Optional[int]
    country: Optional[str]
    gender: oneof(str, ["male", "female"])  # type: ignore
    timestamp: datetime = field(timestamp=True)

    @expectations
    def dataset_expectations(cls):
        return [
            expect_column_values_to_be_between(
                column=str(cls.age), min_value=0, max_value=100
            ),
            expect_column_values_to_be_in_set(
                column=str(cls.gender), value_set=["male", "female"], mostly=0.9
            ),
            expect_column_values_to_not_be_null(
                column=str(cls.country), mostly=0.9
            ),
        ]


class TestExpectationCreation(unittest.TestCase):
    @pytest.mark.integration
    @mock
    def test_dataset_expectation_creation_integration(self, client):
        response = client.commit(message="mg", datasets=[UserInfoDS])
        assert response.status_code == requests.codes.OK, response.json()


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
            def dataset_expectations(cls):
                return [
                    expect_column_values_to_be_between_random(
                        column=str(cls.age), min_value=0, max_value=100
                    ),
                    expect_column_values_to_be_in_set(
                        column=str(cls.gender),
                        value_set=["male", "female"],
                        mostly=0.9,
                    ),
                    expect_column_values_to_not_be_null(
                        column=str(cls.country), mostly=0.9
                    ),
                ]

    assert (
        str(e.value)
        == "name 'expect_column_values_to_be_between_random' is not defined"
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
            def dataset_expectations(cls):
                return [
                    expect_column_values_to_be_between(
                        column=str(cls.age), minimum_value=0, max_value=100
                    ),
                    expect_column_values_to_be_in_set(
                        column=str(cls.gender),
                        value_set=["male", "female"],
                        mostly=0.9,
                    ),
                    expect_column_values_to_not_be_null(
                        column=str(cls.country), mostly=0.9
                    ),
                ]

    assert (
        str(e.value)
        == "expect_column_values_to_be_between() got an unexpected keyword argument 'minimum_value'"
    )

    with pytest.raises(Exception) as e:

        @meta(owner="test@test.com")
        @dataset
        class UserInfoDSInvalid4:
            user_id: int = field(key=True)
            name: str = field()
            age: Optional[int]
            country: Optional[str]
            gender: oneof(str, ["male", "female"])
            timestamp: datetime = field(timestamp=True)

            @meta(owner="test@test.ai.com")
            @expectations
            def dataset_expectations(cls):
                return [
                    expect_column_values_to_be_between(
                        column=str(cls.age), min_value=0, max_value=100
                    ),
                    expect_column_values_to_be_in_set(
                        column=str(cls.gender),
                        value_set=["male", "female"],
                        mostly=0.9,
                    ),
                    expect_column_values_to_not_be_null(
                        column=str(cls.country), mostly=0.9
                    ),
                ]

    assert str(e.value) == "Expectations cannot have metadata."
