import unittest
from datetime import datetime
from typing import Optional

import pytest
import requests
from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.expectations_pb2 as exp_proto
from fennel.datasets import dataset, field
from fennel.lib.expectations import (
    expectations,
    expect_column_values_to_be_between,
    expect_column_values_to_be_in_set,
    expect_column_values_to_not_be_null,
)
from fennel.lib.metadata import meta
from fennel.lib.schema import oneof
from fennel.test_lib import *


def test_dataset_expectation_creation(grpc_stub):
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

    view = InternalTestClient(grpc_stub)
    view.add(UserInfoDS)
    sync_request = view._get_sync_request_proto()
    assert len(sync_request.datasets) == 1
    # d = {
    #     "suite": "dataset_UserInfoDS_expectations",
    #     "expectations": [
    #         {
    #             "expectationType": "expect_column_values_to_be_between",
    #             "expectationKwargs": '{"column": "age", "min_value": 0, "max_value": 100, "strict_min": false, "strict_max": false, "allow_cross_type_comparisons": null, "parse_strings_as_datetimes": false, "output_strftime_format": null, "mostly": null, "row_condition": null, "condition_parser": null, "result_format": null, "include_config": true, "catch_exceptions": null, "meta": null}',
    #         },
    #         {
    #             "expectationType": "expect_column_values_to_be_in_set",
    #             "expectationKwargs": '{"column": "gender", "value_set": ["male", "female"], "mostly": 0.9, "parse_strings_as_datetimes": null, "result_format": null, "row_condition": null, "condition_parser": null, "include_config": true, "catch_exceptions": null, "meta": null}',
    #         },
    #         {
    #             "expectationType": "expect_column_values_to_not_be_null",
    #             "expectationKwargs": '{"column": "country", "mostly": 0.9, "result_format": null, "row_condition": null, "condition_parser": null, "include_config": true, "catch_exceptions": null, "meta": null}',
    #         },
    #     ],
    # }
    # expected_exp_request = ParseDict(d, exp_proto.Expectations())
    # TODO(aditya, mohit): Enable this once proto have expectations
    # act_config = sync_request.datasets[0].expectations
    # assert act_config == expected_exp_request, error_message(
    #     act_config, expected_exp_request
    # )


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
    @mock_client
    def test_dataset_expectation_creation_integration(self, client):
        response = client.sync(datasets=[UserInfoDS])
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
        class UserInfoDSInvalid3:
            user_id: int = field(key=True)
            name: str = field()
            age: Optional[int]
            country: Optional[str]
            gender: oneof(str, ["male", "female"])
            timestamp: datetime = field(timestamp=True)

            @expectations(version=1)
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

    assert str(e.value) == "Versioning is not yet supported for expectations."

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
