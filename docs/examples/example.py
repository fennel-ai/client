import unittest
from datetime import datetime
import pytest
import pandas as pd
import requests

# docsnip user_info_dataset
from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.lib.schema.schema import oneof, regex, between
from fennel.sources import source, Webhook
from fennel.test_lib import mock

EMAIL_REGEX = r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+"

webhook = Webhook(name="fennel_webhook")


@meta(owner="test@test.com")
@source(webhook.endpoint("UserInfoDataset"))
@dataset
class UserInfoDataset:
    user_id: int = field(key=True).meta(description="User ID")  # type: ignore
    name: str = field().meta(description="User name")  # type: ignore
    age: between(int, 0, 100) = field().meta(  # type: ignore
        description="User age"
    )
    gender: oneof(str, ["male", "female"])  # type: ignore # noqa
    country_code: oneof(int, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])  # type: ignore
    email: regex(EMAIL_REGEX)  # type: ignore
    timestamp: datetime = field(timestamp=True)


# /docsnip


# docsnip testing_multiple_snippets
class TestDataset(unittest.TestCase):
    @mock
    def test_log_with_additional_schema(self, client):
        # Log correct data
        client.sync(datasets=[UserInfoDataset])
        now = datetime.now()
        data = [
            {
                "user_id": 1,
                "name": "John",
                "age": 30,
                "gender": "male",
                "country_code": 1,
                "email": "john@fennel",
                "timestamp": now,
            },
        ]
        df = pd.DataFrame(data)
        response = client.log("fennel_webhook", "UserInfoDataset", df)
        assert response.status_code == requests.codes.OK, response.json()

        # Log incorrect data
        now = datetime.now()
        data = [
            {
                "user_id": 1,
                "name": "John",
                "age": 123,
                "gender": "male",
                "country_code": 1,
                "email": "john@fennel",
                "timestamp": now,
            },
        ]
        df = pd.DataFrame(data)
        if client.is_integration_client():
            response = client.log("fennel_webhook", "UserInfoDataset", df)
            assert (
                response.json()["error"]
                == """error: input parse error: value 123 does not match between type Between(Between { dtype: Int, min: Int(0), max: Int(100), strict_min: false, strict_max: false })"""
            )
            assert response.status_code == requests.codes.BAD
        else:
            with pytest.raises(Exception) as e:
                client.log("fennel_webhook", "UserInfoDataset", df)
            assert (
                str(e.value)
                == "Schema validation failed during data insertion to "
                "`UserInfoDataset` [ValueError('Field `age` is of type between, but the value `123` is out of bounds. Error found during checking schema for `UserInfoDataset`.')]"
            )

        now = datetime.now()
        data = [
            {
                "user_id": 1,
                "name": "John",
                "age": 12,
                "gender": "transgender",
                "country_code": 11,
                "email": "john@fennel",
                "timestamp": now,
            },
        ]
        df = pd.DataFrame(data)
        if client.is_integration_client():
            response = client.log("fennel_webhook", "UserInfoDataset", df)
            assert response.status_code == requests.codes.BAD
            assert (
                response.json()["error"]
                == """error: input parse error: expected string in [String("male"), String("female")], but got transgender"""
            )
        else:
            with pytest.raises(Exception) as e:
                client.log("fennel_webhook", "UserInfoDataset", df)
            assert (
                """Schema validation failed during data insertion to `UserInfoDataset` [ValueError("Field 'gender' is of type oneof, but the value 'transgender' is not found in the set of options ['female', 'male']. Error found during checking schema for `UserInfoDataset`."), ValueError("Field 'country_code' is of type oneof, but the value '11' is not found in the set of options [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]. Error found during checking schema for `UserInfoDataset`.")]"""
                == str(e.value)
            )

        now = datetime.now()
        data = [
            {
                "user_id": 1,
                "name": "John",
                "age": 1,
                "gender": "male",
                "country_code": 1,
                "email": "johnfennel",
                "timestamp": now,
            },
        ]
        df = pd.DataFrame(data)
        if client.is_integration_client():
            response = client.log("fennel_webhook", "UserInfoDataset", df)
            assert (
                response.json()["error"]
                == """error: input parse error: expected regex string [a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+ to match, but got johnfennel"""
            )
            assert response.status_code == requests.codes.BAD
        else:
            with pytest.raises(Exception) as e:
                client.log("fennel_webhook", "UserInfoDataset", df)
            assert (
                """[ValueError('Field `email` is of type regex, but the value `johnfennel` does not match the regex `[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]"""
                in str(e.value)
            )


# /docsnip
