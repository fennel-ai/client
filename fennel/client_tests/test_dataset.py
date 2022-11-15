import unittest
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta
from fennel.test_lib import mock_client


################################################################################
#                           Dataset Unit Tests
################################################################################


@meta(owner="test@test.com")
@dataset
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    # Users date of birth
    dob: str = field().meta(deleted=True)  # type: ignore
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


class TestDataset(unittest.TestCase):
    @mock_client
    def test_log_to_dataset(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # # Sync the dataset
        client.sync(datasets=[UserInfoDataset])

        data = [
            [18232, "Ross", 32, "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK

    @mock_client
    def test_invalid_dataschema(self, client):
        """Check if invalid data raises an error."""
        client.sync(datasets=[UserInfoDataset])
        data = [
            [18232, "Ross", "32", "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]

        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        assert (
            response.json()["error"]
            == """("Could not convert '32' with type str: """
            """tried to convert to int64", 'Conversion """
            """failed for column age with type object')"""
        )

    @mock_client
    def test_deleted_field(self, client):
        client.sync(datasets=[UserInfoDataset])

        data = [
            [18232, "Ross", "01021990", 32, "USA", 1668475993],
            [18234, "Monica", "02031998", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "dob", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK
