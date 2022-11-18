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
class UserInformationDataset:
    user_id: int = field(key=True)
    name: str
    age: Optional[int]
    country: Optional[str]
    timestamp: datetime = field(timestamp=True)


class TestDataset(unittest.TestCase):
    @mock_client
    def test_log_to_dataset(self, client):
        """Log some data to the dataset and check if it is logged correctly."""
        # # Sync the dataset
        client.sync(datasets=[UserInformationDataset])

        data = [
            [18232, "Ross", 32, "USA", 1668475993],
            [18234, "Monica", 24, "Chile", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.OK

        # Do some lookups
        user_ids = pd.Series([18232, 18234])
        ts = pd.Series([1668500000, 1668500000])
        df = UserInformationDataset.lookup(ts, user_id=user_ids)

        # assert df["name"].tolist() == ["Ross", "Monica"]
        # assert df["age"].tolist() == [32, 24]
        # assert df["country"].tolist() == ["USA", "Chile"]
        #
        # # Do some lookups with a timestamp
        # ts = pd.Series([1668475343, 1668475343])
        # df = UserInfoDataset.lookup(ts, user_id=user_ids)
        #
        # assert df["name"].tolist() == [None, "Monica"]
        # assert df["age"].tolist() == [None, 24]
        # assert df["country"].tolist() == [None, "Chile"]
        #
        # data = [
        #     [18232, "Ross", 33, "Russia", 1668500001],
        #     [18234, "Monica", 25, "Columbia", 1668500004],
        # ]
        # columns = ["user_id", "name", "age", "country", "timestamp"]
        # df = pd.DataFrame(data, columns=columns)
        # response = client.log("UserInfoDataset", df)
        # assert response.status_code == requests.codes.OK
        #
        # # Do some lookups
        # ts = pd.Series([1668500001, 1668500001])
        # df = UserInfoDataset.lookup(ts, user_id=user_ids)
        # assert df.shape == (2, 4)
        # assert df["user_id"].tolist() == [18232, 18234]
        # assert df["age"].tolist() == [33, 24]
        # assert df["country"].tolist() == ["Russia", "Chile"]
        #
        # ts = pd.Series([1668500004, 1668500004])
        # df = UserInfoDataset.lookup(ts, user_id=user_ids)
        # assert df.shape == (2, 4)
        # assert df["user_id"].tolist() == [18232, 18234]
        # assert df["age"].tolist() == [33, 25]
        # assert df["country"].tolist() == ["Russia", "Columbia"]

    @mock_client
    def test_invalid_dataschema(self, client):
        """Check if invalid data raises an error."""
        client.sync(datasets=[UserInformationDataset])
        data = [
            [18232, "Ross", "32", "USA", 1668475993],
            [18234, "Monica", 24, "USA", 1668475343],
        ]
        columns = ["user_id", "name", "age", "country", "timestamp"]
        df = pd.DataFrame(data, columns=columns)
        response = client.log("UserInfoDataset", df)
        assert response.status_code == requests.codes.BAD_REQUEST
        assert len(response.json()["error"]) > 0

    @mock_client
    def test_deleted_field(self, client):
        with self.assertRaises(Exception) as e:
            @meta(owner="test@test.com")
            @dataset
            class UserInfoDataset:
                user_id: int = field(key=True)
                name: str
                age: Optional[int] = field().meta(deleted=True)
                country: Optional[str]
                timestamp: datetime = field(timestamp=True)

            client.sync(datasets=[UserInfoDataset])

        assert (
                str(e.exception)
                == "Dataset currently does not support deleted or deprecated fields."
        )
