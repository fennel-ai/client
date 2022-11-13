from datetime import datetime
from typing import Optional

import pytest

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.test_lib import *


def test_MultipleDateTime(grpc_stub):
    with pytest.raises(ValueError) as e:

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
            timestamp: datetime

    _ = InternalTestClient(grpc_stub)
    assert str(e.value) == "Multiple timestamp fields are not supported."


def test_InvalidRetentionWindow(grpc_stub):
    with pytest.raises(TypeError) as e:

        @dataset(retention=324)
        class Activity:
            user_id: int
            action_type: float
            amount: Optional[float]
            timestamp: datetime

    assert (
        str(e.value) == "duration 324 must be a specified as a string for eg. "
        "1d/2m/3y."
    )


def test_DatasetWithPipes(grpc_stub):
    with pytest.raises(Exception) as e:

        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @dataset
        class ABCDataset:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @staticmethod
            @pipeline
            def create_pipeline(self, a: Dataset):
                return a

    assert str(e.value) == "pipeline must take atleast one Dataset."

    with pytest.raises(TypeError) as e:

        @dataset
        class ABCDataset2:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @staticmethod
            @pipeline(XYZ)
            def create_pipeline(self, a: Dataset):
                return a

    assert (
        str(e.value)
        == "pipeline functions cannot have self as a parameter and are "
        "like static methods."
    )


def test_DatasetIncorrectJoin(grpc_stub):
    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: str
            timestamp: datetime

        @dataset
        class ABCDataset:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @staticmethod
            @pipeline(XYZ)
            def create_pipeline(a: Dataset):
                b = a.transform(lambda x: x)
                return a.join(b, on=["user_id"])  # type: ignore

    assert str(e.value) == "Cannot join with an intermediate dataset"
