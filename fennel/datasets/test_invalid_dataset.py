from datetime import datetime
from typing import Optional, List

import pytest

from fennel.datasets import dataset, pipeline, field, Dataset
from fennel.lib.schema import inputs
from fennel.test_lib import *


def test_multiple_date_time(grpc_stub):
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
    assert (
        str(e.value) == "Multiple timestamp fields are not supported in "
        "dataset `UserInfoDataset`."
    )


def test_invalid_retention_window(grpc_stub):
    with pytest.raises(TypeError) as e:

        @dataset(history=324)
        class Activity:
            user_id: int
            action_type: float
            amount: Optional[float]
            timestamp: datetime

    assert (
        str(e.value) == "duration 324 must be a specified as a string for eg. "
        "1d/2m/3y."
    )


def test_dataset_with_pipes(grpc_stub):
    @dataset
    class XYZ:
        user_id: int
        name: str
        timestamp: datetime

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline
            def create_pipeline(cls, a: Dataset):
                return a

    assert (
        str(e.value) == "pipeline `create_pipeline` must be called with an id."
    )

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset1:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(XYZ)
            def create_pipeline(cls, a: Dataset):
                return a

    assert str(e.value) == "pipeline `XYZ` must be called with an id."

    with pytest.raises(Exception) as e:

        @dataset
        class ABCDataset2:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(id=1)
            def create_pipeline(cls, a: Dataset):
                return a

    assert (
        str(e.value)
        == "pipeline `create_pipeline` must have Datasets as @input "
        "parameters."
    )

    with pytest.raises(TypeError) as e:

        @dataset
        class ABCDataset3:
            a: int = field(key=True)
            b: int = field(key=True)
            c: int
            d: datetime

            @pipeline(id=1)  # type: ignore
            @inputs(XYZ)
            def create_pipeline(a: Dataset):  # type: ignore
                return a

    assert (
        str(e.value)
        == "pipeline functions are classmethods and must have cls as the "
        "first parameter, found `a` for pipeline `create_pipeline`."
    )


def test_dataset_incorrect_join(grpc_stub):
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

            @pipeline(id=1)
            @inputs(XYZ)
            def create_pipeline(cls, a: Dataset):
                b = a.transform(lambda x: x)
                return a.left_join(b, on=["user_id"])  # type: ignore

    assert str(e.value) == "Cannot join with an intermediate dataset"


def test_dataset_optional_key(grpc_stub):
    with pytest.raises(ValueError) as e:

        @dataset
        class XYZ:
            user_id: int
            name: Optional[str] = field(key=True)
            timestamp: datetime

    assert str(e.value) == "Key name in dataset XYZ cannot be Optional."


def test_protected_fields(grpc_stub):
    with pytest.raises(Exception) as e:

        @dataset(history="324d")
        class Activity:
            fields: List[int]
            key_fields: float
            on_demand: Optional[float]
            timestamp_field: datetime

    assert (
        str(e.value)
        == "[Exception('Field name `fields` is reserved. Please use a "
        "different name in dataset `Activity`.'), Exception('Field "
        "name `key_fields` is reserved. Please use a different name in dataset `Activity`"
        ".'), Exception('Field name `on_demand` is reserved. Please "
        "use a different name in dataset `Activity`.'), Exception('Field "
        "name `timestamp_field` is reserved. Please use a different "
        "name in dataset `Activity`.')]"
    )
