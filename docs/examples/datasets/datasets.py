from datetime import datetime
from typing import Optional

import pytest

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta


# docsnip user_dataset
@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class User:
    uid: int = field(key=True)
    dob: datetime
    country: str
    update_time: datetime = field(timestamp=True)


# /docsnip


# valid - has no key fields, which is fine.
# no explicitly marked timestamp fields so update_time, which is of type
# datetime is automatically assumed to be the timestamp field
def test_valid_user_dataset():
    def indentation(s):
        # docsnip valid_user_dataset
        @meta(owner="data-eng-oncall@fennel.ai")
        @dataset
        class UserValidDataset:
            uid: int
            country: str
            update_time: datetime

        # /docsnip

        # docsnip metaflags_dataset
        @meta(owner="abc-team@fennel.ai", tags=["PII", "experimental"])
        @dataset
        class UserWithMetaFlags:
            uid: int = field(key=True)
            height: float = field().meta(description="height in inches")
            weight: float = field().meta(description="weight in lbs")
            updated: datetime

        # /docsnip


# invalid - key fields can not have an optional type
def test_optional_key_field():
    with pytest.raises(Exception) as e:
        # docsnip invalid_user_dataset_optional_key_field
        @meta(owner="test@fennel.ai")
        @dataset
        class User:
            uid: Optional[int] = field(key=True)
            country: str
            update_time: datetime

        # /docsnip
    assert "Key uid in dataset User cannot be Optional" in str(e.value)


# invalid - no field of `datetime` type
def test_no_datetime_field():
    with pytest.raises(Exception) as e:
        # docsnip invalid_user_dataset_no_datetime_field
        @meta(owner="data-eng-oncall@fennel.ai")
        @dataset
        class User:
            uid: int
            country: str
            update_time: int

        # /docsnip
    assert "No timestamp field found" in str(e.value)


# invalid - no explicitly marked `timestamp` field
# and multiple fields of type `datetime` so timestamp
# field is amgiguous
def test_ambiguous_timestamp_field():
    with pytest.raises(Exception) as e:
        # docsnip invalid_user_dataset_ambiguous_timestamp_field
        @meta(owner="data-eng-oncall@fennel.ai")
        @dataset
        class User:
            uid: int
            country: str
            created_time: datetime
            updated_time: datetime

        # /docsnip
    assert "Multiple timestamp fields are not supported" in str(e.value)
