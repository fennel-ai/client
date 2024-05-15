from datetime import datetime
from typing import Optional

import pytest

__owner__ = "nikhil@fennel.ai"


def test_basic_dataset():
    # docsnip user_dataset
    from fennel.datasets import dataset, field
    from fennel.lib import meta

    @meta(owner="data-eng-oncall@fennel.ai")
    @dataset
    class User:
        uid: int = field(key=True)
        dob: datetime
        country: str
        update_time: datetime = field(timestamp=True)

    # /docsnip

    # docsnip dataset_version
    @dataset(version=2)  # docsnip-highlight
    class Product:
        pid: int = field(key=True)
        price: float
        update_time: datetime = field(timestamp=True)

    # /docsnip


def test_dataset_index_basic():
    def _basic():
        # docsnip dataset_index_basic
        from fennel.datasets import dataset, field

        @dataset(index=True)  # docsnip-highlight
        class User:
            uid: int = field(key=True)  # docsnip-highlight
            dob: datetime
            country: str
            update_time: datetime = field(timestamp=True)

        # /docsnip

    def _advanced():
        # docsnip dataset_index_advanced
        from fennel.datasets import dataset, field

        @dataset(index=True, offline=None)  # docsnip-highlight
        class User:
            uid: int = field(key=True)
            dob: datetime
            country: str
            update_time: datetime = field(timestamp=True)

        # /docsnip

    _basic()
    _advanced()


def test_invalid_index():
    with pytest.raises(Exception):
        # docsnip invalid_index
        from fennel.datasets import dataset

        @dataset(index=True)  # docsnip-highlight
        class UserValidDataset:
            uid: int
            country: str
            update_time: datetime

        # /docsnip


# valid - has no key fields, which is fine.
# no explicitly marked timestamp fields so update_time, which is of type
# datetime is automatically assumed to be the timestamp field
def test_valid_user_dataset():
    # docsnip valid_user_dataset
    from fennel.datasets import dataset, field
    from fennel.lib import meta

    @meta(owner="henry@fennel.ai")
    @dataset
    class UserValidDataset:
        uid: int
        country: str
        update_time: datetime  # docsnip-highlight

    # /docsnip


def test_valid_dataset_multiple_datetime_fields():
    # docsnip valid_dataset_multiple_datetime_fields
    from fennel.datasets import dataset, field
    from fennel.lib import meta

    @meta(owner="laura@fennel.ai")
    @dataset
    class User:
        uid: int
        country: str
        # docsnip-highlight start
        update_time: datetime = field(timestamp=True)
        signup_time: datetime
        # docsnip-highlight end

    # /docsnip


def test_metaflags_dataset():
    # docsnip metaflags_dataset
    from fennel.datasets import dataset, field
    from fennel.lib import meta

    @meta(owner="abc-team@fennel.ai", tags=["PII", "experimental"])
    @dataset
    class UserWithMetaFlags:
        uid: int = field(key=True)
        height: float = field().meta(description="height in inches")
        weight: float = field().meta(description="weight in lbs")
        updated: datetime

    # /docsnip


def test_metaflags_dataset_default_owners():
    # docsnip metaflags_dataset_default_owners
    from fennel.datasets import dataset, field
    from fennel.lib import meta

    __owner__ = "hoang@fennel.ai"

    @dataset
    class UserBMI:
        uid: int = field(key=True)
        height: float
        weight: float
        bmi: float
        updated: datetime

    @meta(owner="luke@fennel.ai")
    @dataset
    class UserName:
        uid: int = field(key=True)
        name: str
        updated: datetime

    @dataset
    class UserLocation:
        uid: int = field(key=True)
        city: str
        updated: datetime

    # /docsnip
    # just something to use __owner__ to remove lint warning
    assert len(__owner__) > 0


# invalid - key fields can not have an optional type
def test_optional_key_field():
    with pytest.raises(Exception) as e:
        # docsnip invalid_user_dataset_optional_key_field
        from fennel.datasets import dataset, field
        from fennel.lib import meta

        @meta(owner="test@fennel.ai")
        @dataset
        class User:
            uid: Optional[int] = field(key=True)  # docsnip-highlight
            country: str
            update_time: datetime

        # /docsnip
    assert "Key uid in dataset User cannot be Optional" in str(e.value)


# invalid - no field of `datetime` type
def test_no_datetime_field():
    with pytest.raises(Exception) as e:
        # docsnip invalid_user_dataset_no_datetime_field
        from fennel.datasets import dataset, field
        from fennel.lib import meta

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
# field is ambiguous
def test_ambiguous_timestamp_field():
    with pytest.raises(Exception) as e:
        # docsnip invalid_user_dataset_ambiguous_timestamp_field
        from fennel.datasets import dataset, field
        from fennel.lib import meta

        @meta(owner="data-eng-oncall@fennel.ai")
        @dataset
        class User:
            uid: int
            country: str
            # docsnip-highlight start
            created_time: datetime
            updated_time: datetime
            # docsnip-highlight end

        # /docsnip
    assert "Multiple timestamp fields are not supported" in str(e.value)
