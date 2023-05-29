from datetime import datetime
from typing import Optional

import pytest

from fennel.datasets import dataset, field
from fennel.lib.metadata import meta


def test_invalid_email():
    with pytest.raises(ValueError) as e:

        @meta(owner="test", description="test", tags=["test"], deprecated=True)
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
            timestamp: datetime = field(timestamp=True)

    expected_err = (
        "1 validation error for Metadata\n"
        "owner\n"
        "  Invalid email 'test' (type=value_error)"
    )
    assert str(e.value) == expected_err
