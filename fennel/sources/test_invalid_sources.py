from datetime import datetime
from typing import Optional

import pytest

from fennel.dataset import dataset, field
from fennel.sources import source, S3, MySQL
# noinspection PyUnresolvedReferences
from fennel.test_lib import *

mysql = MySQL(
    name="mysql",
    host="localhost",
    db_name="test",
    username="root",
    password="root",
)


def test_SimpleSource(grpc_stub):
    @source(mysql, every="1h")
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


s3 = S3(
    name='ratings_source',
    bucket="all_ratings",
    path_prefix="prod/apac/",
    aws_access_key_id="ALIAQOTFAKEACCCESSKEYIDGTAXJY6MZWLP",
    aws_secret_access_key="8YCvIs8f0+FAKESECRETKEY+7uYSDmq164v9hNjOIIi3q1uV8rv",
    src_schema={"Name": "string", "Weight": "number", "Age": "integer"},
    delimiter=",",
)


def test_MultipleSources(grpc_stub):
    with pytest.raises(AttributeError) as e:
        @source(s3.table("user"), every="1h")
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

        view = InternalTestView(grpc_stub)
        view.add(UserInfoDataset)
        sync_request = view.to_proto()
        assert len(sync_request.dataset_requests) == 1
        dataset_request = sync_request.dataset_requests[0]
        assert len(dataset_request.sources) == 3

    assert str(e.value) == "'S3' object has no attribute 'table'"
