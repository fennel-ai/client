# Unit Tests for client.py

import pytest

from fennel.client.client import _s3_connector_dict, _validate_branch_name
from fennel.connectors.connectors import S3, CSV
from fennel.testing import mock


def test_s3_connector_dict():
    # Test with defaults: csv, not pre sorted, no creds
    s3_src = S3.get("s3_source")
    s3_conn = s3_src.bucket("bucket", "prefix")
    res = _s3_connector_dict(s3_conn)
    # The expected field names match the serde deserialization of S3Table on the server
    expected = {
        "bucket": "bucket",
        "path_prefix": "prefix",
        "format": {
            # 44 is the ascii value for comma
            "csv": {"delimiter": 44}
        },
        "pre_sorted": False,
        "db": {
            "name": "extract_historical_s3_input",
            "db": {"S3": {"creds": None, "role_arn": None}},
        },
    }
    assert res == expected

    # Test with creds
    s3_src.aws_access_key_id = "access_key"
    s3_src.aws_secret_access_key = "secret_key"
    s3_conn = s3_src.bucket("bucket", "prefix", format=CSV(delimiter="\t"))
    res = _s3_connector_dict(s3_conn)
    expected = {
        "bucket": "bucket",
        "path_prefix": "prefix",
        "format": {
            # 16 the ascii value for tab
            "csv": {"delimiter": 9}
        },
        "pre_sorted": False,
        "db": {
            "name": "extract_historical_s3_input",
            "db": {
                "S3": {
                    "creds": {
                        "access_key": "access_key",
                        "secret_key": "secret_key",
                    },
                    "role_arn": None,
                }
            },
        },
    }
    assert res == expected

    # Test with pre sorted, json
    s3_conn = s3_src.bucket("bucket", "prefix", format="json", presorted=True)
    res = _s3_connector_dict(s3_conn)
    expected = {
        "bucket": "bucket",
        "path_prefix": "prefix",
        "format": "json",
        "pre_sorted": True,
        "db": {
            "name": "extract_historical_s3_input",
            "db": {
                "S3": {
                    "creds": {
                        "access_key": "access_key",
                        "secret_key": "secret_key",
                    },
                    "role_arn": None,
                }
            },
        },
    }
    assert res == expected

    # Test with role_arn
    s3_src.aws_access_key_id = None
    s3_src.aws_secret_access_key = None
    s3_src.role_arn = "arn:aws:iam::765237557123:role/admin"
    s3_conn = s3_src.bucket("bucket", "prefix", format=CSV(delimiter="\t"))
    res = _s3_connector_dict(s3_conn)
    expected = {
        "bucket": "bucket",
        "path_prefix": "prefix",
        "format": {
            # 16 the ascii value for tab
            "csv": {"delimiter": 9}
        },
        "pre_sorted": False,
        "db": {
            "name": "extract_historical_s3_input",
            "db": {
                "S3": {
                    "creds": None,
                    "role_arn": "arn:aws:iam::765237557123:role/admin",
                }
            },
        },
    }
    assert res == expected

    # Test with invalid role_arn
    with pytest.raises(Exception) as e:
        s3_src.role_arn = "arn:aws:iam::765237557123:role/admin"
        s3_src.aws_access_key_id = "access_key"
        s3_src.aws_secret_access_key = "secret_key"
        res = _s3_connector_dict(s3_conn)
    assert "Both access key credentials and role arn should not be set" in str(
        e
    )

    # Test with invalid creds
    with pytest.raises(Exception) as e:
        s3_src.aws_secret_access_key = None
        s3_conn = s3_src.bucket("bucket", "prefix")
        res = _s3_connector_dict(s3_conn)
    assert "secret key not found" in str(e)

    with pytest.raises(Exception) as e:
        s3_src.aws_access_key_id = None
        s3_src.aws_secret_access_key = "secret_key"
        s3_conn = s3_src.bucket("bucket", "prefix")
        res = _s3_connector_dict(s3_conn)
    assert "access key id not found" in str(e)


def test_valid_branch_names():
    valid_names = [
        "main",
        "main.branch",
        "a.b.c",
        ".....",
        "main-branch",
        "main_branch",
        "main-branch-1",
        "main-branch-1-2",
        "main-branch_1-2-3",
        "aditya_1-2-3",
        "aditya/branch-testing",
    ]
    for name in valid_names:
        _validate_branch_name(name)
    invalid_names = [
        "main branch",
        "ad$23",
        ".-_%",
        "main/branch" "@branchname" "branch$name",
        "branch name",
        "branch-name-1$",
    ]
    for name in invalid_names:
        with pytest.raises(Exception) as e:
            _validate_branch_name(name)
        assert "Branch name should only contain alphanumeric characters" in str(
            e
        )


@mock
def test_delete_branch(client):
    client.init_branch("BranchA")
    client.init_branch("BranchB")
    with pytest.raises(Exception) as e:
        client.init_branch("BranchA")
    assert str(e.value) == "Branch name: `BranchA` already exists"
    assert client.list_branches() == ["main", "BranchA", "BranchB"]

    with pytest.raises(Exception) as e:
        client.delete_branch("main")
    assert str(e.value) == "Cannot delete the main branch."
    client.delete_branch("BranchA")
    assert client.list_branches() == ["main", "BranchB"]
    client.delete_branch("BranchB")
    assert client.list_branches() == ["main"]
