# Unit Tests for client.py

from fennel.client.client import _s3_connector_dict, _validate_branch_name
from fennel.sources.sources import S3

import pytest


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
            "db": {"S3": {"creds": None}},
        },
    }
    assert res == expected

    # Test with creds
    s3_src.aws_access_key_id = "access_key"
    s3_src.aws_secret_access_key = "secret_key"
    s3_conn = s3_src.bucket("bucket", "prefix", delimiter="\t")
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
                    }
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
                    }
                }
            },
        },
    }
    assert res == expected

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
        "aditya/branch-testing",
    ]
    for name in invalid_names:
        with pytest.raises(Exception) as e:
            _validate_branch_name(name)
        assert "Branch name should only contain alphanumeric characters" in str(
            e
        )
