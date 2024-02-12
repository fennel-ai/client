from datetime import datetime
from datetime import timedelta
import pytest

HOUR = timedelta(hours=1)

import pandas as pd
from fennel.test_lib import mock

__owner__ = "aditya@fennel.ai"


@mock
def test_basic(client):
    # docsnip basic
    from fennel.featuresets import featureset, feature, extractor
    from fennel.lib.schema import inputs, outputs

    @featureset
    class Numbers:
        num: int = feature(id=1)
        is_even: bool = feature(id=2)
        is_odd: bool = feature(id=3)

        @extractor
        @inputs(num)
        @outputs(is_even, is_odd)
        def my_extractor(cls, ts, nums: pd.Series):
            is_even = nums.apply(lambda x: x % 2 == 0)
            is_odd = is_even.apply(lambda x: not x)
            return pd.DataFrame({"is_even": is_even, "is_odd": is_odd})

    client.sync(featuresets=[Numbers])
    feature_df = client.extract(
        inputs=[Numbers.num],
        outputs=[Numbers.is_even, Numbers.is_odd],
        input_dataframe=pd.DataFrame({"Numbers.num": [1, 2, 3, 4]}),
    )
    pd.testing.assert_frame_equal(
        feature_df,
        pd.DataFrame(
            {
                "Numbers.is_even": [False, True, False, True],
                "Numbers.is_odd": [True, False, True, False],
            }
        ),
    )
    # /docsnip

    def _unused():
        # docsnip extract_historical_api
        response = client.extract_historical(
            inputs=[Numbers.num],
            outputs=[Numbers.is_even, Numbers.is_odd],
            format="pandas",
            input_dataframe=pd.DataFrame(
                {"Numbers.num": [1, 2, 3, 4]},
                {"timestamp": [datetime.utcnow() - HOUR * i for i in range(4)]},
            ),
            timestamp_column="timestamp",
        )
        print(response)
        # /docsnip

    # docsnip extract_historical_progress
    request_id = "bf5dfe5d-0040-4405-a224-b82c7a5bf085"
    response = client.extract_historical_progress(request_id)
    print(response)
    # /docsnip
    # docsnip extract_historical_cancel
    request_id = "bf5dfe5d-0040-4405-a224-b82c7a5bf085"
    response = client.extract_historical_cancel_request(request_id)
    print(response)
    # /docsnip
    # docsnip extract_historical_response
    {
        "request_id": "bf5dfe5d-0040-4405-a224-b82c7a5bf085",
        "output_bucket": "my-bucket",
        "output_prefix": "/data/training",
        "completion_rate": 0.76,
        "failure_rate": 0.0,
    }
    # /docsnip

    with pytest.raises(Exception):
        # docsnip extract_historical_s3
        from fennel.sources import S3

        s3 = S3(
            name="extract_hist_input",
            aws_access_key_id="<ACCESS KEY HERE>",
            aws_secret_access_key="<SECRET KEY HERE>",
        )
        s3_input_connection = s3.bucket("bucket", prefix="data/user_features")
        s3_output_connection = s3.bucket("bucket", prefix="output")

        response = client.extract_historical(
            inputs=[Numbers.num],
            outputs=[Numbers.is_even, Numbers.is_odd],
            format="csv",
            timestamp_column="timestamp",
            input_s3=s3_input_connection,
            output_s3=s3_output_connection,
        )
        # /docsnip
