from datetime import datetime, timedelta

import pandas as pd

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.sources import source, Webhook
from fennel.test_lib import mock_client

webhook = Webhook(name="fennel_webhook")


# docsnip datasets_lookup
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("User"))
@dataset
class User:
    uid: int = field(key=True)
    home_city: str
    cur_city: str
    timestamp: datetime = field(timestamp=True)


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserFeature:
    uid: int = feature(id=1)
    name: str = feature(id=2)
    in_home_city: bool = feature(id=3)

    @extractor(depends_on=[User])
    @inputs(uid)
    @outputs(in_home_city)
    def func(cls, ts: pd.Series, uid: pd.Series):
        df, _found = User.lookup(ts, uid=uid)
        return pd.Series(
            name="in_home_city", data=df["home_city"] == df["cur_city"]
        )


# /docsnip


@mock_client
def test_user_dataset_lookup(client):
    client.sync(datasets=[User], featuresets=[UserFeature])
    now = datetime.now()

    data = [
        [1, "San Francisco", "New York", now - timedelta(days=1)],
        [2, "San Francisco", "San Francisco", now - timedelta(days=1)],
        [3, "Chicago", "San Francisco", now - timedelta(days=1)],
    ]

    df = pd.DataFrame(
        data, columns=["uid", "home_city", "cur_city", "timestamp"]
    )

    res = client.log("fennel_webhook", "User", df)
    assert res.status_code == 200, res.json()
    feature_df = client.extract_features(
        output_feature_list=[UserFeature.in_home_city],
        input_feature_list=[UserFeature.uid],
        input_dataframe=pd.DataFrame({"UserFeature.uid": [1, 2, 3]}),
    )
    assert feature_df["UserFeature.in_home_city"].tolist() == [
        False,
        True,
        False,
    ]
