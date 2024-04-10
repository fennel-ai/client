from datetime import datetime, timedelta

import pandas as pd

from fennel.datasets import dataset, field, index
from fennel.featuresets import featureset, feature as F, extractor
from fennel.lib import meta, inputs, outputs
from fennel.connectors import source, Webhook
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")


# docsnip datasets_lookup
@meta(owner="data-eng-oncall@fennel.ai")
@source(webhook.endpoint("User"), disorder="14d", cdc="append")
@index
@dataset
class User:
    uid: int = field(key=True)
    home_city: str
    cur_city: str
    timestamp: datetime = field(timestamp=True)


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserFeature:
    uid: int
    name: str
    in_home_city: bool

    @extractor(deps=[User])
    @inputs("uid")
    @outputs("in_home_city")
    def func(cls, ts: pd.Series, uid: pd.Series):
        df, _found = User.lookup(ts, uid=uid)
        return pd.Series(
            name="in_home_city",
            data=df["home_city"] == df["cur_city"],
        )


# /docsnip


@mock
def test_user_dataset_lookup(client):
    client.commit(message="msg", datasets=[User], featuresets=[UserFeature])
    now = datetime.utcnow()

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
    feature_df = client.query(
        outputs=[UserFeature.in_home_city],
        inputs=[UserFeature.uid],
        input_dataframe=pd.DataFrame({"UserFeature.uid": [1, 2, 3]}),
    )
    assert feature_df["UserFeature.in_home_city"].tolist() == [
        False,
        True,
        False,
    ]
