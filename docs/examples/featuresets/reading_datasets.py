from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.sources import source, Webhook
from fennel.test_lib import mock

webhook = Webhook(name="fennel_webhook")


# docsnip featuresets_reading_datasets
@meta(owner="data-eng-team@fennel.ai")
@source(webhook.endpoint("User"))
@dataset
class User:
    uid: int = field(key=True)
    name: str
    timestamp: datetime


@meta(owner="data-science-team@fennel.ai")
@featureset
class UserFeatures:
    uid: int = feature(id=1)
    name: str = feature(id=2)

    @extractor(depends_on=[User])
    @inputs(uid)
    @outputs(name)
    def func(cls, ts: pd.Series, uids: pd.Series):
        names, found = User.lookup(ts, uid=uids)
        names.fillna("Unknown", inplace=True)
        return names[["name"]]


# /docsnip


@mock
def test_lookup_in_extractor(client, fake_data_plane):
    client.sync(datasets=[User], featuresets=[UserFeatures])
    now = datetime.now()
    data = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "timestamp": [now, now, now],
        }
    )
    res = client.log("fennel_webhook", "User", data)
    assert res.status_code == 200, res.json()

    feature_df = client.extract_features(
        output_feature_list=[UserFeatures.name],
        input_feature_list=[UserFeatures.uid],
        input_dataframe=pd.DataFrame(
            {
                "UserFeatures.uid": [1, 2, 3, 4],
            }
        ),
    )
    assert feature_df["UserFeatures.name"].tolist() == [
        "Alice",
        "Bob",
        "Charlie",
        "Unknown",
    ]
