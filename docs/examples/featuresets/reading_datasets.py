from datetime import datetime
import pandas as pd

from fennel.datasets import dataset, field
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import mock_client
from fennel.lib.metadata import meta

# docsnip featuresets_reading_datasets
from fennel.featuresets import depends_on, featureset, extractor, feature


@meta(owner="data-eng-oncall@fennel.ai")
@dataset
class User:
    uid: int = field(key=True)
    name: str
    timestamp: datetime


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserFeatures:
    uid: int = feature(id=1)
    name: str = feature(id=2)

    @extractor
    @depends_on(User)
    def func(cls, ts: Series[datetime], uids: Series[uid]) -> Series[name]:
        names, found = User.lookup(ts, uid=uids)
        names.fillna("Unknown", inplace=True)
        return names[["name"]]


# /docsnip


@mock_client
def test_lookup_in_extractor(client):
    client.sync(datasets=[User], featuresets=[UserFeatures])
    now = datetime.now()
    data = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "timestamp": [now, now, now],
        }
    )
    res = client.log("User", data)
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
