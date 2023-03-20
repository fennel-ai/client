from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.schema import Series
from fennel.lib.metadata import meta
from fennel.datasets import dataset, field
from datetime import datetime
from fennel.test_lib import mock_client
import pandas as pd
from datetime import datetime, timedelta


# docsnip datasets_lookup
@meta(owner="data-eng-oncall@fennel.ai")
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

    @depends_on(User)
    @extractor
    def func(
        cls, ts: Series[datetime], uid: Series[uid]
    ) -> Series[in_home_city]:
        df, _found = User.lookup(ts, uid=uid)
        return df["home_city"] == df["cur_city"]


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

    res = client.log("User", df)
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
