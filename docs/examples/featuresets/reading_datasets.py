from datetime import datetime

import pandas as pd

from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import meta
from fennel.lib import inputs, outputs
from fennel.sources import source, Webhook
from fennel.testing import mock

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


# docsnip derived_extractors
@meta(owner="data-science-team@fennel.ai")
@featureset
class Request:
    user_id: int = feature(id=1)


@meta(owner="data-science-team@fennel.ai")
@featureset
class UserFeaturesDerived:
    uid: int = feature(id=1).extract(feature=Request.user_id)
    name: str = feature(id=2).extract(field=User.name, default="Unknown")


# /docsnip


# docsnip derived_extractor_with_provider
@meta(owner="data-science-team@fennel.ai")
@featureset
class Request2:
    uid: int = feature(id=1)


@meta(owner="data-science-team@fennel.ai")
@featureset
class UserFeaturesDerived2:
    name: str = feature(id=1).extract(
        field=User.name, provider=Request2, default="Unknown"
    )


# /docsnip


@mock
def test_lookup_in_extractor(client):
    client.commit(
        datasets=[User],
        featuresets=[UserFeatures, UserFeaturesDerived, UserFeaturesDerived2],
    )
    now = datetime.utcnow()
    data = pd.DataFrame(
        {
            "uid": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "timestamp": [now, now, now],
        }
    )
    res = client.log("fennel_webhook", "User", data)
    assert res.status_code == 200, res.json()

    feature_df = client.query(
        outputs=[UserFeatures.name],
        inputs=[UserFeatures.uid],
        input_dataframe=pd.DataFrame(
            {
                "UserFeatures.uid": [1, 2, 3, 4],
            }
        ),
    )

    expected = ["Alice", "Bob", "Charlie", "Unknown"]
    assert feature_df["UserFeatures.name"].tolist() == expected

    feature_df = client.query(
        outputs=[UserFeaturesDerived.name],
        inputs=[Request.user_id],
        input_dataframe=pd.DataFrame(
            {
                "Request.user_id": [1, 2, 3, 4],
            }
        ),
    )
    assert feature_df["UserFeaturesDerived.name"].tolist() == expected

    feature_df = client.query(
        outputs=[UserFeaturesDerived2.name],
        inputs=[Request2.uid],
        input_dataframe=pd.DataFrame(
            {
                "Request2.uid": [1, 2, 3, 4],
            }
        ),
    )
    assert feature_df["UserFeaturesDerived2.name"].tolist() == expected
