from datetime import datetime
import pandas as pd
from fennel.testing import mock

__owner__ = "ml-team@fennel.ai"


# docsnip featuresets_reading_datasets
from fennel.datasets import dataset, field, index
from fennel.connectors import source, Webhook
from fennel.featuresets import featureset, extractor, feature
from fennel.lib import inputs, outputs

webhook = Webhook(name="fennel_webhook")


@source(webhook.endpoint("User"), disorder="14d", cdc="append")
# docsnip-highlight start
@index
@dataset
# docsnip-highlight end
class User:  # docsnip-highlight
    uid: int = field(key=True)
    name: str
    timestamp: datetime


@featureset
class UserFeatures:
    uid: int = feature(id=1)
    name: str = feature(id=2)

    @extractor(depends_on=[User])  # docsnip-highlight
    @inputs(uid)
    @outputs(name)
    def func(cls, ts: pd.Series, uids: pd.Series):
        # docsnip-highlight next-line
        names, found = User.lookup(ts, uid=uids)
        names.fillna("Unknown", inplace=True)
        return names[["name"]]


# /docsnip


# docsnip derived_extractors
@featureset
class Request:
    user_id: int = feature(id=1)


@featureset
class UserFeaturesDerived:
    uid: int = feature(id=1).extract(feature=Request.user_id)
    name: str = feature(id=2).extract(field=User.name, default="Unknown")


# /docsnip


# docsnip derived_extractor_with_provider
@featureset
class Request2:
    uid: int = feature(id=1)


@featureset
class UserFeaturesDerived2:
    name: str = feature(id=1).extract(
        field=User.name, provider=Request2, default="Unknown"
    )


# /docsnip


@mock
def test_lookup_in_extractor(client):
    client.commit(
        message="user: add user dataset and featuresets",
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
