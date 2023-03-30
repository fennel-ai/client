import pandas as pd

from fennel.featuresets import feature, featureset, extractor
from fennel.lib.metadata import meta
from fennel.lib.schema import inputs, outputs
from fennel.test_lib import mock_client


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class User:
    id: int = feature(id=1)
    age: float = feature(id=2)

    @extractor
    @inputs(id)
    @outputs(age)
    def user_age(cls, ts: pd.Series, id: pd.Series):
        # Mock age calculation based on user id
        return id * 10


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserPost:
    uid: int = feature(id=1)
    pid: int = feature(id=2)
    score: float = feature(id=3)
    affinity: float = feature(id=4)

    @extractor
    @inputs(uid, pid)
    @outputs(score, affinity)
    def user_post_affinity(cls, ts: pd.Series, uid: pd.Series, pid: pd.Series):
        # Mock affinity calculation based on user id and post id
        return pd.DataFrame(
            {
                "affinity": uid * pid,
                "score": uid * pid * 2,
            }
        )


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class Request:
    ip: str = feature(id=1)


@mock_client
def test_e2e_extraction(client):
    client.sync(featuresets=[User, UserPost, Request])
    # docsnip e2e_extraction
    feature_df = client.extract_features(
        output_feature_list=[
            User.age,
            UserPost.score,
            UserPost.affinity
            # there are 10 features in this list
        ],
        input_feature_list=[
            User.id,
            UserPost.uid,
            UserPost.pid,
            Request.ip,
        ],
        input_dataframe=pd.DataFrame(
            {
                "User.id": [18232, 18234],
                "UserPost.uid": [18232, 18234],
                "UserPost.pid": [32341, 52315],
                "Request.ip": ["1.1.1.1", "2.2.2.2"],
            }
        ),
    )
    # /docsnip
    assert feature_df.shape == (2, 3)
