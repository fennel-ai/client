import pandas as pd

from fennel.featuresets import feature as f, featureset, extractor
from fennel.lib import meta, inputs, outputs
from fennel.testing import mock


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class User:
    id: int
    age: float

    @extractor
    @inputs("id")
    @outputs("age")
    def user_age(cls, ts: pd.Series, id: pd.Series):
        # Mock age calculation based on user id
        return pd.Series(name="age", data=id * 10)


@meta(owner="data-eng-oncall@fennel.ai")
@featureset
class UserPost:
    uid: int
    pid: int
    score: float
    affinity: float

    @extractor
    @inputs("uid", "pid")
    @outputs("score", "affinity")
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
    ip: str


@mock
def test_e2e_extraction(client):
    client.commit(message="msg", featuresets=[User, UserPost, Request])
    # docsnip e2e_extraction
    feature_df = client.query(
        outputs=[
            "User.age",
            "UserPost.score",
            "UserPost.affinity",
            # there are 10 features in this list
        ],
        inputs=[
            "User.id",
            "UserPost.uid",
            "UserPost.pid",
            "Request.ip",
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
