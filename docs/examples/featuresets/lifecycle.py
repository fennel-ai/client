from datetime import datetime

import pandas as pd

from fennel.featuresets import feature as F, featureset, extractor
from fennel.lib import meta, inputs, outputs
from fennel.testing import mock


# docsnip featureset_metaflags
@meta(owner="anti-fraud-team@fintech.com")
@featureset
class Movie:
    duration: int = F().meta(description="duration in seconds")
    over_2hrs: bool = F().meta(owner="laura@fintech.com")

    @extractor
    @inputs(duration)
    @outputs(over_2hrs)
    def func(cls, ts: pd.Series, durations: pd.Series):
        return pd.Series(name="over_2hrs", data=durations > 2 * 3600)


# /docsnip


@mock
def test_featureset_metaflags(client):
    client.commit(message="some commit msg", featuresets=[Movie])

    ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    durations = pd.Series([3600, 7200, 7201, 10800])
    res = Movie.func(Movie, ts, durations)
    assert res.tolist() == [False, False, True, True]
