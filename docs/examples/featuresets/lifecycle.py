from datetime import datetime
import pandas as pd

from fennel.datasets import dataset, field
from fennel.lib.schema import Series, DataFrame
from fennel.test_lib import mock_client
from fennel.lib.metadata import meta
from fennel.featuresets import feature, featureset, extractor, depends_on


# docsnip featureset_metaflags
@meta(owner="anti-fraud-team@fintech.com")
@featureset
class Movie:
    duration: int = feature(id=1).meta(description="duration in seconds")
    over_2hrs: bool = feature(id=2).meta(owner="laura@fintech.com")

    @extractor
    def func(
        cls, ts: Series[datetime], durations: Series[duration]
    ) -> Series[over_2hrs]:
        return durations > 2 * 3600


# /docsnip


@mock_client
def test_featureset_metaflags(client):
    client.sync(featuresets=[Movie])

    ts = pd.Series([datetime(2020, 1, 1), datetime(2020, 1, 2)])
    durations = pd.Series([3600, 7200, 7201, 10800])
    res = Movie.func(Movie, ts, durations)
    assert res.tolist() == [False, False, True, True]
