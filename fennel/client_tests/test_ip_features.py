from datetime import datetime, timedelta
from fennel.datasets import dataset, pipeline, field, Dataset, Count
from fennel.lib import (
    inputs,
)


__owner__ = "eng@fennel.ai"


@dataset(index=True)
class IpFirstUsername:
    ip: str = field(key=True)
    username: str
    ts: datetime

    @pipeline
    @inputs(IovationEvent)
    def compute_first(cls, events: Dataset):
        first_event = events.groupby('ip').first()
        schema = first_event.schema()
        del schema['ip']
        del schema['username']
        del schema['ts']
        print(schema)
        return first_event.drop(list(schema.keys()))


@dataset(index=True)
class IspSharedIps:
    isp: str = field(key=True)
    shared_ips: int
    ts: datetime

    @pipeline
    @inputs(IovationEvent, IpFirstUsername)
    def compute_shared_ip_count(cls, events: Dataset, first_username: Dataset):
        events = events.rename({'username': 'in_username'})  # type: ignore
        joined = events.join(first_username, on=['ip'], how='inner')
        joined = joined.filter(lambda row: row['in_username'] != row['username'])  # type: ignore
        counts = joined.groupby('isp').aggregate(
            shared_ips=Count(of='ip', unique=True, approx=True, window='forever')
        )
        return counts


@dataset(index=True)
class IspIps:
    isp: str = field(key=True)
    ip_count: int
    ts: datetime

    @pipeline
    @inputs(IovationEvent)
    def compute_ip_count(cls, events: Dataset):
        return events.groupby('isp').aggregate(
            ip_count=Count(of='ip', unique=True, approx=True, window='forever')
        )


import pandas as pd

from fennel.featuresets import featureset, extractor
from fennel.lib import inputs, outputs

from datasets.isp import IspSharedIps, IspIps
from featuresets.iovation_features import IovationRegistrationFeatures

__owner__ = "mansikataria@cloud.upwork.com"

@featureset
class IspScoreFeatures:
    isp_score: float

    @extractor(deps=[IspSharedIps, IspIps], version=1)
    @inputs(IovationRegistrationFeatures.isp)
    @outputs("isp_score")
    def extract_isp_score(cls, ts: pd.Series, isps: pd.Series):
        isps_df, found = IspIps.lookup(ts, isp=isps)
        isps_df = isps_df.fillna(0)
        shared_ips_df, found = IspSharedIps.lookup(ts, isp=isps)
        shared_ips_df = shared_ips_df.fillna(0)
        joined_df = pd.concat([isps_df, shared_ips_df], axis=1)
        joined_df['isp_score'] = (joined_df['shared_ips'] / joined_df['ip_count']).fillna(0.0)
        return joined_df['isp_score']

from datetime import datetime, timedelta

from fennel.testing import mock
from pandas.testing import assert_series_equal
import pandas as pd

from datasets.iovation import IovationEvent, IovationEventRaw, IovationEventLog, IovationRegistrationEvent
from datasets.isp import IspSharedIps, IpFirstUsername, IspIps
from featuresets.iovation_features import RequestFeatures, RegistrationFeatures, IovationRegistrationFeatures
from fennel.testing import log

from featuresets.isp import IspScoreFeatures


@mock
def test_isp_features(client):
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    # Sync the dataset
    client.commit(datasets=[IovationEvent,IovationEventRaw,IovationEventLog, IovationRegistrationEvent, IspSharedIps, IpFirstUsername, IspIps],
                  featuresets=[IspScoreFeatures, IovationRegistrationFeatures, RequestFeatures],
                  message="Initial commit")
    log(IovationEventLog, pd.DataFrame({
        'ts': [one_day_ago, one_day_ago, one_day_ago, one_day_ago, one_day_ago],
        'event_id': [2, 2, 2, 0, 2],
        'username': ['abhay', 'alex', 'mattie', 'alex', 'nikhil'],
        'ip': ['myip1', 'myip2', 'myip1', 'myip2', 'myip3'],
        'isp': ['xfinity', 'att', 'xfinity', 'att', 'fennel_isp'],
        # Ignore these data points for this test
        'device': [''] * 5,
        'iov_country': [''] * 5,
        'iov_timezone': [''] * 5,
    }))

    df, found = IspSharedIps.lookup(pd.Series([now, now, now]), isp=pd.Series(["xfinity", "fennel_isp", 'att']))
    df = df.fillna(0)
    df['shared_ips'] = df['shared_ips'].astype(int)
    assert found.tolist() == [True, False, False]
    assert df['shared_ips'].tolist() == [1, 0, 0]

    log(IovationEventLog, pd.DataFrame({
        'ts': [one_day_ago],
        'event_id': [2],
        'username': ['ashwin'],
        'ip': ['myip2'],
        'isp': ['att'],
        # Ignore these data points for this test
        'device': [''] * 1,
        'iov_country': [''] * 1,
        'iov_timezone': [''] * 1,
    }))

    df, found = IspSharedIps.lookup(pd.Series([now, now, now]), isp=pd.Series(["xfinity", "fennel_isp", 'att']))
    df = df.fillna(0)
    df['shared_ips'] = df['shared_ips'].astype(int)
    assert found.tolist() == [True, False, True]
    assert df['shared_ips'].tolist() == [1, 0, 1]

    log(IovationEventLog, pd.DataFrame({
        'ts': [one_day_ago],
        'event_id': [0],
        'username': ['alex'],
        'ip': ['myip2'],
        'isp': ['att'],
        # Ignore these data points for this test
        'device': [''] * 1,
        'iov_country': [''] * 1,
        'iov_timezone': [''] * 1,
    }))

    df, found = IspSharedIps.lookup(pd.Series([now, now, now]), isp=pd.Series(["xfinity", "fennel_isp", 'att']))
    df = df.fillna(0)
    df['shared_ips'] = df['shared_ips'].astype(int)
    assert found.tolist() == [True, False, True]
    assert df['shared_ips'].tolist() == [1, 0, 1]

    log(IovationEventLog, pd.DataFrame({
        'ts': [one_day_ago] * 3,
        'event_id': [2, 2, 0],
        'username': ['aditya', 'daniel', 'abhay'],
        'ip': ['myip3', 'myip5', 'myip3'],
        'isp': ['xfinity', 'att', 'xfinity'],
        'device': [''] * 3,
        'iov_country': [''] * 3,
        'iov_timezone': [''] * 3,
    }))
    df, found = IspSharedIps.lookup(pd.Series([now, now, now]), isp=pd.Series(["xfinity", "fennel_isp", 'att']))
    df = df.fillna(0)
    df['shared_ips'] = df['shared_ips'].astype(int)
    assert found.tolist() == [True, False, True]
    assert df['shared_ips'].tolist() == [2, 0, 1]


@mock
def test_isp_features_sample_data(client):
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    # Sync the dataset
    client.commit(datasets=[IovationEvent, IovationEventRaw, IovationEventLog, IovationRegistrationEvent, IspSharedIps,
                            IpFirstUsername, IspIps],
                  featuresets=[IspScoreFeatures, IovationRegistrationFeatures, RequestFeatures],
                  message="Initial commit")
    df = pd.read_csv('data/iovation_event_json_samples.csv')

    log(IovationEventLog, df)

    df, found = IspSharedIps.lookup(pd.Series([now, now, now]), isp=pd.Series(["xfinity", "fennel_isp", 'att']))
    df = df.fillna(0)
    df['shared_ips'] = df['shared_ips'].astype(int)
    assert found.tolist() == [True, False, False]
    assert df['shared_ips'].tolist() == [1, 0, 0]