import time
from datetime import datetime

import pandas as pd
import pytest

import fennel._vendor.requests as requests
from fennel.datasets import dataset, field
from fennel.sources import Webhook, source
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "saiharsha@fennel.ai"


@source(
    webhook.endpoint("ClicksDS1"),
    cdc="append",
    disorder="1d",
    bounded=True,
    idleness="4s",
)
@dataset
class BoundedClicksDS:
    display_id: int = field(key=True)
    ad_id: int
    clicked: bool
    timestamp: datetime = field(timestamp=True)


@source(
    webhook.endpoint("ClicksDS2"), cdc="append", disorder="1d", bounded=False
)
@dataset
class UnBoundedClicksDS:
    display_id: int = field(key=True)
    ad_id: int
    clicked: bool
    timestamp: datetime = field(timestamp=True)


@mock
def test_idleness_for_bounded_source(client):
    client.commit(
        message="first_commit", datasets=[BoundedClicksDS], featuresets=[]
    )

    # Log 3 rows of data
    now = datetime.utcnow()
    data = [
        {
            "display_id": 1,
            "ad_id": 2,
            "clicked": True,
            "timestamp": now,
        },
        {
            "display_id": 2,
            "ad_id": 3,
            "clicked": False,
            "timestamp": now,
        },
        {
            "display_id": 3,
            "ad_id": 4,
            "clicked": True,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "ClicksDS1", df)
    assert response.status_code == requests.codes.OK, response.json()

    # We should get data for keys 1 and 2 since they are logged above and no data for 4
    now = datetime.utcnow()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 2, 4])
    df, _ = BoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [2, 3, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 2 seconds and log 2 more rows of data
    time.sleep(2)
    now = datetime.utcnow()
    data = [
        {
            "display_id": 4,
            "ad_id": 5,
            "clicked": True,
            "timestamp": now,
        },
        {
            "display_id": 5,
            "ad_id": 6,
            "clicked": False,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "ClicksDS1", df)
    assert response.status_code == requests.codes.OK, response.json()

    # We should get data for keys 4, 5 since they are logged above and no data for 6
    now = datetime.utcnow()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([4, 5, 6])
    df, _ = BoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [5, 6, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 5s so that new data which is not logged is not ingested since idleness for this source is 4s
    time.sleep(5)
    now = datetime.utcnow()
    data = [
        {
            "display_id": 6,
            "ad_id": 7,
            "clicked": True,
            "timestamp": now,
        },
        {
            "display_id": 7,
            "ad_id": 8,
            "clicked": False,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "ClicksDS1", df)
    assert response.status_code == requests.codes.OK, response.json()

    # We should get data for key 1 since they are logged above and no data for 6, 7 since we logged the data after
    # the source is closed
    now = datetime.utcnow()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 6, 7])
    df, _ = BoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [2, None, None]
    assert df["clicked"].to_list() == [True, None, None]


@mock
def test_idleness_for_unbounded_source(client):
    client.commit(
        message="first_commit", datasets=[UnBoundedClicksDS], featuresets=[]
    )

    # Log 3 rows of data
    now = datetime.utcnow()
    data = [
        {
            "display_id": 1,
            "ad_id": 2,
            "clicked": True,
            "timestamp": now,
        },
        {
            "display_id": 2,
            "ad_id": 3,
            "clicked": False,
            "timestamp": now,
        },
        {
            "display_id": 3,
            "ad_id": 4,
            "clicked": True,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "ClicksDS2", df)
    assert response.status_code == requests.codes.OK, response.json()

    # We should get data for keys 1 and 2 since they are logged above and no data for 4
    now = datetime.utcnow()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 2, 4])
    df, _ = UnBoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [2, 3, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 2 seconds and log 2 more rows of data
    time.sleep(2)
    now = datetime.utcnow()
    data = [
        {
            "display_id": 4,
            "ad_id": 5,
            "clicked": True,
            "timestamp": now,
        },
        {
            "display_id": 5,
            "ad_id": 6,
            "clicked": False,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "ClicksDS2", df)
    assert response.status_code == requests.codes.OK, response.json()

    # We should get data for keys 4, 5 since they are logged above and no data for 6
    now = datetime.utcnow()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([4, 5, 6])
    df, _ = UnBoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [5, 6, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 5s and this new data gets ingested since the source is unbounded
    time.sleep(5)
    now = datetime.utcnow()
    data = [
        {
            "display_id": 6,
            "ad_id": 7,
            "clicked": True,
            "timestamp": now,
        },
        {
            "display_id": 7,
            "ad_id": 8,
            "clicked": False,
            "timestamp": now,
        },
    ]
    df = pd.DataFrame(data)
    response = client.log("fennel_webhook", "ClicksDS2", df)
    assert response.status_code == requests.codes.OK, response.json()

    # We should get data for keys 1, 6, 7 since the data is logged above and source is unbounded
    now = datetime.utcnow()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 6, 7])
    df, _ = UnBoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [2, 7, 8]
    assert df["clicked"].to_list() == [True, True, False]
