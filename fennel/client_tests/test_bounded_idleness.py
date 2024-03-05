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


def _log_clicks_data_batch1(client, webhook_endpoint):
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
    response = client.log("fennel_webhook", webhook_endpoint, df)

    assert response.status_code == requests.codes.OK, response.json()


def _log_clicks_data_batch2(client, webhook_endpoint):
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
    response = client.log("fennel_webhook", webhook_endpoint, df)

    assert response.status_code == requests.codes.OK, response.json()


def _log_clicks_data_batch3(client, webhook_endpoint):
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
    response = client.log("fennel_webhook", webhook_endpoint, df)

    assert response.status_code == requests.codes.OK, response.json()


@pytest.mark.integration
@mock
def test_idleness_for_bounded_source(client):
    client.commit(datasets=[BoundedClicksDS], featuresets=[])

    # Log data
    _log_clicks_data_batch1(client, "ClicksDS1")
    client.sleep()

    now = datetime.now()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 2, 4])

    df, _ = BoundedClicksDS.lookup(ts, display_id=display_id_keys)

    assert df["ad_id"].to_list() == [2, 3, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 2 seconds and log more data
    time.sleep(2)
    _log_clicks_data_batch2(client, "ClicksDS1")
    client.sleep()

    now = datetime.now()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([4, 5, 6])

    df, _ = BoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [5, 6, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 5s so that new data is not logged
    time.sleep(5)
    _log_clicks_data_batch3(client, "ClicksDS1")
    client.sleep()

    now = datetime.now()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 6, 7])

    df, _ = BoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [2, None, None]
    assert df["clicked"].to_list() == [True, None, None]


@pytest.mark.integration
@mock
def test_idleness_for_unbounded_source(client):
    client.commit(datasets=[UnBoundedClicksDS], featuresets=[])

    # Log data
    _log_clicks_data_batch1(client, "ClicksDS2")
    client.sleep()

    now = datetime.now()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 2, 4])

    df, _ = UnBoundedClicksDS.lookup(ts, display_id=display_id_keys)

    assert df["ad_id"].to_list() == [2, 3, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 2 seconds and log more data
    time.sleep(2)
    _log_clicks_data_batch2(client, "ClicksDS2")
    client.sleep()

    now = datetime.now()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([4, 5, 6])

    df, _ = UnBoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [5, 6, None]
    assert df["clicked"].to_list() == [True, False, None]

    # Sleep for 5s so that new data is not logged
    time.sleep(5)
    _log_clicks_data_batch3(client, "ClicksDS2")
    client.sleep()

    now = datetime.now()
    ts = pd.Series([now, now, now])
    display_id_keys = pd.Series([1, 6, 7])

    df, _ = UnBoundedClicksDS.lookup(ts, display_id=display_id_keys)
    assert df["ad_id"].to_list() == [2, 7, 8]
    assert df["clicked"].to_list() == [True, True, False]
