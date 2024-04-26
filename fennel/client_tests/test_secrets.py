from datetime import datetime, timezone

import pandas as pd
import pytest

from fennel.connectors import source, Webhook
from fennel.datasets import Dataset, dataset, field, pipeline
from fennel.lib import secrets, inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "nitin@fennel.ai"


@dataset
@source(webhook.endpoint("PublicURLs"), disorder="14d", cdc="upsert")
class PublicURLs:
    url: str = field(key=True)
    created_at: datetime


@dataset(index=True)
class PublicURLsTransformed:
    url: str = field(key=True)
    token: str
    created_at: datetime

    @pipeline
    @inputs(PublicURLs)
    def pipeline(cls, event: Dataset):
        return event.assign(
            "token", str, lambda x: x["url"].apply(secrets.get)  # type: ignore
        )


@pytest.mark.integration
@mock
def test_secrets(client):
    # Adding secrets
    client.add_secret("chat.openai.com", "xyz")
    client.add_secret("maps.google.com", "abc")

    # Sync the dataset
    client.commit(message="msg", datasets=[PublicURLs, PublicURLsTransformed])
    now = datetime.now(timezone.utc)
    df = pd.DataFrame(
        {
            "url": ["chat.openai.com", "maps.google.com"],
            "created_at": [now, now],
        }
    )

    # Logging data
    client.log("fennel_webhook", "PublicURLs", df)

    if client.is_integration_client():
        client.sleep()

    # Lookup
    res, found = client.lookup(
        PublicURLsTransformed,
        keys=pd.DataFrame(
            {"url": ["chat.openai.com", "maps.google.com", "fennel.ai"]}
        ),
    )

    assert res.shape == (3, 3)
    assert res["token"].tolist() == ["xyz", "abc", pd.NA]

    # test delete
    client.delete_secret("chat.openai.com")

    if not client.is_integration_client():
        with pytest.raises(KeyError) as e:
            client.get_secret("chat.openai.com")
        assert str(e.value) == "'Secret : `chat.openai.com` does not exist.'"
    else:
        with pytest.raises(Exception) as e:
            client.get_secret("chat.openai.com")
        assert "Server returned: 500, Failed to get user secret mapping" in str(
            e.value
        )
