import sys
from datetime import datetime, timezone
from decimal import Decimal as PythonDecimal
from typing import Optional

import pandas as pd
import pytest

from fennel._vendor import requests
from fennel.connectors import Webhook, source
from fennel.datasets import (
    dataset,
    field,
    pipeline,
    Dataset,
    Average,
    Sum,
    Max,
    Min,
    Stddev,
    Quantile,
)
from fennel.dtypes import Decimal, Continuous
from fennel.featuresets import featureset, feature as F
from fennel.lib import inputs
from fennel.testing import mock

webhook = Webhook(name="fennel_webhook")
__owner__ = "eng@fennel.ai"


@source(webhook.endpoint("UserInfoDataset"), disorder="14d", cdc="upsert")
@dataset(index=True)
class UserInfoDataset:
    user_id: int = field(key=True)
    name: str
    age: int
    country: str
    net_worth: Decimal[2]
    income: Decimal[2]
    timestamp: datetime = field(timestamp=True)


@mock
def test_gg(client):
    client.commit(datasets=[UserInfoDataset], message="message")
