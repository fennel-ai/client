import numpy as np
import pytest
from typing import List, Optional
import unittest
from datetime import datetime

import pandas as pd
import os

from fennel.testing import mock

__owner__ = "nikhil@fennel.ai"


class TesUnionSnips(unittest.TestCase):
    @mock
    def test_basic(self, client):
        os.environ["KAFKA_USERNAME"] = "username"
        os.environ["KAFKA_PASSWORD"] = "password"
        # docsnip basic
        from fennel.datasets import dataset, pipeline, Dataset
        from fennel.lib import inputs
        from fennel.connectors import source, S3, Kafka

        cutoff = datetime(2024, 1, 1, 0, 0, 0)
        s3 = S3(name="mys3")
        bucket = s3.bucket("data", path="orders")
        kafka = Kafka(
            name="my_kafka",
            bootstrap_servers="localhost:9092",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=os.environ["KAFKA_USERNAME"],
            sasl_plain_password=os.environ["KAFKA_PASSWORD"],
        )

        @source(bucket, cdc="append", disorder="2d", until=cutoff)
        @dataset
        class OrdersBackfill:
            uid: int
            skuid: int
            timestamp: datetime

        @source(kafka.topic("order"), cdc="append", disorder="1d", since=cutoff)
        @dataset
        class OrdersLive:
            uid: int
            skuid: int
            timestamp: datetime

        @dataset
        class Union:
            uid: int
            skuid: int
            timestamp: datetime

            @pipeline
            @inputs(OrdersBackfill, OrdersLive)
            def explode_pipeline(cls, backfill: Dataset, live: Dataset):
                # docsnip-highlight next-line
                return backfill + live

        # /docsnip

        client.commit(
            message="some msg", datasets=[OrdersBackfill, OrdersLive, Union]
        )
