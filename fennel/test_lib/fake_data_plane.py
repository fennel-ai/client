import pandas as pd
from typing import Optional

from fennel.sources import (
    DataSource,
    S3,
    Kafka,
    Postgres,
    BigQuery,
    MySQL,
    Snowflake,
    S3Connector,
    KafkaConnector,
    TableConnector,
)


class ClientDataPlaneBridge:
    def __init__(self, client, data_plane_id: str):
        self.client = client
        self.id = data_plane_id

    def upload(self, df: pd.DataFrame):
        return self.client.process_data_plane_records(self.id, df)


class S3DataPlane:
    def __init__(self, client, src):
        self.client = client
        self.src = src

    def bucket(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: str = ",",
        format: str = "csv",
        cursor: Optional[str] = None,
    ) -> ClientDataPlaneBridge:
        connector = S3Connector(
            self.src,
            bucket_name,
            prefix,
            delimiter,
            format,
            cursor,
        )
        return ClientDataPlaneBridge(self.client, connector.identifier())


class TableDataPlane:
    def __init__(self, client, src):
        self.client = client
        self.src = src

    def table(self, table_name: str, cursor: str = "") -> ClientDataPlaneBridge:
        connector = TableConnector(self.src, table_name, cursor)
        return ClientDataPlaneBridge(self.client, connector.identifier())


class KafkaDataPlane:
    def __init__(self, client, src):
        self.client = client
        self.src = src

    def topic(self, topic_name: str) -> ClientDataPlaneBridge:
        connector = KafkaConnector(self.src, topic_name)
        return ClientDataPlaneBridge(self.client, connector.identifier())


class FakeDataPlane:
    def __init__(self, client):
        self.client = client

    def __getitem__(self, src: DataSource):
        if isinstance(src, S3):
            return S3DataPlane(self.client, src)
        elif isinstance(src, Kafka):
            return KafkaDataPlane(self.client, src)
        elif isinstance(src, Postgres):
            return TableDataPlane(self.client, src)
        elif isinstance(src, MySQL):
            return TableDataPlane(self.client, src)
        elif isinstance(src, BigQuery):
            return TableDataPlane(self.client, src)
        elif isinstance(src, Snowflake):
            return TableDataPlane(self.client, src)
        else:
            raise NotImplementedError(f"Unsupported data plane source: {src}")
