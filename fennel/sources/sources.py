from __future__ import annotations

import json
from datetime import datetime
from enum import Enum

from typing import Any, Callable, List, Optional, TypeVar

from fennel._vendor.pydantic import BaseModel  # type: ignore
from fennel._vendor.pydantic import validator  # type: ignore
from fennel.lib.duration import (
    Duration,
)

T = TypeVar("T")
SOURCE_FIELD = "__fennel_data_sources__"
SINK_FIELD = "__fennel_data_sinks__"
DEFAULT_EVERY = Duration("30m")
DEFAULT_LATENESS = Duration("1h")
DEFAULT_CDC = "append"


# ------------------------------------------------------------------------------
# source & sink decorators
# ------------------------------------------------------------------------------


def source(
    conn: DataConnector,
    every: Optional[Duration] = None,
    lateness: Optional[Duration] = None,
    cdc: Optional[str] = None,
) -> Callable[[T], Any]:
    if not isinstance(conn, DataConnector):
        if not isinstance(conn, DataSource):
            raise TypeError("Expected a DataSource, found %s" % type(conn))
        raise TypeError(
            f"{conn.name} does not specify required fields "
            f"{', '.join(conn.required_fields())}."
        )

    def decorator(dataset_cls: T):
        conn.every = every if every is not None else DEFAULT_EVERY
        conn.lateness = lateness if lateness is not None else DEFAULT_LATENESS
        conn.cdc = cdc if cdc is not None else DEFAULT_CDC
        if hasattr(dataset_cls, SOURCE_FIELD):
            raise Exception(
                "Multiple sources are not supported in dataset `%s`."
                % dataset_cls.__name__  # type: ignore
            )
        else:
            setattr(dataset_cls, SOURCE_FIELD, conn)
        return dataset_cls

    return decorator


def sink(
    conn: DataConnector, every: Optional[Duration] = None
) -> Callable[[T], Any]:
    def decorator(dataset_cls: T):
        if every is not None:
            conn.every = every
        if hasattr(dataset_cls, SINK_FIELD):
            connectors = getattr(dataset_cls, SINK_FIELD)
            connectors.append(conn)
            setattr(dataset_cls, SINK_FIELD, connectors)
        else:
            setattr(dataset_cls, SINK_FIELD, [conn])
        return dataset_cls

    return decorator


# ------------------------------------------------------------------------------
# DataSources
# ------------------------------------------------------------------------------


class DataSource(BaseModel):
    """DataSources are used to define the source of data for a dataset. They
    primarily contain the credentials for the source and can typically contain
    multiple tables/physical datasets. DataSources can also be defined
    through the console and are identified by a unique name."""

    name: str

    def type(self):
        return str(self.__class__.__name__)

    def required_fields(self) -> List[str]:
        raise NotImplementedError()

    def identifier(self) -> str:
        raise NotImplementedError()


class Webhook(DataSource):
    def required_fields(self) -> List[str]:
        return ["endpoint"]

    def endpoint(self, endpoint: str) -> WebhookConnector:
        return WebhookConnector(self, endpoint)

    def identifier(self) -> str:
        return f"[Webhook: {self.name}]"


class SQLSource(DataSource):
    host: str
    db_name: str
    username: str
    password: str
    jdbc_params: Optional[str] = None
    _get: bool = False

    def required_fields(self) -> List[str]:
        return ["table", "cursor"]


class S3(DataSource):
    aws_access_key_id: Optional[str]
    aws_secret_access_key: Optional[str]

    def bucket(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: str = ",",
        format: str = "csv",
        presorted: bool = False,
    ) -> S3Connector:
        return S3Connector(
            self,
            bucket_name,
            prefix,
            delimiter,
            format,
            presorted,
        )

    def required_fields(self) -> List[str]:
        return ["bucket", "prefix"]

    @staticmethod
    def get(name: str) -> S3:
        return S3(
            name=name,
            _get=True,
            aws_access_key_id="",
            aws_secret_access_key="",
        )

    def identifier(self) -> str:
        return f"[S3: {self.name}]"


class BigQuery(DataSource):
    project_id: str
    dataset_id: str
    credentials_json: str

    @validator("credentials_json")
    def validate_json(cls, v: str) -> str:
        try:
            json.loads(v)
        except Exception:
            raise ValueError("can't deserialize json")
        return v

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    def required_fields(self) -> List[str]:
        return ["table", "cursor"]

    @staticmethod
    def get(name: str) -> BigQuery:
        return BigQuery(
            name=name,
            _get=True,
            project_id="",
            dataset_id="",
            credentials_json="",
        )

    def identifier(self) -> str:
        return f"[BigQuery: {self.name}]"


class Avro(BaseModel):
    registry: str
    url: str
    username: Optional[str]
    password: Optional[str]
    token: Optional[str]


class Kafka(DataSource):
    bootstrap_servers: str
    security_protocol: str
    sasl_mechanism: Optional[str]
    sasl_plain_username: Optional[str]
    sasl_plain_password: Optional[str]
    verify_cert: Optional[bool]

    @validator("security_protocol")
    def validate_security_protocol(cls, security_protocol: str) -> str:
        if security_protocol not in [
            "PLAINTEXT",
            "SASL_PLAINTEXT",
            "SASL_SSL",
        ]:
            raise ValueError(
                "security protocol must be one of "
                "PLAINTEXT, SASL_PLAINTEXT, SASL_SSL"
            )
        return security_protocol

    def required_fields(self) -> List[str]:
        return ["topic"]

    def topic(
        self, topic_name: str, format: Optional[str | Avro] = None
    ) -> KafkaConnector:
        return KafkaConnector(self, topic_name, format)

    @staticmethod
    def get(name: str) -> Kafka:
        return Kafka(
            name=name,
            _get=True,
            bootstrap_servers="",
            security_protocol="PLAINTEXT",
            sasl_mechanism="",
            sasl_plain_username="",
            sasl_plain_password="",
            verify_cert=None,
        )

    def identifier(self) -> str:
        return f"[Kafka: {self.name}]"


class Postgres(SQLSource):
    port: int = 5432

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    @staticmethod
    def get(name: str) -> Postgres:
        return Postgres(
            name=name,
            _get=True,
            host="",
            db_name="",
            username="",
            password="",
        )

    def identifier(self) -> str:
        return f"[Postgres: {self.name}]"


class MySQL(SQLSource):
    port: int = 3306

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    @staticmethod
    def get(name: str) -> MySQL:
        return MySQL(
            name=name,
            _get=True,
            host="",
            db_name="",
            username="",
            password="",
        )

    def identifier(self) -> str:
        return f"[MySQL: {self.name}]"


class Snowflake(DataSource):
    account: str
    db_name: str
    username: str
    password: str
    warehouse: str
    src_schema: str
    role: str
    jdbc_params: Optional[str] = None

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    def required_fields(self) -> List[str]:
        return ["table", "cursor"]

    @staticmethod
    def get(name: str) -> Snowflake:
        return Snowflake(
            name=name,
            _get=True,
            account="",
            db_name="",
            username="",
            password="",
            warehouse="",
            src_schema="",
            role="",
        )

    def identifier(self) -> str:
        return f"[Snowflake: {self.name}]"


class Kinesis(DataSource):
    role_arn: str
    _get: bool = False

    def stream(
        self,
        stream_arn: str,
        init_position: InitPosition,
        init_timestamp: Optional[datetime] = None,
        format: str = "json",
    ) -> KinesisConnector:
        return KinesisConnector(
            self, stream_arn, init_position, init_timestamp, format
        )

    @staticmethod
    def get(name: str) -> Kinesis:
        return Kinesis(
            name=name,
            _get=True,
            role_arn="",
        )

    def identifier(self) -> str:
        return f"[Kinesis: {self.name}]"

    def required_fields(self) -> List[str]:
        return ["role_arn"]


# ------------------------------------------------------------------------------
# DataConnector
# ------------------------------------------------------------------------------
class DataConnector:
    """DataConnector is a fully specified data source or sink. It contains
    all the fields required to fetch data from a source or sink. DataConnectors
    are only created by code and are attached to a dataset."""

    data_source: DataSource
    every: Duration
    lateness: Duration
    cdc: str

    def identifier(self):
        raise NotImplementedError


class WebhookConnector(DataConnector):
    """
    Webhook is a DataConnector that is push based rather than pull based.
    This connector enables users to push data directly to fennel either using
    the REST API or the Python SDK.
    """

    endpoint: str

    def __init__(self, source, endpoint):
        self.data_source = source
        self.endpoint = endpoint

    def identifier(self) -> str:
        return f"{self.data_source.identifier()}(endpoint={self.endpoint})"


class TableConnector(DataConnector):
    """DataConnectors which only need a table name and a cursor to be
    specified. Includes BigQuery, MySQL, Postgres, and Snowflake."""

    table_name: str
    cursor: str

    def __init__(self, source, table_name, cursor):
        self.data_source = source
        self.table_name = table_name
        self.cursor = cursor

    def identifier(self) -> str:
        return f"{self.data_source.identifier()}(table={self.table_name})"


class KafkaConnector(DataConnector):
    """DataConnectors which only need a topic to be specified. Includes
    Kafka."""

    topic: str
    format: Optional[str | Avro]

    def __init__(self, source, topic, format):
        self.data_source = source
        self.topic = topic
        self.format = format

    def identifier(self) -> str:
        return f"{self.data_source.identifier()}(topic={self.topic}, format={self.format})"


class S3Connector(DataConnector):
    bucket_name: Optional[str]
    path_prefix: Optional[str]
    delimiter: str = ","
    format: str = "csv"
    presorted: bool = False

    def __init__(
        self,
        data_source,
        bucket_name,
        path_prefix,
        delimiter,
        format,
        presorted,
    ):
        self.data_source = data_source
        self.bucket_name = bucket_name
        self.path_prefix = path_prefix
        self.delimiter = delimiter
        self.format = format
        self.presorted = presorted

        if self.format not in ["csv", "json", "parquet", "hudi", "delta"]:
            raise (
                ValueError(
                    "format must be either csv, json, parquet, hudi, delta"
                )
            )
        if self.format == "csv" and self.delimiter not in [",", "\t", "|"]:
            raise (ValueError("delimiter must be one of [',', '\t', '|']"))

    def identifier(self) -> str:
        return (
            f"{self.data_source.identifier()}(bucket={self.bucket_name}"
            f",prefix={self.path_prefix})"
        )


class InitPosition(Enum):
    LATEST = "LATEST"
    TRIM_HORIZON = "TRIM_HORIZON"
    AT_TIMESTAMP = "AT_TIMESTAMP"


class KinesisConnector(DataConnector):
    def __init__(
        self,
        data_source,
        stream_arn: str,
        init_position: InitPosition,
        init_timestamp: Optional[datetime],
        format: str,
    ):
        self.data_source = data_source
        self.init_position = init_position
        if (
            init_position == InitPosition.AT_TIMESTAMP
            and init_timestamp is None
        ):
            raise AttributeError(
                "init_timestamp must be specified for AT_TIMESTAMP init_position"
            )
        elif (
            init_position != InitPosition.AT_TIMESTAMP
            and init_timestamp is not None
        ):
            raise AttributeError(
                "init_timestamp must not be specified for LATEST or TRIM_HORIZON init_position"
            )
        elif (
            init_position == InitPosition.AT_TIMESTAMP
            and init_timestamp is not None
        ):
            # Check if init_timestamp is in the past and is of type datetime
            if not isinstance(init_timestamp, datetime):
                raise AttributeError("init_timestamp must be of type datetime")
            if init_timestamp > datetime.now():
                raise AttributeError("init_timestamp must be in the past")

        self.init_timestamp = init_timestamp
        if format not in ["json"]:
            raise AttributeError("Kinesis format must be json")
        self.format = format
        self.stream_arn = stream_arn

    stream_arn: str
    init_position: InitPosition
    init_timestamp: Optional[datetime]
    format: str = "json"
