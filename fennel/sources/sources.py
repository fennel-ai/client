from __future__ import annotations

import json
from typing import List, Optional, TypeVar, Dict

from pydantic import BaseModel

import fennel.errors as errors
import fennel.gen.source_pb2 as proto
from fennel.lib.duration import Duration, duration_to_micros

T = TypeVar('T')
SOURCE_FIELD = '__fennel_data_sources__'
SINK_FIELD = '__fennel_data_sinks__'


def _data_connector_validate(src: DataConnector) -> List[Exception]:
    exceptions = []
    if src.table_name is not None and src.support_single_stream():
        exceptions.append(
            errors.IncorrectSourceException(
                "table must be None since it supports only a single stream"
            )
        )
    elif src.table_name is None and not src.support_single_stream():
        exceptions.append(
            errors.IncorrectSourceException(
                "table must be provided since it supports multiple streams/tables"
            )
        )
    exceptions.extend(src.validate())
    return exceptions


def source(conn: DataConnector, every: Optional[Duration] = None):
    def decorator(dataset_cls: T):
        _data_connector_validate(conn)
        conn.every = every
        if hasattr(dataset_cls, SOURCE_FIELD):
            connectors = getattr(dataset_cls, SOURCE_FIELD)
            connectors.append(conn)
            setattr(dataset_cls, SOURCE_FIELD, connectors)
        else:
            setattr(dataset_cls, SOURCE_FIELD, [conn])
        return dataset_cls

    return decorator


def sink(conn: DataConnector, every: Optional[Duration] = None):
    def decorator(dataset_cls: T):
        _data_connector_validate(conn)
        conn.every = every
        if hasattr(dataset_cls, SINK_FIELD):
            connectors = getattr(dataset_cls, SINK_FIELD)
            connectors.append(conn)
            setattr(dataset_cls, SINK_FIELD, connectors)
        else:
            setattr(dataset_cls, SINK_FIELD, [conn])
        return dataset_cls

    return decorator


class DataConnector(BaseModel):
    name: str
    table_name: Optional[str] = None
    every: Optional[Duration] = None

    def type(self):
        return str(self.__class__.__name__)

    def validate(self) -> List[Exception]:
        pass

    def support_single_stream(self):
        return False


class SQLSource(DataConnector):
    host: str
    db_name: str
    username: str
    password: str
    jdbc_params: Optional[str] = None

    def validate(self) -> List[Exception]:
        exceptions = []
        if not isinstance(self.host, str):
            exceptions.append(TypeError("host must be a string"))
        if not isinstance(self.db_name, str):
            exceptions.append(TypeError("db_name must be a string"))
        if not isinstance(self.username, str):
            exceptions.append(TypeError("username must be a string"))
        if not isinstance(self.password, str):
            exceptions.append(TypeError("password must be a string"))
        if self.jdbc_params is not None and not isinstance(
                self.jdbc_params, str
        ):
            exceptions.append(TypeError("jdbc_params must be a string"))
        return exceptions

    def table(self, table_name: str):
        self.table_name = table_name
        return self


class S3(DataConnector):
    bucket_name: Optional[str]
    path_prefix: Optional[str]
    aws_access_key_id: str
    aws_secret_access_key: str
    src_schema: Optional[Dict[str, str]]
    delimiter: str = ","
    format: str = "csv"

    def support_single_stream(self):
        return True

    def validate(self) -> List[Exception]:
        exceptions = []
        if self.format not in ["csv", "json", "parquet"]:
            exceptions.append(TypeError("format must be csv"))
        if self.delimiter not in [",", "\t", "|"]:
            exceptions.append(
                TypeError("delimiter must be one of [',', '\t', '|']")
            )
        return exceptions

    def to_proto(self):
        source_proto = proto.DataConnector(name=self.name,
            every=duration_to_micros(self.every))
        source_proto.s3.CopyFrom(
            proto.S3(
                bucket=self.bucket_name,
                path_prefix=self.path_prefix,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                schema=self.src_schema,
                delimiter=self.delimiter,
                format=self.format,
            )
        )
        return source_proto

    def bucket(self, bucket_name: str, prefix: str, src_schema: Dict[str, str],
               delimiter: str = ",", format: str = "csv"):
        self.bucket_name = bucket_name
        self.path_prefix = prefix
        self.src_schema = src_schema
        self.delimiter = delimiter
        self.format = format
        return self


class BigQuery(DataConnector):
    project_id: str
    dataset_id: str
    credentials_json: str

    def validate(self) -> List[Exception]:
        exceptions = []
        try:
            json.loads(self.credentials_json)
        except Exception as e:
            exceptions.append(e)
        return exceptions

    def table(self, table_name: str):
        self.table_name = table_name
        return self

    def to_proto(self):
        source_proto = proto.DataConnector(name=self.name,
            every=duration_to_micros(self.every))
        source_proto.bigquery.CopyFrom(
            proto.BigQuery(
                project_id=self.project_id,
                dataset=self.dataset_id,
                credentials_json=self.credentials_json,
            )
        )
        return source_proto


class Postgres(SQLSource):
    port: int = 5432

    def to_proto(self):
        source_proto = proto.DataConnector(name=self.name,
            every=duration_to_micros(self.every))
        source_proto.sql.CopyFrom(
            proto.SQL(
                host=self.host,
                db=self.db_name,
                username=self.username,
                password=self.password,
                jdbc_params=self.jdbc_params,
                port=self.port,
            )
        )
        return source_proto


class MySQL(SQLSource):
    port: int = 3306

    def to_proto(self):
        source_proto = proto.DataConnector(name=self.name,
            every=duration_to_micros(self.every))
        source_proto.sql.CopyFrom(
            proto.SQL(
                host=self.host,
                db=self.db_name,
                username=self.username,
                password=self.password,
                jdbc_params=self.jdbc_params,
                port=self.port,
            )
        )
        return source_proto


class Snowflake(DataConnector):
    account: str
    db_name: str
    username: str
    password: str
    warehouse: str
    src_schema: str
    role: str
    jdbc_params: Optional[str] = None

    def to_proto(self):
        source_proto = proto.DataConnector(name=self.name,
            every=duration_to_micros(self.every))
        source_proto.snowflake.CopyFrom(
            proto.Snowflake(
                account=self.account,
                db=self.db_name,
                username=self.username,
                password=self.password,
                jdbc_params=self.jdbc_params,
                warehouse=self.warehouse,
                schema=self.src_schema,
                role=self.role,
            )
        )
        return source_proto

    def table(self, table_name: str):
        self.table_name = table_name
        return self

    def validate(self) -> List[Exception]:
        exceptions = []
        if not isinstance(self.account, str):
            exceptions.append(TypeError("account must be a string"))
        if not isinstance(self.db_name, str):
            exceptions.append(TypeError("db_name must be a string"))
        if not isinstance(self.username, str):
            exceptions.append(TypeError("username must be a string"))
        if not isinstance(self.password, str):
            exceptions.append(TypeError("password must be a string"))
        if not isinstance(self.warehouse, str):
            exceptions.append(TypeError("warehouse must be a string"))
        if not isinstance(self.src_schema, str):
            exceptions.append(TypeError("src_schema must be a string"))
        if not isinstance(self.role, str):
            exceptions.append(TypeError("role must be a string"))
        if self.jdbc_params is not None and not isinstance(
                self.jdbc_params, str
        ):
            exceptions.append(TypeError("jdbc_params must be a string"))
        return exceptions
