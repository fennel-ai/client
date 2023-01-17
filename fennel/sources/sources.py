from __future__ import annotations

import json
from typing import Any, Callable, List, Optional, TypeVar

from pydantic import BaseModel

import fennel.gen.source_pb2 as proto
from fennel.lib.duration import Duration, duration_to_micros

T = TypeVar("T")
SOURCE_FIELD = "__fennel_data_sources__"
SINK_FIELD = "__fennel_data_sinks__"
DEFAULT_EVERY = Duration("30m")


# ------------------------------------------------------------------------------
# source & sink decorators
# ------------------------------------------------------------------------------


def source(
    conn: DataConnector, every: Optional[Duration] = None
) -> Callable[[T], Any]:
    if not isinstance(conn, DataConnector):
        if not isinstance(conn, DataSource):
            raise TypeError("Expected a DataSource, found %s" % type(conn))
        raise TypeError(
            f"{conn.name} does not specify required fields "
            f"{', '.join(conn.required_fields())}."
        )

    def decorator(dataset_cls: T):
        if every is not None:
            conn.every = every
        if hasattr(dataset_cls, SOURCE_FIELD):
            connectors = getattr(dataset_cls, SOURCE_FIELD)
            connectors.append(conn)
            setattr(dataset_cls, SOURCE_FIELD, connectors)
        else:
            setattr(dataset_cls, SOURCE_FIELD, [conn])
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

    def __post_init__(self):
        exceptions = self._validate()
        if len(exceptions) > 0:
            raise Exception(exceptions)

    def type(self):
        return str(self.__class__.__name__)

    def _validate(self) -> List[Exception]:
        raise NotImplementedError()

    def required_fields(self) -> List[str]:
        raise NotImplementedError()


class SQLSource(DataSource):
    host: str
    db_name: str
    username: str
    password: str
    jdbc_params: Optional[str] = None
    _get: bool = False

    def _validate(self) -> List[Exception]:
        exceptions: List[Exception] = []
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

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    def required_fields(self) -> List[str]:
        return ["table", "cursor"]


class S3(DataSource):
    aws_access_key_id: str
    aws_secret_access_key: str

    def _validate(self) -> List[Exception]:
        pass

    def to_proto(self):
        source_proto = proto.DataSource(
            name=self.name,
        )
        source_proto.s3.CopyFrom(
            proto.S3(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        )
        return source_proto

    def bucket(
        self,
        bucket_name: str,
        prefix: str,
        delimiter: str = ",",
        format: str = "csv",
        cursor: Optional[str] = None,
    ) -> S3Connector:
        return S3Connector(
            self,
            bucket_name,
            prefix,
            delimiter,
            format,
            cursor,
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


class BigQuery(DataSource):
    project_id: str
    dataset_id: str
    credentials_json: str

    def _validate(self) -> List[Exception]:
        exceptions = []
        try:
            json.loads(self.credentials_json)
        except Exception as e:
            exceptions.append(e)
        return exceptions

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    def to_proto(self):
        source_proto = proto.DataSource(name=self.name)
        source_proto.bigquery.CopyFrom(
            proto.BigQuery(
                project_id=self.project_id,
                dataset=self.dataset_id,
                credentials_json=self.credentials_json,
            )
        )
        return source_proto

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


class Postgres(SQLSource):
    port: int = 5432

    def to_proto(self):
        if self._get:
            return proto.DataSource(name=self.name, existing=True)

        source_proto = proto.DataSource(
            name=self.name,
        )
        source_proto.sql.CopyFrom(
            proto.SQL(
                host=self.host,
                db=self.db_name,
                username=self.username,
                password=self.password,
                jdbc_params=self.jdbc_params,
                port=self.port,
                sql_type=proto.SQL.Postgres,
            )
        )
        return source_proto

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


class MySQL(SQLSource):
    port: int = 3306

    def to_proto(self):
        if self._get:
            return proto.DataSource(name=self.name, existing=True)

        source_proto = proto.DataSource(name=self.name)
        source_proto.sql.CopyFrom(
            proto.SQL(
                host=self.host,
                db=self.db_name,
                username=self.username,
                password=self.password,
                jdbc_params=self.jdbc_params,
                port=self.port,
                sql_type=proto.SQL.MySQL,
            )
        )
        return source_proto

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


class Snowflake(DataSource):
    account: str
    db_name: str
    username: str
    password: str
    warehouse: str
    src_schema: str
    role: str
    jdbc_params: Optional[str] = None

    def to_proto(self):
        source_proto = proto.DataSource(
            name=self.name,
        )
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

    def table(self, table_name: str, cursor: str) -> TableConnector:
        return TableConnector(self, table_name, cursor)

    def _validate(self) -> List[Exception]:
        exceptions: List[Exception] = []
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


# ------------------------------------------------------------------------------
# DataConnector
# ------------------------------------------------------------------------------


class DataConnector:
    """DataConnector is a fully specified data source or sink. It contains
    all the fields required to fetch data from a source or sink. DataConnectors
    are only created by code and are attached to a dataset."""

    data_source: DataSource
    every: Duration

    def __post_init__(self):
        exceptions = self._validate()
        if len(exceptions) > 0:
            raise Exception(exceptions)

    def _validate(self) -> List[Exception]:
        return []

    def to_proto(self):
        raise NotImplementedError


class TableConnector(DataConnector):
    """DataConnectors which only need a table name and a cursor to be
    specified.Includes BigQuery, MySQL, Postgres, and Snowflake."""

    table: str
    cursor: str

    def __init__(self, data_source, table, cursor):
        self.data_source = data_source
        self.table = table
        self.cursor = cursor
        self.every = DEFAULT_EVERY

    def to_proto(self):
        return proto.DataConnector(
            source=self.data_source.to_proto(),
            cursor=self.cursor,
            every=duration_to_micros(self.every),
            table=self.table,
        )


class S3Connector(DataConnector):
    bucket_name: Optional[str]
    path_prefix: Optional[str]
    delimiter: str = ","
    format: str = "csv"
    cursor: Optional[str] = None

    def __init__(
        self,
        data_source,
        bucket_name,
        path_prefix,
        delimiter,
        format,
        cursor,
    ):
        self.data_source = data_source
        self.bucket_name = bucket_name
        self.path_prefix = path_prefix
        self.delimiter = delimiter
        self.format = format
        self.cursor = cursor
        self.every = DEFAULT_EVERY

    def _validate(self) -> List[Exception]:
        exceptions: List[Exception] = []
        if self.format not in ["csv", "json", "parquet"]:
            exceptions.append(TypeError("format must be csv"))
        if self.delimiter not in [",", "\t", "|"]:
            exceptions.append(
                Exception("delimiter must be one of [',', '\t', '|']")
            )
        return exceptions

    def to_proto(self):
        s3_conn = proto.DataConnector(
            source=self.data_source.to_proto(),
            every=duration_to_micros(self.every),
            s3_connector=proto.S3Connector(
                bucket=self.bucket_name,
                path_prefix=self.path_prefix,
                delimiter=self.delimiter,
                format=self.format,
            ),
        )
        if self.cursor is not None:
            s3_conn.cursor = self.cursor
        return s3_conn
