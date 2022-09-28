import dataclasses
import json
from typing import Callable, Optional, List

import fennel.errors as errors
import fennel.gen.stream_pb2 as proto
from fennel.utils import check_response
import fennel.errors as errors


@dataclasses.dataclass(frozen=True)
class Source:
    name: str

    def type(self):
        return str(self.__class__.__name__)

    def validate(self) -> List[Exception]:
        pass

    def support_single_stream(self):
        return False


@dataclasses.dataclass(frozen=True)
class SQLSource(Source):
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
        if self.jdbc_params is not None and not isinstance(self.jdbc_params, str):
            exceptions.append(TypeError("jdbc_params must be a string"))
        return exceptions


@dataclasses.dataclass(frozen=True)
class S3(Source):
    bucket: str
    path_prefix: str
    aws_access_key_id: str
    aws_secret_access_key: str
    schema: str
    delimiter: str = ","
    format: str = "csv"

    def support_single_stream(self):
        return True

    def validate(self) -> List[Exception]:
        exceptions = []
        if self.format not in ["csv", "json", "parquet"]:
            exceptions.append(TypeError("format must be csv"))
        if self.delimiter not in [",", "\t", "|"]:
            exceptions.append(TypeError("delimiter must be one of [',', '\t', '|']"))
        return exceptions


@dataclasses.dataclass(frozen=True)
class BigQuery(Source):
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


@dataclasses.dataclass(frozen=True)
class Postgres(SQLSource):
    port: int = 5432


@dataclasses.dataclass(frozen=True)
class MySQL(SQLSource):
    port: int = 3306


def create_grpc_request(src: Source):
    req = proto.CreateSourceRequest(name=src.name)
    if src.type() == "S3":
        req.s3.CopyFrom(proto.S3(
            bucket=src.bucket,
            path_prefix=src.path_prefix,
            aws_access_key_id=src.aws_access_key_id,
            aws_secret_access_key=src.aws_secret_access_key,
            schema=src.schema,
            delimiter=src.delimiter,
            format=src.format))
    elif src.type() == "BigQuery":
        req.bigquery.CopFrom(proto.BigQuery(
            project_id=src.project_id,
            dataset_id=src.dataset_id,
            credentials_json=src.credentials_json))
    elif src.type() == "Postgres" or src.type() == "MySQL":
        req.sql.CopyFrom(proto.SQL(
            sql_type=proto.SQL.SQLType.Postgres if src.type() == "Postgres" else proto.SQL.SQLType.MySQL,
            host=src.host,
            db=src.db_name,
            username=src.username,
            password=src.password,
            port=src.port,
            jdbc_params=src.jdbc_params))
    else:
        raise Exception("Unknown source type")
    return req


def source(src: Source, table: Optional[str] = None):
    def decorator(fn: Callable):
        def ret(*args, **kwargs):
            raise Exception("can not call func")
            return fn(*args, **kwargs)

        def validate() -> List[Exception]:
            exceptions = []
            if table is not None:
                if src.support_single_stream():
                    exceptions.append(
                        errors.IncorrectSourceException("table must be None since it supports only a single stream"))
            else:
                if not src.support_single_stream():
                    exceptions.append(errors.IncorrectSourceException(
                        "table must be provided since it supports multiple streams/tables"))
            exceptions.extend(src.validate())
            exceptions.extend(fn.validate())
            if fn.__code__.co_argcount != 2:
                exceptions.append(Exception("fn must have two arguments, self and pandas dataframe"))
            return exceptions

        setattr(ret, "validate", validate)

        def create_source_request():
            if table is not None:
                if src.support_single_stream():
                    raise Exception("table must be None since it supports only a single stream")
            else:
                if not src.support_single_stream():
                    raise Exception("table must be provided since it supports multiple streams/tables")
            grpc_request = create_grpc_request(src)
            if src.type() == "S3":
                grpc_request.s3.schema = json.dumps(src.schema)
            return grpc_request

        setattr(ret, "create_source_request", create_source_request)

        setattr(ret, "source", src)
        setattr(ret, "table", table)
        setattr(ret, "populator_func", fn)

        return ret

    return decorator
