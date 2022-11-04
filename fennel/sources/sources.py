import json
from typing import Callable, List, Optional

import pandas as pd
from pydantic import BaseModel

import fennel.errors as errors


class Source(BaseModel):
    name: str

    def type(self):
        return str(self.__class__.__name__)

    def validate(self) -> List[Exception]:
        pass

    def support_single_stream(self):
        return False


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
        if self.jdbc_params is not None and not isinstance(
                self.jdbc_params, str
        ):
            exceptions.append(TypeError("jdbc_params must be a string"))
        return exceptions


class S3(Source):
    bucket: str
    path_prefix: str
    aws_access_key_id: str
    aws_secret_access_key: str
    src_schema: str
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


class Postgres(SQLSource):
    port: int = 5432


class MySQL(SQLSource):
    port: int = 3306


def populator(source: Source, table: Optional[str] = None):
    def decorator(fn: Callable):
        # Runs validation checks on the returned value of the
        def ret(cls, *args, **kwargs):
            result = fn(cls, *args, **kwargs)
            if type(result) != pd.DataFrame:
                raise Exception(
                    fn.__name__, " function must return a pandas DataFrame"
                )
            field2types = cls.src_schema.get_fields_and_types()
            # Basic Type Check
            for col, dtype in zip(result.columns, result.dtypes):
                if col not in field2types:
                    raise Exception("Column {} not in schema".format(col))
                if not field2types[col].type_check(dtype):
                    raise Exception(
                        f"Column {col} type mismatch, got {dtype} expected {field2types[col]}"
                    )
            # Deeper Type Check
            for (colname, colvals) in result.items():
                for val in colvals:
                    type_errors = field2types[colname].validate(val)
                    if type_errors:
                        raise Exception(
                            f"Column {colname} value {val} failed validation: {type_errors}"
                        )
            return result

        def validate() -> List[Exception]:
            exceptions = []
            if table is not None:
                if source.support_single_stream():
                    exceptions.append(
                        errors.IncorrectSourceException(
                            "table must be None since it supports only a single stream"
                        )
                    )
            else:
                if not source.support_single_stream():
                    exceptions.append(
                        errors.IncorrectSourceException(
                            "table must be provided since it supports multiple streams/tables"
                        )
                    )
            exceptions.extend(source.validate())
            # exceptions.extend(fn.validate())
            if fn.__code__.co_argcount != 2:
                exceptions.append(
                    Exception(
                        "fn must have two arguments, self and pandas dataframe"
                    )
                )
            return exceptions

        setattr(ret, "validate", validate)

        def create_source_request():
            if table is not None:
                if source.support_single_stream():
                    raise Exception(
                        "table must be None since it supports only a single stream"
                    )
            else:
                if not source.support_single_stream():
                    raise Exception(
                        "table must be provided since it supports multiple streams/tables"
                    )
            grpc_request = None
            if source.type() == "S3":
                grpc_request.s3.src_schema = json.dumps(source.schema)
            return grpc_request

        setattr(ret, "create_source_request", create_source_request)

        setattr(ret, "source", source)
        setattr(ret, "table", table)
        setattr(ret, "populator_func", fn)
        setattr(ret, "func_name", fn.__name__)

        return ret

    return decorator
