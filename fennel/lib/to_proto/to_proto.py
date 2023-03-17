from __future__ import annotations

import inspect
import json
from typing import Any, Dict, List, Optional, Tuple

import google.protobuf.duration_pb2 as duration_proto  # type: ignore

import fennel.gen.connector_pb2 as connector_proto
import fennel.gen.dataset_pb2 as ds_proto
import fennel.gen.expectations_pb2 as exp_proto
import fennel.gen.featureset_pb2 as fs_proto
import fennel.gen.metadata_pb2 as metadata_proto
import fennel.gen.pycode_pb2 as pycode_proto
import fennel.gen.schema_pb2 as schema_proto
import fennel.gen.services_pb2 as services_proto
import fennel.sources as sources
from fennel.datasets import Dataset, Pipeline, Field
from fennel.featuresets import (
    Featureset,
    Feature,
    Extractor,
)
from fennel.lib.duration import (
    Duration,
    duration_to_timedelta,
)
from fennel.lib.metadata import get_metadata_proto, get_meta_attr
from fennel.lib.schema import get_datatype
from fennel.lib.to_proto import Serializer


def _cleanup_dict(d) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _expectations_to_proto(
    exp: Any, entity_name: str, entity_type: str
) -> List[exp_proto.Expectations]:
    if exp is None:
        return []
    exp_protos = []
    for e in exp.expectations:
        exp_protos.append(
            exp_proto.Expectation(
                expectation_type=e[0],
                expectation_kwargs=json.dumps(_cleanup_dict(e[1])),
            )
        )
    return [
        exp_proto.Expectations(
            suite=exp.suite,
            expectations=exp_protos,
            version=exp.version,
            metadata=None,
            entity_name=entity_name,
            e_type=exp_proto.Expectations.EntityType.Dataset
            if entity_type == "dataset"
            else exp_proto.Expectations.EntityType.Featureset,
        )
    ]


# ------------------------------------------------------------------------------
# Sync
# ------------------------------------------------------------------------------
def to_sync_request_proto(
    registered_objs: List[Any],
) -> services_proto.SyncRequest:
    datasets = []
    pipelines = []
    operators = []
    conn_sources = []
    external_dbs_by_name = {}
    external_dbs = []
    featuresets = []
    features = []
    extractors = []
    expectations: List[exp_proto.Expectations] = []
    for obj in registered_objs:
        if isinstance(obj, Dataset):
            datasets.append(dataset_to_proto(obj))
            pipelines.extend(pipelines_from_ds(obj))
            operators.extend(operators_from_ds(obj))
            expectations.extend(expectations_from_ds(obj))
            (ext_dbs, s) = sources_from_ds(obj, sources.SOURCE_FIELD)
            conn_sources.extend(s)
            # dedup external dbs by the name
            for ext_db in ext_dbs:
                # TODO(mohit): Also validate that if the name is the same, there should
                # be no difference in the other fields
                if ext_db.name not in external_dbs_by_name:
                    external_dbs_by_name[ext_db.name] = ext_db
                    external_dbs.append(ext_db)
        elif isinstance(obj, Featureset):
            featuresets.append(featureset_to_proto(obj))
            features.extend(features_from_fs(obj))
            extractors.extend(extractors_from_fs(obj))
        else:
            raise ValueError(f"Unknown object type {type(obj)}")

    return services_proto.SyncRequest(
        datasets=datasets,
        pipelines=pipelines,
        operators=operators,
        feature_sets=featuresets,
        features=features,
        extractors=extractors,
        sources=conn_sources,
        extdbs=external_dbs,
        expectations=expectations,
    )


# ------------------------------------------------------------------------------
# Dataset
# ------------------------------------------------------------------------------


def dataset_to_proto(ds: Dataset) -> ds_proto.CoreDataset:
    _check_owner_exists(ds)
    history = duration_proto.Duration()
    history.FromTimedelta(ds._history)

    # TODO(mohit, aditya): add support for `retention` in Dataset
    retention = duration_proto.Duration()
    retention.FromTimedelta(ds._history)
    return ds_proto.CoreDataset(
        name=ds.__name__,
        metadata=get_metadata_proto(ds),
        dsschema=_fields_to_dsschema(ds.fields),
        history=history,
        retention=retention,
        field_metadata=_field_metadata(ds._fields),
    )


def _fields_to_dsschema(fields: List[Field]) -> schema_proto.DSSchema:
    keys = []
    values = []
    ts = None
    for field in fields:
        if field.key:
            keys.append(_field_to_proto(field))
        elif field.timestamp:
            if ts is not None:
                raise ValueError("Multiple timestamp fields are not supported")
            ts = field.name
        else:
            values.append(_field_to_proto(field))
    return schema_proto.DSSchema(
        keys=schema_proto.Schema(fields=keys),
        values=schema_proto.Schema(fields=values),
        timestamp=str(ts),
    )


def _field_metadata(fields: List[Field]) -> Dict[str, metadata_proto.Metadata]:
    return {field.name: get_metadata_proto(field) for field in fields}


def _field_to_proto(field: Field) -> schema_proto.Field:
    return schema_proto.Field(
        name=field.name,
        dtype=get_datatype(field.dtype),
    )


def pipelines_from_ds(ds: Dataset) -> List[ds_proto.Pipeline]:
    pipelines = []
    for pipeline in ds._pipelines:
        pipelines.append(_pipeline_to_proto(pipeline, ds.__name__))
    return pipelines


def _pipeline_to_proto(
    pipeline: Pipeline, dataset_name: str
) -> ds_proto.Pipeline:
    return ds_proto.Pipeline(
        name=pipeline.name,
        dataset_name=dataset_name,
        # TODO(mohit): Deprecate this field
        signature=pipeline.name,
        metadata=get_metadata_proto(pipeline.func),
        input_dataset_names=[dataset._name for dataset in pipeline.inputs],
        idx=pipeline.id,
    )


def operators_from_ds(ds: Dataset) -> List[ds_proto.Operator]:
    operators = []
    for pipeline in ds._pipelines:
        operators.extend(_operators_from_pipeline(pipeline))
    return operators


def _operators_from_pipeline(pipeline: Pipeline):
    serializer = Serializer(pipeline=pipeline)
    return serializer.serialize()


def expectations_from_ds(ds: Dataset) -> List[exp_proto.Expectations]:
    return _expectations_to_proto(ds.expectations, ds._name, "dataset")


def sources_from_ds(
    ds: Dataset, source_field
) -> Tuple[List[connector_proto.ExtDatabase], List[connector_proto.Source]]:
    if hasattr(ds, source_field):
        ext_dbs = []
        conn_sources = []
        ds_sources: List[sources.DataConnector] = getattr(ds, source_field)
        for source in ds_sources:
            (extdb, s) = _conn_to_source_proto(source, ds._name)
            ext_dbs.append(extdb)
            conn_sources.append(s)
        return (ext_dbs, conn_sources)
    return ([], [])


# ------------------------------------------------------------------------------
# Featureset
# ------------------------------------------------------------------------------


def featureset_to_proto(fs: Featureset) -> fs_proto.CoreFeatureset:
    _check_owner_exists(fs)
    return fs_proto.CoreFeatureset(
        name=fs._name,
        metadata=get_metadata_proto(fs),
    )


def features_from_fs(fs: Featureset) -> List[fs_proto.Feature]:
    features = []
    for feature in fs._features:
        features.append(_feature_to_proto(feature))
    return features


def _feature_to_proto(f: Feature) -> fs_proto.Feature:
    return fs_proto.Feature(
        id=f.id,
        name=f.name,
        metadata=get_metadata_proto(f),
        dtype=get_datatype(f.dtype),
        feature_set_name=f.featureset_name,
    )


def extractors_from_fs(fs: Featureset) -> List[fs_proto.Extractor]:
    extractors = []
    for extractor in fs._extractors:
        extractors.append(_extractor_to_proto(extractor))
    return extractors


# Feature as input
def feature_to_proto_as_input(f: Feature) -> fs_proto.Input:
    return fs_proto.Input(
        feature=fs_proto.Input.Feature(
            feature_set_name=f.featureset_name,
            name=f.name,
        )
    )


# Extractor
def _extractor_to_proto(extractor: Extractor) -> fs_proto.Extractor:
    inputs = []
    for input in extractor.inputs:
        if isinstance(input, Feature):
            inputs.append(feature_to_proto_as_input(input))
        elif type(input) is tuple:
            for f in input:
                inputs.append(feature_to_proto_as_input(f))
        elif isinstance(input, Featureset):
            raise TypeError(
                f"Extractor input {input} is a Featureset, please use a"
                f"a DataFrame of features"
            )
        else:
            raise TypeError(
                f"Extractor input {input} is not a Feature or "
                f"a DataFrame of features, but a {type(input)}"
            )
    return fs_proto.Extractor(
        name=extractor.name,
        datasets=[
            dataset._name for dataset in extractor.get_dataset_dependencies()
        ],
        inputs=inputs,
        features=extractor.output_features,
        metadata=get_metadata_proto(extractor.func),
        version=extractor.version,
        pycode=pycode_proto.PyCode(
            pickled=extractor.pickled_func,
            source_code=inspect.getsource(extractor.func),
        ),
        feature_set_name=extractor.featureset,
    )


def _check_owner_exists(obj):
    owner = get_meta_attr(obj, "owner")
    if owner is None or owner == "":
        if isinstance(obj, Featureset):
            raise Exception(f"Featureset {obj._name} must have an owner.")
        elif isinstance(obj, Dataset):
            raise Exception(f"Dataset {obj._name} must have an owner.")
        else:
            raise Exception(f"Object {obj.__name__} must have an owner.")


# ------------------------------------------------------------------------------
# Connector
# ------------------------------------------------------------------------------
def _conn_to_source_proto(
    connector: sources.DataConnector,
    dataset_name: str,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if isinstance(connector, sources.S3Connector):
        return _s3_conn_to_source_proto(connector, dataset_name)
    elif isinstance(connector, sources.TableConnector):
        return _table_conn_to_source_proto(connector, dataset_name)
    else:
        raise ValueError(f"Unknown connector type: {type(connector)}")


def _s3_conn_to_source_proto(
    connector: sources.S3Connector, dataset_name: str
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if not isinstance(data_source, sources.S3):
        raise ValueError("S3Connector must have S3 as data_source")
    ext_db = _s3_to_ext_db_proto(
        data_source.name,
        data_source.aws_access_key_id,
        data_source.aws_secret_access_key,
    )
    ext_table = _s3_to_ext_table_proto(
        ext_db,
        bucket=connector.bucket_name,
        path_prefix=connector.path_prefix,
        delimiter=connector.delimiter,
        format=connector.format,
    )
    source = connector_proto.Source(
        table=ext_table,
        dataset=dataset_name,
        every=to_duration_proto(connector.every),
        lateness=to_duration_proto(connector.lateness),
        cursor=connector.cursor,
    )
    return (ext_db, source)


def _s3_to_ext_db_proto(
    name: str, aws_access_key_id: str, aws_secret_access_key: str
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        s3=connector_proto.S3(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        ),
    )


def _s3_to_ext_table_proto(
    db: connector_proto.ExtDatabase,
    bucket: Optional[str],
    path_prefix: Optional[str],
    delimiter: str,
    format: str,
) -> connector_proto.ExtTable:
    if bucket is None:
        raise ValueError("bucket must be specified")
    if path_prefix is None:
        raise ValueError("path_prefix must be specified")
    return connector_proto.ExtTable(
        s3_table=connector_proto.S3Table(
            db=db,
            bucket=bucket,
            path_prefix=path_prefix,
            delimiter=delimiter,
            format=format,
        )
    )


def _table_conn_to_source_proto(
    connector: sources.TableConnector, dataset_name: str
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if isinstance(data_source, sources.BigQuery):
        return _bigquery_conn_to_source_proto(
            connector, data_source, dataset_name
        )
    elif isinstance(data_source, sources.Snowflake):
        return _snowflake_conn_to_source_proto(
            connector, data_source, dataset_name
        )
    elif isinstance(data_source, sources.MySQL):
        return _mysql_conn_to_source_proto(connector, data_source, dataset_name)
    elif isinstance(data_source, sources.Postgres):
        return _pg_conn_to_source_proto(connector, data_source, dataset_name)
    else:
        raise ValueError(f"Unknown data source type: {type(data_source)}")


def _bigquery_conn_to_source_proto(
    connector: sources.TableConnector,
    data_source: sources.BigQuery,
    dataset_name: str,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    ext_db = _bigquery_to_ext_db_proto(
        data_source.name,
        data_source.project_id,
        data_source.dataset_id,
        data_source.credentials_json,
    )
    ext_table = _bigquery_to_ext_table_proto(
        ext_db,
        table_name=connector.table_name,
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset_name,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            lateness=to_duration_proto(connector.lateness),
        ),
    )


def _bigquery_to_ext_db_proto(
    name: str, project_id: str, dataset_id: str, credentials_json: str
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        bigquery=connector_proto.Bigquery(
            project_id=project_id,
            dataset=dataset_id,
            credentials_json=credentials_json,
        ),
    )


def _bigquery_to_ext_table_proto(
    db: connector_proto.ExtDatabase, table_name: str
) -> connector_proto.ExtTable:
    return connector_proto.ExtTable(
        bigquery_table=connector_proto.BigqueryTable(
            db=db,
            table_name=table_name,
        ),
    )


def _snowflake_conn_to_source_proto(
    connector: sources.TableConnector,
    data_source: sources.Snowflake,
    dataset_name: str,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    ext_db = _snowflake_to_ext_db_proto(
        name=data_source.name,
        account=data_source.account,
        user=data_source.username,
        password=data_source.password,
        schema=data_source.src_schema,
        warehouse=data_source.warehouse,
        role=data_source.role,
        database=data_source.db_name,
        jbdc_params=data_source.jdbc_params,
    )
    ext_table = _snowflake_to_ext_table_proto(
        db=ext_db, table_name=connector.table_name
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset_name,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            lateness=to_duration_proto(connector.lateness),
        ),
    )


def _snowflake_to_ext_db_proto(
    name: str,
    account: str,
    user: str,
    password: str,
    schema: str,
    warehouse: str,
    role: str,
    database: str,
    jbdc_params: Optional[str] = None,
) -> connector_proto.ExtDatabase:
    if jbdc_params is None:
        jbdc_params = ""

    return connector_proto.ExtDatabase(
        name=name,
        snowflake=connector_proto.Snowflake(
            account=account,
            user=user,
            password=password,
            schema=schema,
            warehouse=warehouse,
            role=role,
            jdbc_params=jbdc_params,
            database=database,
        ),
    )


def _snowflake_to_ext_table_proto(
    db: connector_proto.ExtDatabase, table_name: str
) -> connector_proto.ExtTable:
    return connector_proto.ExtTable(
        snowflake_table=connector_proto.SnowflakeTable(
            db=db,
            table_name=table_name,
        ),
    )


def _mysql_conn_to_source_proto(
    connector: sources.TableConnector,
    data_source: sources.MySQL,
    dataset_name: str,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if data_source._get:
        ext_db = _mysql_ref_to_ext_db_proto(name=data_source.name)
    else:
        ext_db = _mysql_to_ext_db_proto(
            name=data_source.name,
            host=data_source.host,
            port=data_source.port,
            user=data_source.username,
            password=data_source.password,
            database=data_source.db_name,
            jdbc_params=data_source.jdbc_params,
        )
    ext_table = _mysql_to_ext_table_proto(
        db=ext_db, table_name=connector.table_name
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset_name,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            lateness=to_duration_proto(connector.lateness),
        ),
    )


def _mysql_to_ext_db_proto(
    name: str,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int,
    jdbc_params: Optional[str] = None,
) -> connector_proto.ExtDatabase:
    if jdbc_params is None:
        jdbc_params = ""
    return connector_proto.ExtDatabase(
        name=name,
        mysql=connector_proto.MySQL(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            jdbc_params=jdbc_params,
        ),
    )


def _mysql_to_ext_table_proto(
    db: connector_proto.ExtDatabase, table_name: str
) -> connector_proto.ExtTable:
    return connector_proto.ExtTable(
        mysql_table=connector_proto.MySQLTable(
            db=db,
            table_name=table_name,
        ),
    )


def _mysql_ref_to_ext_db_proto(name: str) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        reference=connector_proto.Reference(
            dbtype=connector_proto.Reference.MYSQL
        ),
    )


def _pg_conn_to_source_proto(
    connector: sources.TableConnector,
    data_source: sources.Postgres,
    dataset_name: str,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if data_source._get:
        ext_db = _pg_ref_to_ext_db_proto(name=data_source.name)
    else:
        ext_db = _pg_to_ext_db_proto(
            name=data_source.name,
            host=data_source.host,
            port=data_source.port,
            user=data_source.username,
            password=data_source.password,
            database=data_source.db_name,
            jdbc_params=data_source.jdbc_params,
        )
    ext_table = _pg_to_ext_table_proto(
        db=ext_db, table_name=connector.table_name
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset_name,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            lateness=to_duration_proto(connector.lateness),
        ),
    )


def _pg_to_ext_db_proto(
    name: str,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int,
    jdbc_params: Optional[str] = None,
) -> connector_proto.ExtDatabase:
    if jdbc_params is None:
        jdbc_params = ""

    return connector_proto.ExtDatabase(
        name=name,
        postgres=connector_proto.Postgres(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
            jdbc_params=jdbc_params,
        ),
    )


def _pg_to_ext_table_proto(
    db: connector_proto.ExtDatabase, table_name: str
) -> connector_proto.ExtTable:
    return connector_proto.ExtTable(
        pg_table=connector_proto.PostgresTable(
            db=db,
            table_name=table_name,
        ),
    )


def _pg_ref_to_ext_db_proto(name: str) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        reference=connector_proto.Reference(
            dbtype=connector_proto.Reference.POSTGRES
        ),
    )


# ------------------------------------------------------------------------------
# Duration
# ------------------------------------------------------------------------------


def to_duration_proto(duration: Duration) -> duration_proto.Duration:
    proto = duration_proto.Duration()
    proto.FromTimedelta(duration_to_timedelta(duration))
    return proto
