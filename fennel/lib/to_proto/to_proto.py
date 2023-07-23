from __future__ import annotations

import inspect
import json
from datetime import datetime
from textwrap import dedent, indent

import google.protobuf.duration_pb2 as duration_proto  # type: ignore
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import BoolValue
from typing import Any, Dict, List, Optional, Tuple

import fennel.gen.connector_pb2 as connector_proto
import fennel.gen.dataset_pb2 as ds_proto
import fennel.gen.expectations_pb2 as exp_proto
import fennel.gen.featureset_pb2 as fs_proto
import fennel.gen.kinesis_pb2 as kinesis_proto
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
from fennel.lib.includes import FENNEL_INCLUDED_MOD
from fennel.lib.metadata import get_metadata_proto, get_meta_attr
from fennel.lib.schema import get_datatype
from fennel.lib.to_proto import Serializer
from fennel.lib.to_proto.source_code import (
    get_featureset_core_code,
    get_dataset_core_code,
    get_all_imports,
)
from fennel.lib.to_proto.source_code import to_includes_proto
from fennel.utils import fennel_get_source


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
    e_type = exp_proto.Expectations.EntityType.Dataset
    if entity_type == "pipeline":
        e_type = exp_proto.Expectations.EntityType.Pipeline
    elif entity_type == "featureset":
        e_type = exp_proto.Expectations.EntityType.Featureset

    return [
        exp_proto.Expectations(
            suite=exp.suite,
            expectations=exp_protos,
            version=exp.version,
            metadata=None,
            entity_name=entity_name,
            e_type=e_type,
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
    featureset_obj_map = {}
    for obj in registered_objs:
        if isinstance(obj, Featureset):
            featureset_obj_map[obj._name] = obj

    for obj in registered_objs:
        if isinstance(obj, Dataset):
            is_source_dataset = hasattr(obj, sources.SOURCE_FIELD)
            if len(obj._pipelines) == 0 and not is_source_dataset:
                raise ValueError(
                    f"Dataset {obj._name} has no pipelines defined. "
                    f"Please define at least one pipeline or mark it as a source "
                    f"data set by defining a @source decorator."
                )

            datasets.append(dataset_to_proto(obj))
            pipelines.extend(pipelines_from_ds(obj))
            operators.extend(operators_from_ds(obj))
            expectations.extend(expectations_from_ds(obj))
            res = sources_from_ds(
                obj, sources.SOURCE_FIELD, obj.timestamp_field
            )
            if res is None:
                continue
            (ext_db, s) = res
            conn_sources.append(s)
            # dedup external dbs by the name
            # TODO(mohit): Also validate that if the name is the same, there should
            # be no difference in the other fields
            if ext_db.name not in external_dbs_by_name:
                external_dbs_by_name[ext_db.name] = ext_db
                external_dbs.append(ext_db)
        elif isinstance(obj, Featureset):
            featuresets.append(featureset_to_proto(obj))
            features.extend(features_from_fs(obj))
            extractors.extend(extractors_from_fs(obj, featureset_obj_map))
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
    imports = dedent(
        """
        from datetime import datetime
        import pandas as pd
        import numpy as np
        from typing import List, Dict, Tuple, Optional, Union, Any
        from fennel.lib.metadata import meta
        from fennel.lib.includes import includes
        from fennel.datasets import *
        from fennel.lib.schema import *
        from fennel.datasets.datasets import dataset_lookup
        """
    )

    return ds_proto.CoreDataset(
        name=ds.__name__,
        metadata=get_metadata_proto(ds),
        dsschema=_fields_to_dsschema(ds.fields),
        history=history,
        retention=retention,
        field_metadata=_field_metadata(ds._fields),
        pycode=pycode_proto.PyCode(
            source_code=fennel_get_source(ds.__fennel_original_cls__),
            generated_code=get_dataset_core_code(ds),
            core_code=get_dataset_core_code(ds),
            entry_point=ds.__name__,
            includes=[],
            ref_includes={},
            imports=imports,
        ),
        is_source_dataset=hasattr(ds, sources.SOURCE_FIELD),
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
        pipelines.append(_pipeline_to_proto(pipeline, ds))
    return pipelines


def _pipeline_to_proto(pipeline: Pipeline, ds: Dataset) -> ds_proto.Pipeline:
    dependencies = []
    gen_code = ""
    if hasattr(pipeline.func, FENNEL_INCLUDED_MOD):
        for f in getattr(pipeline.func, FENNEL_INCLUDED_MOD):
            dep = to_includes_proto(f)
            gen_code = "\n" + dedent(dep.generated_code) + "\n" + gen_code
            dependencies.append(dep)

    pipeline_code = fennel_get_source(pipeline.func)
    gen_code += pipeline_code

    return ds_proto.Pipeline(
        name=pipeline.name,
        dataset_name=ds._name,
        # TODO(mohit): Deprecate this field
        signature=pipeline.name,
        metadata=get_metadata_proto(pipeline.func),
        input_dataset_names=[dataset._name for dataset in pipeline.inputs],
        version=pipeline.version,
        active=pipeline.active,
        pycode=pycode_proto.PyCode(
            source_code=pipeline_code,
            core_code=pipeline_code,
            generated_code=gen_code,
            entry_point=pipeline.name,
            includes=dependencies,
            ref_includes={},
            imports=get_all_imports(),
        ),
    )


def operators_from_ds(ds: Dataset) -> List[ds_proto.Operator]:
    operators = []
    for pipeline in ds._pipelines:
        operators.extend(_operators_from_pipeline(pipeline, ds))
    return operators


def _operators_from_pipeline(pipeline: Pipeline, ds: Dataset):
    serializer = Serializer(pipeline=pipeline, dataset=ds)
    return serializer.serialize()


def expectations_from_ds(ds: Dataset) -> List[exp_proto.Expectations]:
    # Mirror dataset
    if hasattr(ds, sources.SOURCE_FIELD):
        return _expectations_to_proto(ds.expectations, ds._name, "dataset")

    # Derived dataset
    expectations = []
    for pipeline in ds._pipelines:
        expectations.extend(
            _expectations_to_proto(ds.expectations, pipeline.fqn, "pipeline")
        )
    return expectations


def sources_from_ds(
    ds: Dataset, source_field, timestamp_field: str
) -> Optional[Tuple[connector_proto.ExtDatabase, connector_proto.Source]]:
    """
    Returns the source proto for a dataset if it exists

    :param ds: The dataset to get the source proto for.
    :param source_field: The attr for the source field.
    :param timestamp_field: An optional column that can be used to sort the
    data from the source.
    """
    if hasattr(ds, source_field):
        source: sources.DataConnector = getattr(ds, source_field)
        return _conn_to_source_proto(source, ds._name, timestamp_field)
    return None  # type: ignore


# ------------------------------------------------------------------------------
# Featureset
# ------------------------------------------------------------------------------


def featureset_to_proto(fs: Featureset) -> fs_proto.CoreFeatureset:
    _check_owner_exists(fs)
    imports = dedent(
        """
        from datetime import datetime
        import pandas as pd
        import numpy as np
        from typing import List, Dict, Tuple, Optional, Union, Any, no_type_check
        from fennel.featuresets import *
        from fennel.featuresets import featureset, feature
        from fennel.lib.metadata import meta
        from fennel.lib.includes import includes
        from fennel.lib.schema import *
        """
    )

    return fs_proto.CoreFeatureset(
        name=fs._name,
        metadata=get_metadata_proto(fs),
        pycode=pycode_proto.PyCode(
            source_code=fennel_get_source(fs.__fennel_original_cls__),
            core_code=get_featureset_core_code(fs),
            generated_code=get_featureset_core_code(fs),
            entry_point=fs._name,
            includes=[],
            ref_includes={},
            imports=imports,
        ),
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


def extractors_from_fs(
    fs: Featureset, fs_obj_map: Dict[str, Featureset]
) -> List[fs_proto.Extractor]:
    extractors = []
    for extractor in fs._extractors:
        extractors.append(_extractor_to_proto(extractor, fs, fs_obj_map))
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
def _extractor_to_proto(
    extractor: Extractor, fs: Featureset, fs_obj_map: Dict[str, Featureset]
) -> fs_proto.Extractor:
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
        pycode=to_extractor_pycode(extractor, fs, fs_obj_map),
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
    connector: sources.DataConnector, dataset_name: str, timestamp_field: str
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if isinstance(connector, sources.S3Connector):
        return _s3_conn_to_source_proto(
            connector, dataset_name, timestamp_field
        )
    elif isinstance(connector, sources.TableConnector):
        return _table_conn_to_source_proto(
            connector, dataset_name, timestamp_field
        )
    elif isinstance(connector, sources.KafkaConnector):
        return _kafka_conn_to_source_proto(connector, dataset_name)
    elif isinstance(connector, sources.WebhookConnector):
        return _webhook_to_source_proto(connector, dataset_name)
    elif isinstance(connector, sources.KinesisConnector):
        return _kinesis_conn_to_source_proto(connector, dataset_name)
    else:
        raise ValueError(f"Unknown connector type: {type(connector)}")


def _webhook_to_source_proto(
    connector: sources.WebhookConnector, dataset_name: str
):
    data_source = connector.data_source
    ext_db = connector_proto.ExtDatabase(
        name=data_source.name,
        webhook=connector_proto.Webhook(name=data_source.name),
    )
    return (
        ext_db,
        connector_proto.Source(
            table=connector_proto.ExtTable(
                endpoint=connector_proto.WebhookEndpoint(
                    endpoint=connector.endpoint,
                    db=ext_db,
                ),
            ),
            lateness=to_duration_proto(connector.lateness),
            dataset=dataset_name,
        ),
    )


def _kafka_conn_to_source_proto(
    connector: sources.KafkaConnector, dataset_name: str
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if not isinstance(data_source, sources.Kafka):
        raise ValueError("KafkaConnector must have Kafka as data_source")
    ext_db = _kafka_to_ext_db_proto(
        data_source.name,
        data_source.bootstrap_servers,
        data_source.security_protocol,
        data_source.sasl_mechanism,
        data_source.sasl_plain_username,
        data_source.sasl_plain_password,
        data_source.sasl_jaas_config,
        data_source.verify_cert,
    )
    source = connector_proto.Source(
        table=connector_proto.ExtTable(
            kafka_topic=connector_proto.KafkaTopic(
                topic=connector.topic,
                db=ext_db,
            ),
        ),
        lateness=to_duration_proto(connector.lateness),
        dataset=dataset_name,
    )
    return (ext_db, source)


def _kafka_to_ext_db_proto(
    name: str,
    bootstrap_servers: str,
    security_protocol: str,
    sasl_mechanism: Optional[str],
    sasl_plain_username: Optional[str],
    sasl_plain_password: Optional[str],
    sasl_jaas_config: Optional[str],
    verify_cert: Optional[bool],
) -> connector_proto.ExtDatabase:
    if sasl_mechanism is None:
        sasl_mechanism = ""
    if sasl_plain_username is None:
        sasl_plain_username = ""
    if sasl_plain_password is None:
        sasl_plain_password = ""
    if sasl_jaas_config is None:
        sasl_jaas_config = ""

    return connector_proto.ExtDatabase(
        name=name,
        kafka=connector_proto.Kafka(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_jaas_config=sasl_jaas_config,
            enable_ssl_certificate_verification=BoolValue(value=verify_cert),
        ),
    )


def _s3_conn_to_source_proto(
    connector: sources.S3Connector, dataset_name: str, timestamp_field: str
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
        timestamp_field=timestamp_field,
    )
    return (ext_db, source)


def _s3_to_ext_db_proto(
    name: str,
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        s3=connector_proto.S3(
            aws_access_key_id=aws_access_key_id or "",
            aws_secret_access_key=aws_secret_access_key or "",
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
    connector: sources.TableConnector, dataset_name: str, timestamp_field: str
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if isinstance(data_source, sources.BigQuery):
        return _bigquery_conn_to_source_proto(
            connector, data_source, dataset_name, timestamp_field
        )
    elif isinstance(data_source, sources.Snowflake):
        return _snowflake_conn_to_source_proto(
            connector, data_source, dataset_name, timestamp_field
        )
    elif isinstance(data_source, sources.MySQL):
        return _mysql_conn_to_source_proto(
            connector, data_source, dataset_name, timestamp_field
        )
    elif isinstance(data_source, sources.Postgres):
        return _pg_conn_to_source_proto(
            connector, data_source, dataset_name, timestamp_field
        )
    else:
        raise ValueError(f"Unknown data source type: {type(data_source)}")


def _bigquery_conn_to_source_proto(
    connector: sources.TableConnector,
    data_source: sources.BigQuery,
    dataset_name: str,
    timestamp_field: str,
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
            timestamp_field=timestamp_field,
        ),
    )


def _bigquery_to_ext_db_proto(
    name: str,
    project_id: str,
    dataset_id: str,
    credentials_json: str,
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
    timestamp_field: str,
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
            timestamp_field=timestamp_field,
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
    timestamp_field: str,
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
            timestamp_field=timestamp_field,
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
    timestamp_field: str,
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
            timestamp_field=timestamp_field,
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


def _kinesis_conn_to_source_proto(
    connector: sources.KinesisConnector,
    dataset_name: str,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if data_source._get:
        ext_db = _kinesis_ref_to_ext_db_proto(name=data_source.name)
    else:
        ext_db = _kinesis_to_ext_db_proto(
            name=data_source.name,
            role_arn=data_source.role_arn,
        )
    ext_table = _kinesis_to_ext_table_proto(
        db=ext_db,
        stream_arn=connector.stream_arn,
        init_position=connector.init_position,
        init_timestamp=connector.init_timestamp,
        format=connector.format,
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset_name,
            lateness=to_duration_proto(connector.lateness),
        ),
    )


def _kinesis_ref_to_ext_db_proto(name: str) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        reference=connector_proto.Reference(
            dbtype=connector_proto.Reference.KINESIS
        ),
    )


def _kinesis_to_ext_db_proto(
    name: str, role_arn: str
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        kinesis=connector_proto.Kinesis(
            role_arn=role_arn,
        ),
    )


def _kinesis_to_ext_table_proto(
    db: connector_proto.ExtDatabase,
    stream_arn: str,
    init_position: sources.InitPosition,
    init_timestamp: Optional[datetime] = None,
    format: str = "json",
) -> connector_proto.ExtTable:
    if init_position == sources.InitPosition.TRIM_HORIZON:
        ip = kinesis_proto.InitPosition.TRIM_HORIZON
    elif init_position == sources.InitPosition.LATEST:
        ip = kinesis_proto.InitPosition.LATEST
    elif init_position == sources.InitPosition.AT_TIMESTAMP:
        ip = kinesis_proto.InitPosition.AT_TIMESTAMP
    else:
        raise ValueError(f"Unknown init position: {init_position}")

    # Convert init_timestamp to timestamp proto
    if init_timestamp is not None:
        timestamp = Timestamp()
        timestamp.FromDatetime(init_timestamp)
    else:
        timestamp = None
    return connector_proto.ExtTable(
        kinesis_stream=connector_proto.KinesisStream(
            db=db,
            stream_arn=stream_arn,
            init_position=ip,
            init_timestamp=timestamp,
            format=format,
        ),
    )


# ------------------------------------------------------------------------------
# Duration
# ------------------------------------------------------------------------------


def to_duration_proto(duration: Duration) -> duration_proto.Duration:
    proto = duration_proto.Duration()
    proto.FromTimedelta(duration_to_timedelta(duration))
    return proto


# ------------------------------------------------------------------------------
# Includes
# ------------------------------------------------------------------------------


def to_extractor_pycode(
    extractor: Extractor,
    featureset: Featureset,
    fs_obj_map: Dict[str, Featureset],
) -> pycode_proto.PyCode:
    dependencies = []
    gen_code = ""
    if hasattr(extractor.func, FENNEL_INCLUDED_MOD):
        for f in getattr(extractor.func, FENNEL_INCLUDED_MOD):
            dep = to_includes_proto(f)
            gen_code = "\n" + dedent(dep.generated_code) + "\n" + gen_code
            dependencies.append(dep)

    # Extractor code construction
    for dataset in extractor.get_dataset_dependencies():
        gen_code += get_dataset_core_code(dataset)

    input_fs_added = set()
    for input in extractor.inputs:
        if not isinstance(input, Feature):
            raise ValueError(
                f"Extractor {extractor.name} must have inputs "
                f"of type Feature, but got {type(input)}"
            )
        # Dont add the featureset of the extractor itself
        if input.featureset_name == featureset._name:
            continue
        if input.featureset_name not in input_fs_added:
            input_fs_added.add(input.featureset_name)
            if input.featureset_name not in fs_obj_map:
                raise ValueError(
                    f"Extractor {extractor.name} has an input "
                    f"feature {input.name} from featureset "
                    f"{input.featureset_name} which is not synced"
                )
            gen_code = (
                gen_code
                + get_featureset_core_code(fs_obj_map[input.featureset_name])
                + "\n"
            )

    extractor_src_code = dedent(inspect.getsource(extractor.func))
    indented_code = indent(extractor_src_code, " " * 4)
    featureset_core_code = get_featureset_core_code(featureset)
    gen_code = gen_code + "\n" + featureset_core_code + "\n" + indented_code
    ref_includes = {featureset._name: pycode_proto.RefType.Featureset}
    datasets = extractor.get_dataset_dependencies()
    for d in datasets:
        ref_includes[d._name] = pycode_proto.RefType.Dataset

    ret_code = f"""
def {featureset._name}_{extractor.name}(*args, **kwargs):
    x = {featureset._name}.__fennel_original_cls__
    return getattr(x, "{extractor.name}")(*args, **kwargs)
    """

    gen_code = gen_code + ret_code
    return pycode_proto.PyCode(
        source_code=extractor_src_code,
        core_code=extractor_src_code,
        generated_code=gen_code,
        entry_point=f"{featureset._name}_{extractor.name}",
        includes=dependencies,
        ref_includes=ref_includes,
        imports=get_all_imports(),
    )
