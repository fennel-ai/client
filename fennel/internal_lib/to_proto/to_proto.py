from __future__ import annotations

import inspect
import json
from datetime import datetime
from textwrap import dedent, indent
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Mapping,
    Set,
    Callable,
)

import google.protobuf.duration_pb2 as duration_proto  # type: ignore
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.wrappers_pb2 import StringValue

import fennel.connectors as connectors
import fennel.gen.connector_pb2 as connector_proto
import fennel.gen.dataset_pb2 as ds_proto
import fennel.gen.expectations_pb2 as exp_proto
import fennel.gen.featureset_pb2 as fs_proto
import fennel.gen.http_auth_pb2 as http_auth_proto
import fennel.gen.kinesis_pb2 as kinesis_proto
import fennel.gen.metadata_pb2 as metadata_proto
import fennel.gen.pycode_pb2 as pycode_proto
import fennel.gen.schema_pb2 as schema_proto
import fennel.gen.schema_registry_pb2 as schema_registry_proto
import fennel.gen.services_pb2 as services_proto
from fennel.connectors import kinesis
from fennel.connectors.connectors import CSV, SnapshotData
from fennel.datasets import Dataset, Pipeline, Field
from fennel.datasets.datasets import (
    indices_from_ds,
)
from fennel.dtypes.dtypes import FENNEL_STRUCT
from fennel.expr.expr import Expr, TypedExpr
from fennel.expr.serializer import ExprSerializer
from fennel.expr.visitor import ExprPrinter
from fennel.featuresets import Featureset, Feature, Extractor, ExtractorType
from fennel.internal_lib.duration import (
    Duration,
    duration_to_timedelta,
)
from fennel.internal_lib.schema import (
    get_datatype,
    validate_val_with_dtype,
    get_python_type_from_pd,
)
from fennel.internal_lib.to_proto import Serializer
from fennel.internal_lib.to_proto.source_code import (
    get_featureset_core_code,
    get_dataset_core_code,
    get_all_imports,
    get_featureset_gen_code,
    wrap_function,
)
from fennel.internal_lib.to_proto.source_code import to_includes_proto
from fennel.internal_lib.utils import dtype_to_string
from fennel.lib.includes import FENNEL_INCLUDED_MOD
from fennel.lib.metadata import get_metadata_proto, get_meta_attr, OWNER
from fennel.utils import fennel_get_source


def _cleanup_dict(d) -> Dict[str, Any]:
    return {k: v for k, v in d.items() if v is not None}


def _expectations_to_proto(
    exp: Any, entity_name: str, entity_type: str, entity_version: int
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
            entity_version=entity_version,
        )
    ]


def val_as_json(val: Any) -> str:
    if getattr(val.__class__, FENNEL_STRUCT, False):
        return json.dumps(val.as_json())
    try:
        return json.dumps(val)
    except TypeError:
        return json.dumps(str(val))


# ------------------------------------------------------------------------------
# Sync
# ------------------------------------------------------------------------------


def to_sync_request_proto(
    registered_objs: List[Any],
    message: str,
    env: Optional[str] = None,
) -> services_proto.SyncRequest:
    datasets = []
    pipelines = []
    operators = []
    conn_sources = []
    conn_sinks = []
    external_dbs_by_name = {}
    external_dbs = []
    featuresets = []
    features = []
    extractors = []
    expectations: List[exp_proto.Expectations] = []
    offline_indices = []
    online_indices = []
    featureset_obj_map = {}
    for obj in registered_objs:
        if isinstance(obj, Featureset):
            featureset_obj_map[obj._name] = obj

    for obj in registered_objs:
        if isinstance(obj, Dataset):
            is_source_dataset = hasattr(obj, connectors.SOURCE_FIELD)
            datasets.append(dataset_to_proto(obj))
            pipelines.extend(pipelines_from_ds(obj, env))
            operators.extend(operators_from_ds(obj))
            expectations.extend(expectations_from_ds(obj))

            # Adding indexes
            online_index, offline_index = indices_from_ds(obj)
            if online_index:
                online_indices.append(online_index)
            if offline_index:
                offline_indices.append(offline_index)

            sources = source_proto_from_ds(obj, env)
            for src in sources:
                (ext_db, s) = src
                conn_sources.append(s)
                # dedup external dbs by the name
                # TODO(mohit): Also validate that if the name is the same, there should
                # be no difference in the other fields
                if ext_db.name not in external_dbs_by_name:
                    external_dbs_by_name[ext_db.name] = ext_db
                    external_dbs.append(ext_db)

            db_with_sinks = sinks_from_ds(obj, is_source_dataset, env)

            for ext_db, sinks in db_with_sinks:
                conn_sinks.append(sinks)
                # dedup external dbs by the name
                # TODO(mohit): Also validate that if the name is the same, there should
                # be no difference in the other fields
                if ext_db.name not in external_dbs_by_name:
                    external_dbs_by_name[ext_db.name] = ext_db
                    external_dbs.append(ext_db)

        elif isinstance(obj, Featureset):
            featuresets.append(featureset_to_proto(obj))
            features.extend(features_from_fs(obj))
            extractors.extend(extractors_from_fs(obj, featureset_obj_map, env))
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
        sinks=conn_sinks,
        extdbs=external_dbs,
        expectations=expectations,
        offline_indices=offline_indices,
        online_indices=online_indices,
        message=message,
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
        from fennel.internal_lib.schema import *
        from fennel.dtypes.dtypes import *
        from fennel.params.params import *
        from fennel.datasets.datasets import dataset_lookup
        """
    )
    return ds_proto.CoreDataset(
        name=ds.__name__,
        version=ds._version,
        metadata=get_metadata_proto(ds),
        dsschema=fields_to_dsschema(ds.fields),
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
        is_source_dataset=hasattr(ds, connectors.SOURCE_FIELD),
    )


def fields_to_dsschema(fields: List[Field]) -> schema_proto.DSSchema:
    keys = []
    values = []
    ts = None
    erase_keys = []
    for field in fields:
        if field.key:
            if field.erase_key:
                erase_keys.append(field.name)
            keys.append(_field_to_proto(field))
        elif field.timestamp:
            if field.key or field.erase_key:
                raise ValueError(
                    f"Timestamp {field.name} field cannot be key field."
                )
            if ts is not None:
                raise ValueError(
                    "Multiple timestamp fields are not supported. Please set one of the datetime fields to be the timestamp field."
                )
            ts = field.name
        else:
            if field.erase_key:
                raise ValueError(
                    f"Value {field.name} field cannot be an erase key."
                )
            values.append(_field_to_proto(field))
    return schema_proto.DSSchema(
        keys=schema_proto.Schema(fields=keys),
        values=schema_proto.Schema(fields=values),
        timestamp=str(ts),
        erase_keys=erase_keys,
    )


def _field_metadata(fields: List[Field]) -> Dict[str, metadata_proto.Metadata]:
    return {
        field.name: get_metadata_proto(field) for field in fields if field.name
    }


def _field_to_proto(field: Field) -> schema_proto.Field:
    return schema_proto.Field(
        name=field.name,
        dtype=get_datatype(field.dtype),
    )


def sync_validation_for_datasets(ds: Dataset, env: Optional[str] = None):
    """
    This validation function contains the checks that are run just before the sync call.
    It should only contain checks that are not possible to run during the registration phase/compilation phase.
    """
    pipelines = []
    for pipeline in ds._pipelines:
        if pipeline.env.is_entity_selected(env):
            pipelines.append(pipeline)
    sources = source_from_ds(ds, env)
    if len(sources) > 0 and len(pipelines) > 0:
        raise ValueError(
            f"Dataset `{ds._name}` has a source and pipelines defined. "
            f"Please define either a source or pipelines, not both."
        )

    for src in sources:
        # If source cdc is append and dataset has key fields, raise error
        if src.cdc == "append" and len(ds.key_fields) > 0:
            raise ValueError(
                f"Dataset `{ds._name}` has key fields and source cdc is append. "
                f"Please set source cdc to `upsert` or remove key fields."
            )

        if src.cdc == "upsert" and len(ds.key_fields) == 0:
            raise ValueError(
                f"Dataset `{ds._name}` has no key fields and source cdc is upsert. "
                f"Please set key fields or change source cdc to `append`."
            )

    if len(sources) == 0 and len(pipelines) == 1:
        return

    envs: Set[str] = set()
    for pipeline in pipelines:
        env = pipeline.env.environments  # type: ignore
        if env is None:
            raise ValueError(
                f"Pipeline : `{pipeline.name}` has no env. If there are more than one Pipelines for a dataset, "
                f"please specify env for each of them as there can only be one Pipeline for each env."
            )
        if isinstance(env, list):
            for e in env:
                if e in envs:
                    raise ValueError(
                        f"Pipeline : `{pipeline.name}` mapped to Env : {e} which has more than one "
                        f"pipeline. Please specify only one."
                    )
                else:
                    envs.add(e)
        elif isinstance(env, str):
            if env in envs:
                raise ValueError(
                    f"Pipeline : `{pipeline.name}` mapped to Tier : {env} which has more than one pipeline. "
                    f"Please specify only one."
                )
            else:
                envs.add(env)


def pipelines_from_ds(
    ds: Dataset, env: Optional[str] = None
) -> List[ds_proto.Pipeline]:
    pipelines = []
    for pipeline in ds._pipelines:
        if pipeline.env.is_entity_selected(env):
            pipelines.append(pipeline)
    sync_validation_for_datasets(ds, env)
    if len(pipelines) == 1:
        pipelines[0].active = True
    pipeline_protos = []
    for pipeline in pipelines:
        pipeline_protos.append(_pipeline_to_proto(pipeline, ds))
    return pipeline_protos


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
        ds_version=ds._version,
        # TODO(mohit): Deprecate this field
        signature=pipeline.name,
        metadata=get_metadata_proto(pipeline.func),
        input_dataset_names=[dataset._name for dataset in pipeline.inputs],
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
    return _expectations_to_proto(
        ds.expectations, ds._name, "dataset", ds._version
    )


def _validate_source_pre_proc(
    pre_proc: Dict[str, connectors.PreProcValue], ds: Dataset
):
    timestamp_field = ds.timestamp_field
    dataset_schema = ds.schema()
    for field_name, pre_proc_val in pre_proc.items():
        ds_field = None
        for field in ds._fields:
            if field.name == field_name:
                ds_field = field
                break
        if not ds_field:
            raise ValueError(
                f"Dataset `{ds._name}` has a source with a pre_proc value field `{field_name}`, "
                "but the field is not defined in the dataset."
            )
        # If the pre_proc is a `ref`, then skip - we can't check the value type or if the exists in the dataset
        if isinstance(pre_proc_val, connectors.Ref):
            continue
        elif isinstance(pre_proc_val, connectors.Eval):
            # Cannot set timestamp field through preproc eval
            if field_name == timestamp_field:
                raise ValueError(
                    f"Dataset `{ds._name}` has timestamp field set from assign preproc. Please either ref "
                    f"preproc or set a constant value."
                )

            # Run output col dtype validation for Expr.
            if isinstance(pre_proc_val.eval_type, (Expr, TypedExpr)):
                expr = (
                    pre_proc_val.eval_type
                    if isinstance(pre_proc_val.eval_type, Expr)
                    else pre_proc_val.eval_type.expr
                )
                typed_dtype = (
                    None
                    if isinstance(pre_proc_val.eval_type, Expr)
                    else pre_proc_val.eval_type.dtype
                )
                input_schema = ds.schema()
                if pre_proc_val.additional_schema:
                    for name, dtype in pre_proc_val.additional_schema.items():
                        input_schema[name] = dtype
                expr_type = expr.typeof(input_schema)
                if expr_type != get_python_type_from_pd(
                    dataset_schema[field_name]
                ):
                    printer = ExprPrinter()
                    raise TypeError(
                        f"`{field_name}` is of type `{dtype_to_string(dataset_schema[field_name])}` in Dataset `{ds._name}`, "
                        f"can not be cast to `{dtype_to_string(expr_type)}`. Full expression: `{printer.print(expr.root)}`"
                    )
                if typed_dtype and expr_type != typed_dtype:
                    printer = ExprPrinter()
                    raise TypeError(
                        f"`{field_name}` in Dataset `{ds._name}` can not be cast to `{dtype_to_string(typed_dtype)}`, "
                        f"evaluated dtype is `{dtype_to_string(expr_type)}`. Full expression: `{printer.print(expr.root)}`"
                    )
        else:
            # Else check that the data type matches the field type
            if validate_val_with_dtype(ds_field.dtype, pre_proc_val):  # type: ignore
                raise ValueError(
                    f"Dataset `{ds._name}` has a source with a pre_proc value set for field `{field_name}`, "
                    f"but the field type does not match the pre_proc value `{pre_proc_val}` type."
                )


def source_from_ds(
    ds: Dataset, env: Optional[str] = None
) -> List[connectors.DataConnector]:
    if not hasattr(ds, connectors.SOURCE_FIELD):
        return []

    all_sources: List[connectors.DataConnector] = getattr(
        ds, connectors.SOURCE_FIELD
    )
    return [
        source for source in all_sources if source.envs.is_entity_selected(env)
    ]


def source_proto_from_ds(
    ds: Dataset, env: Optional[str] = None
) -> List[Tuple[connector_proto.ExtDatabase, connector_proto.Source]]:
    """
    Returns the source proto for a dataset if it exists

    :param ds: The dataset to get the source proto for.
    :param source_field: The attr for the source field.
    """
    sources = source_from_ds(ds, env)
    ret = []
    for src in sources:
        if src.pre_proc:
            _validate_source_pre_proc(src.pre_proc, ds)
        ret.append(_conn_to_source_proto(src, ds))
    return ret  # type: ignore


def sinks_from_ds(
    ds: Dataset, is_source_dataset: bool = False, env: Optional[str] = None
) -> List[Tuple[connector_proto.ExtDatabase, connector_proto.Sink]]:
    """
    Returns the sink proto for a dataset if it exists

    :param ds: The dataset to get the sink proto for.
    :param is_source_dataset: Determines whether the dataset is source or not
    :param env: Env of the sink
    """
    if hasattr(ds, connectors.SINK_FIELD):
        all_sinks: List[connectors.DataConnector] = getattr(
            ds, connectors.SINK_FIELD
        )
        filtered_sinks = [
            sink for sink in all_sinks if sink.envs.is_entity_selected(env)
        ]

        if len(filtered_sinks) > 0 and is_source_dataset:
            raise ValueError(
                f"Dataset `{ds._name}` error: Cannot define sinks on a source dataset"
            )

        return [
            _conn_to_sink_proto(sink, ds._name, ds._version)
            for sink in filtered_sinks
        ]

    return []  # type: ignore


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
        from fennel.featuresets import featureset, feature as F
        from fennel.lib.metadata import meta
        from fennel.lib.includes import includes
        from fennel.internal_lib.schema import *
        from fennel.params.params import *
        from fennel.dtypes.dtypes import *
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
        name=f.name,
        metadata=get_metadata_proto(f),
        dtype=get_datatype(f.dtype),
        feature_set_name=f.featureset_name,
    )


def sync_validation_for_extractors(extractors: List[Extractor]):
    """
    This validation function contains the checks that are run just before the sync call.
    It should only contain checks that are not possible to run during the registration phase/compilation phase.
    """
    extracted_features: Set[str] = set()
    for extractor in extractors:
        for feature in extractor.outputs:
            if feature.name in extracted_features:
                raise TypeError(
                    f"Feature `{feature.name}` is "
                    f"extracted by multiple extractors including `{extractor.name}` in featureset `{extractor.featureset}`."
                )
            extracted_features.add(feature.name)


def extractors_from_fs(
    fs: Featureset,
    fs_obj_map: Dict[str, Featureset],
    env: Optional[str] = None,
) -> List[fs_proto.Extractor]:
    extractors = []
    extractor_protos = []
    for extractor in fs._extractors:
        if extractor.envs.is_entity_selected(env):
            extractors.append(extractor)
            extractor_protos.append(
                _extractor_to_proto(extractor, fs, fs_obj_map)
            )
    sync_validation_for_extractors(extractors)
    return extractor_protos


# Feature as input
def feature_to_proto_as_input(f: Feature) -> fs_proto.Input:
    return fs_proto.Input(
        feature=fs_proto.Input.Feature(
            feature_set_name=f.featureset_name,
            name=f.name,
        ),
        dtype=get_datatype(f.dtype),
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
                f"`{extractor.name}` extractor input `{input}` is a Featureset, please use a"
                f"dataframe of features"
            )
        else:
            raise TypeError(
                f"`{extractor.name}` extractor input `{input}` is not a feature or "
                f"a dataframe of features, but of type {type(input)}"
            )

    extractor_field_info = None
    if extractor.extractor_type == ExtractorType.LOOKUP:
        if not extractor.derived_extractor_info:
            raise TypeError(
                f"Lookup extractor `{extractor.name}` must have DatasetLookupInfo"
            )
        extractor_field_info = _to_field_lookup_proto(
            extractor.derived_extractor_info
        )

    proto_expr = None
    if extractor.extractor_type == ExtractorType.EXPR:
        if extractor.expr is None:
            raise TypeError(
                f"Expr extractor `{extractor.name}` must have an expr"
            )
        serializer = ExprSerializer()
        proto_expr = serializer.serialize(extractor.expr.root)

    if extractor.extractor_type == ExtractorType.PY_FUNC:
        metadata = get_metadata_proto(extractor.func)
    else:
        metadata = get_metadata_proto(extractor)

    proto_extractor = fs_proto.Extractor(
        name=extractor.name,
        datasets=[
            dataset._name for dataset in extractor.get_dataset_dependencies()
        ],
        inputs=inputs,
        features=[feature.name for feature in extractor.outputs],
        metadata=metadata,
        version=extractor.version,
        pycode=to_extractor_pycode(extractor, fs, fs_obj_map),
        feature_set_name=extractor.featureset,
        extractor_type=extractor.extractor_type,
        field_info=extractor_field_info,
        expr=proto_expr,
    )

    return proto_extractor


def _check_owner_exists(obj):
    owner = get_meta_attr(obj, "owner")
    if owner is None or owner == "":
        if isinstance(obj, Featureset):
            if not hasattr(obj, OWNER) or getattr(obj, OWNER) is None:
                raise Exception(f"Featureset {obj._name} must have an owner.")
        elif isinstance(obj, Dataset):
            if not hasattr(obj, OWNER) or getattr(obj, OWNER) is None:
                raise Exception(f"Dataset {obj._name} must have an owner.")
        else:
            raise Exception(f"Object {obj.__name__} must have an owner.")


def _to_field_lookup_proto(
    info: Extractor.DatasetLookupInfo,
) -> fs_proto.FieldLookupInfo:
    if info.default is None:
        return fs_proto.FieldLookupInfo(
            field=_field_to_proto(info.field), default_value=json.dumps(None)
        )
    default_val = val_as_json(info.default)

    return fs_proto.FieldLookupInfo(
        field=_field_to_proto(info.field),
        default_value=default_val,
    )


# ------------------------------------------------------------------------------
# Connector
# ------------------------------------------------------------------------------


def _pre_proc_value_to_proto(
    dataset: Dataset,
    column_name: str,
    pre_proc_value: connectors.PreProcValue,
) -> connector_proto.PreProcValue:
    if isinstance(pre_proc_value, connectors.Ref):
        return connector_proto.PreProcValue(
            ref=pre_proc_value.name,
        )
    elif isinstance(pre_proc_value, connectors.Eval):
        if pre_proc_value.additional_schema:
            fields = [
                schema_proto.Field(name=name, dtype=get_datatype(dtype))
                for (name, dtype) in pre_proc_value.additional_schema.items()
            ]
            schema = schema_proto.Schema(fields=fields)
        else:
            schema = None

        if isinstance(pre_proc_value.eval_type, (Expr, TypedExpr)):
            serializer = ExprSerializer()
            if isinstance(pre_proc_value.eval_type, Expr):
                root = pre_proc_value.eval_type.root
            else:
                root = pre_proc_value.eval_type.expr.root
            return connector_proto.PreProcValue(
                eval=connector_proto.PreProcValue.Eval(
                    schema=schema,
                    expr=serializer.serialize(root),
                )
            )
        else:
            return connector_proto.PreProcValue(
                eval=connector_proto.PreProcValue.Eval(
                    schema=schema,
                    pycode=_preproc_assign_to_pycode(
                        dataset, column_name, pre_proc_value.eval_type
                    ),
                )
            )
    else:
        # Get the dtype of the pre_proc value.
        #
        # NOTE: We use the same the utility used to serialize the field types so that the types and their
        # serialization are consistent.
        dtype = get_datatype(type(pre_proc_value))
        if dtype == schema_proto.DataType(
            string_type=schema_proto.StringType()
        ):
            return connector_proto.PreProcValue(
                value=schema_proto.Value(string=pre_proc_value)
            )
        elif dtype == schema_proto.DataType(bool_type=schema_proto.BoolType()):
            return connector_proto.PreProcValue(
                value=schema_proto.Value(bool=pre_proc_value)
            )
        elif dtype == schema_proto.DataType(int_type=schema_proto.IntType()):
            return connector_proto.PreProcValue(
                value=schema_proto.Value(int=pre_proc_value)
            )
        elif dtype == schema_proto.DataType(
            double_type=schema_proto.DoubleType()
        ):
            return connector_proto.PreProcValue(
                value=schema_proto.Value(float=pre_proc_value)
            )
        elif dtype == schema_proto.DataType(
            timestamp_type=schema_proto.TimestampType()
        ):
            ts = Timestamp()
            ts.FromDatetime(pre_proc_value)
            return connector_proto.PreProcValue(
                value=schema_proto.Value(timestamp=ts)
            )
        # TODO(mohit): Extend the pre_proc value proto to support other types
        else:
            raise ValueError(
                f"PreProc value {pre_proc_value} is of type {dtype}, "
                f"which is not currently not supported."
            )


def _pre_proc_to_proto(
    dataset: Dataset, pre_proc: Optional[Dict[str, connectors.PreProcValue]]
) -> Mapping[str, connector_proto.PreProcValue]:
    if pre_proc is None:
        return {}
    proto_pre_proc = {}
    for k, v in pre_proc.items():
        proto_pre_proc[k] = _pre_proc_value_to_proto(dataset, k, v)
    return proto_pre_proc


def _preproc_filter_to_pycode(
    dataset: Dataset, func: Optional[Callable]
) -> Optional[pycode_proto.PyCode]:
    if func is None:
        return None
    return wrap_function(
        dataset._name,
        get_dataset_core_code(dataset),
        "",
        to_includes_proto(func),
        is_filter=True,
    )


def _preproc_assign_to_pycode(
    dataset: Dataset, column_name: str, func: Callable
) -> pycode_proto.PyCode:
    return wrap_function(
        dataset._name,
        get_dataset_core_code(dataset),
        "",
        to_includes_proto(func),
        is_assign=True,
        column_name=column_name,
    )


def _conn_to_source_proto(
    connector: connectors.DataConnector,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if isinstance(connector, connectors.S3Connector):
        return _s3_conn_to_source_proto(connector, dataset)
    elif isinstance(connector, connectors.TableConnector):
        return _table_conn_to_source_proto(connector, dataset)
    elif isinstance(connector, connectors.KafkaConnector):
        return _kafka_conn_to_source_proto(connector, dataset)
    elif isinstance(connector, connectors.WebhookConnector):
        return _webhook_to_source_proto(connector, dataset)
    elif isinstance(connector, connectors.KinesisConnector):
        return _kinesis_conn_to_source_proto(connector, dataset)
    elif isinstance(connector, connectors.PubSubConnector):
        return _pubsub_conn_to_source_proto(connector, dataset)
    else:
        raise ValueError(f"Unknown connector type: {type(connector)}")


def _conn_to_sink_proto(
    connector: connectors.DataConnector,
    dataset_name: str,
    ds_version: int,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if isinstance(connector, connectors.KafkaConnector):
        return _kafka_conn_to_sink_proto(connector, dataset_name, ds_version)
    if isinstance(connector, connectors.S3Connector):
        return _s3_conn_to_sink_proto(connector, dataset_name, ds_version)
    else:
        raise ValueError(
            f"Unsupported connector type: {type(connector)} for sink"
        )


def _webhook_to_source_proto(
    connector: connectors.WebhookConnector,
    dataset: Dataset,
):
    data_source = connector.data_source
    ext_db = connector_proto.ExtDatabase(
        name=data_source.name,
        webhook=connector_proto.Webhook(
            name=data_source.name,
            retention=to_duration_proto(data_source.retention),
        ),
    )
    if not connector.cdc:
        raise ValueError("CDC should always be set for webhook source")
    return (
        ext_db,
        connector_proto.Source(
            table=connector_proto.ExtTable(
                endpoint=connector_proto.WebhookEndpoint(
                    endpoint=connector.endpoint,
                    db=ext_db,
                    duration=to_duration_proto(data_source.retention),
                ),
            ),
            disorder=to_duration_proto(connector.disorder),
            dataset=dataset._name,
            ds_version=dataset._version,
            cdc=to_cdc_proto(connector.cdc),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
        ),
    )


def _kafka_conn_to_source_proto(
    connector: connectors.KafkaConnector,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if not isinstance(data_source, connectors.Kafka):
        raise ValueError("KafkaConnector must have Kafka as data_source")
    if not connector.cdc:
        raise ValueError("CDC should always be set for Kafka source")
    ext_db = _kafka_to_ext_db_proto(
        data_source.name,
        data_source.bootstrap_servers,
        data_source.security_protocol,
        data_source.sasl_mechanism,
        data_source.sasl_plain_username,
        data_source.sasl_plain_password,
    )
    source = connector_proto.Source(
        table=connector_proto.ExtTable(
            kafka_topic=connector_proto.KafkaTopic(
                topic=connector.topic,
                db=ext_db,
                format=to_kafka_format_proto(connector.format),
            ),
        ),
        disorder=to_duration_proto(connector.disorder),
        dataset=dataset._name,
        ds_version=dataset._version,
        cdc=to_cdc_proto(connector.cdc),
        pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
        starting_from=_to_timestamp_proto(connector.since),
        until=_to_timestamp_proto(connector.until),
        bounded=connector.bounded,
        idleness=(
            to_duration_proto(connector.idleness)
            if connector.idleness
            else None
        ),
        filter=_preproc_filter_to_pycode(dataset, connector.where),
    )
    return (ext_db, source)


def _kafka_conn_to_sink_proto(
    connector: connectors.KafkaConnector,
    dataset_name: str,
    ds_version: int,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Sink]:
    data_source = connector.data_source
    if not isinstance(data_source, connectors.Kafka):
        raise ValueError("KafkaConnector must have Kafka as data_source")
    if not connector.cdc:
        raise ValueError("CDC should always be set for kafka sink")
    ext_db = _kafka_to_ext_db_proto(
        data_source.name,
        data_source.bootstrap_servers,
        data_source.security_protocol,
        data_source.sasl_mechanism,
        data_source.sasl_plain_username,
        data_source.sasl_plain_password,
    )
    sink = connector_proto.Sink(
        table=connector_proto.ExtTable(
            kafka_topic=connector_proto.KafkaTopic(
                topic=connector.topic,
                db=ext_db,
                format=to_kafka_format_proto(connector.format),
            ),
        ),
        dataset=dataset_name,
        ds_version=ds_version,
        cdc=to_cdc_proto(connector.cdc),
    )
    return ext_db, sink


def _kafka_to_ext_db_proto(
    name: str,
    bootstrap_servers: str,
    security_protocol: str,
    sasl_mechanism: str,
    sasl_plain_username: Optional[str],
    sasl_plain_password: Optional[str],
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        kafka=connector_proto.Kafka(
            bootstrap_servers=bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            sasl_jaas_config="",
        ),
    )


def _s3_conn_to_source_proto(
    connector: connectors.S3Connector,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if not isinstance(data_source, connectors.S3):
        raise ValueError("S3Connector must have S3 as data_source")
    if not connector.cdc:
        raise ValueError("CDC should always be set for S3 source")
    ext_db = _s3_to_ext_db_proto(
        data_source.name,
        data_source.aws_access_key_id,
        data_source.aws_secret_access_key,
        data_source.role_arn,
    )
    ext_table = _s3_to_ext_table_proto(
        ext_db,
        bucket=connector.bucket_name,
        path_prefix=connector.path_prefix,
        path_suffix=connector.path_suffix,
        format=connector.format,
        presorted=connector.presorted,
        spread=connector.spread,
    )
    source = connector_proto.Source(
        table=ext_table,
        dataset=dataset._name,
        ds_version=dataset._version,
        every=to_duration_proto(connector.every),
        disorder=to_duration_proto(connector.disorder),
        starting_from=_to_timestamp_proto(connector.since),
        until=_to_timestamp_proto(connector.until),
        cursor=None,
        timestamp_field=dataset.timestamp_field,
        cdc=to_cdc_proto(connector.cdc),
        pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
        bounded=connector.bounded,
        idleness=(
            to_duration_proto(connector.idleness)
            if connector.idleness
            else None
        ),
        filter=_preproc_filter_to_pycode(dataset, connector.where),
    )
    return (ext_db, source)


def _renames_to_proto(renames: Optional[Dict[str, str]]) -> Mapping[str, str]:
    if renames is None:
        return {}

    renames = {}
    for k, v in renames.items():
        renames[k] = v
    return renames


def _how_to_proto(
    how: Optional[Literal["incremental", "recreate"] | SnapshotData]
) -> Optional[connector_proto.Style]:
    if how is None:
        return None

    if isinstance(how, str):
        if how == "incremental":
            return connector_proto.Style(
                incremental=connector_proto.Incremental()
            )
        elif how == "recreate":
            return connector_proto.Style(recreate=connector_proto.Recreate())

    if isinstance(how, SnapshotData):
        return connector_proto.Style(
            snapshot=connector_proto.SnapshotData(
                marker=how.marker, num_retain=how.num_retain
            )
        )

    raise ValueError("Invalid value for how to convert to proto {}".format(how))
    ...


def _s3_conn_to_sink_proto(
    connector: connectors.S3Connector,
    dataset_name: str,
    ds_version: int,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Sink]:
    data_source = connector.data_source
    if not isinstance(data_source, connectors.S3):
        raise ValueError("S3Connector must have S3 as data_sink")
    ext_db = _s3_to_ext_db_proto(
        data_source.name,
        data_source.aws_access_key_id,
        data_source.aws_secret_access_key,
        data_source.role_arn,
    )
    ext_table = _s3_to_ext_table_proto(
        ext_db,
        bucket=connector.bucket_name,
        path_prefix=connector.path_prefix,
        path_suffix=connector.path_suffix,
        format=connector.format,
        presorted=connector.presorted,
        spread=connector.spread,
    )
    cdc = to_cdc_proto(connector.cdc) if connector.cdc else None
    sink = connector_proto.Sink(
        table=ext_table,
        dataset=dataset_name,
        ds_version=ds_version,
        cdc=cdc,
        every=to_duration_proto(connector.every),
        how=_how_to_proto(connector.how),
        create=connector.create,
        renames=_renames_to_proto(connector.renames),
        since=_to_timestamp_proto(connector.since),
        until=_to_timestamp_proto(connector.until),
    )
    return ext_db, sink


def _s3_to_ext_db_proto(
    name: str,
    aws_access_key_id: Optional[str],
    aws_secret_access_key: Optional[str],
    role_arn: Optional[str],
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        s3=connector_proto.S3(
            aws_access_key_id=aws_access_key_id or "",
            aws_secret_access_key=aws_secret_access_key or "",
            role_arn=role_arn,
        ),
    )


def _s3_to_ext_table_proto(
    db: connector_proto.ExtDatabase,
    bucket: Optional[str],
    path_prefix: Optional[str],
    path_suffix: Optional[str],
    format: str | CSV,
    presorted: bool,
    spread: Optional[Duration],
) -> connector_proto.ExtTable:
    if bucket is None:
        raise ValueError("bucket must be specified")
    if path_prefix is None:
        raise ValueError("prefix or path must be specified")
    if not path_suffix:
        path_suffix = ""

    delimiter = None
    headers = None

    if isinstance(format, str):
        format = format.lower()
    elif isinstance(format, CSV):
        delimiter = format.delimiter
        headers = format.headers
        format = "csv"

    return connector_proto.ExtTable(
        s3_table=connector_proto.S3Table(
            db=db,
            bucket=bucket,
            path_prefix=path_prefix,
            delimiter=delimiter,
            format=format,
            pre_sorted=presorted,
            path_suffix=path_suffix,
            spread=to_duration_proto(spread),
            headers=headers,
        )
    )


def _table_conn_to_source_proto(
    connector: connectors.TableConnector,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    data_source = connector.data_source
    if isinstance(data_source, connectors.BigQuery):
        return _bigquery_conn_to_source_proto(connector, data_source, dataset)
    elif isinstance(data_source, connectors.Snowflake):
        return _snowflake_conn_to_source_proto(connector, data_source, dataset)
    elif isinstance(data_source, connectors.MySQL):
        return _mysql_conn_to_source_proto(connector, data_source, dataset)
    elif isinstance(data_source, connectors.Postgres):
        return _pg_conn_to_source_proto(connector, data_source, dataset)
    elif isinstance(data_source, connectors.Redshift):
        return _redshift_conn_to_source_proto(connector, data_source, dataset)
    elif isinstance(data_source, connectors.Mongo):
        return _mongo_conn_to_source_proto(connector, data_source, dataset)
    else:
        raise ValueError(f"Unknown data source type: {type(data_source)}")


def _bigquery_conn_to_source_proto(
    connector: connectors.TableConnector,
    data_source: connectors.BigQuery,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for BigQuery source")
    ext_db = _bigquery_to_ext_db_proto(
        data_source.name,
        data_source.project_id,
        data_source.dataset_id,
        # Convert service_account_key to str defined in proto
        json.dumps(data_source.service_account_key),
    )
    ext_table = _bigquery_to_ext_table_proto(
        ext_db,
        table_name=connector.table_name,
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset._name,
            ds_version=dataset._version,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            disorder=to_duration_proto(connector.disorder),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            timestamp_field=dataset.timestamp_field,
            cdc=to_cdc_proto(connector.cdc),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
        ),
    )


def _bigquery_to_ext_db_proto(
    name: str,
    project_id: str,
    dataset_id: str,
    service_account_key: str,
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        bigquery=connector_proto.Bigquery(
            project_id=project_id,
            dataset_id=dataset_id,
            service_account_key=service_account_key,
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


def _redshift_conn_to_source_proto(
    connector: connectors.TableConnector,
    data_source: connectors.Redshift,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for Redshift source")
    ext_db = _redshift_to_ext_db_proto(
        name=data_source.name,
        s3_access_role_arn=data_source.s3_access_role_arn,
        username=data_source.username,
        password=data_source.password,
        host=data_source.host,
        port=data_source.port,
        database=data_source.db_name,
        schema=data_source.src_schema,
    )

    ext_table = _redshift_to_ext_table_proto(
        db=ext_db,
        table_name=connector.table_name,
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset._name,
            ds_version=dataset._version,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            disorder=to_duration_proto(connector.disorder),
            timestamp_field=dataset.timestamp_field,
            cdc=to_cdc_proto(connector.cdc),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
        ),
    )


def _redshift_to_authentication_proto(
    s3_access_role_arn: Optional[str],
    username: Optional[str],
    password: Optional[str],
):
    if s3_access_role_arn:
        return connector_proto.RedshiftAuthentication(
            s3_access_role_arn=s3_access_role_arn
        )
    else:
        return connector_proto.RedshiftAuthentication(
            credentials=connector_proto.Credentials(
                username=username, password=password
            )
        )


def _redshift_to_ext_db_proto(
    name: str,
    s3_access_role_arn: Optional[str],
    username: Optional[str],
    password: Optional[str],
    host: str,
    port: int,
    database: str,
    schema: str,
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        redshift=connector_proto.Redshift(
            host=host,
            port=port,
            database=database,
            schema=schema,
            redshift_authentication=_redshift_to_authentication_proto(
                s3_access_role_arn, username, password
            ),
        ),
    )


def _redshift_to_ext_table_proto(
    db: connector_proto.ExtDatabase, table_name: str
) -> connector_proto.ExtTable:
    return connector_proto.ExtTable(
        redshift_table=connector_proto.RedshiftTable(
            db=db,
            table_name=table_name,
        ),
    )


def _mongo_conn_to_source_proto(
    connector: connectors.TableConnector,
    data_source: connectors.Mongo,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for Mongo source")
    ext_db = _mongo_to_ext_db_proto(
        name=data_source.name,
        host=data_source.host,
        database=data_source.db_name,
        user=data_source.username,
        password=data_source.password,
    )

    ext_table = _mongo_to_ext_table_proto(
        db=ext_db,
        table_name=connector.table_name,
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset._name,
            ds_version=dataset._version,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            disorder=to_duration_proto(connector.disorder),
            timestamp_field=dataset.timestamp_field,
            cdc=to_cdc_proto(connector.cdc),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
        ),
    )


def _mongo_to_ext_db_proto(
    name: str,
    host: str,
    database: str,
    user: str,
    password: str,
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        mongo=connector_proto.Mongo(
            host=host,
            database=database,
            user=user,
            password=password,
        ),
    )


def _mongo_to_ext_table_proto(
    db: connector_proto.ExtDatabase, table_name: str
) -> connector_proto.ExtTable:
    return connector_proto.ExtTable(
        mongo_collection=connector_proto.MongoCollection(
            db=db,
            collection_name=table_name,
        ),
    )


def _pubsub_conn_to_source_proto(
    connector: connectors.PubSubConnector,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for PubSub source")
    data_source = connector.data_source
    ext_db = connector_proto.ExtDatabase(
        name=data_source.name,
        pubsub=connector_proto.PubSub(
            project_id=data_source.project_id,
            # Convert service_account_key to str defined in proto
            service_account_key=json.dumps(data_source.service_account_key),
        ),
    )
    return (
        ext_db,
        connector_proto.Source(
            table=connector_proto.ExtTable(
                pubsub_topic=connector_proto.PubSubTopic(
                    db=ext_db,
                    topic_id=connector.topic_id,
                    format=to_pubsub_format_proto(connector.format),
                ),
            ),
            disorder=to_duration_proto(connector.disorder),
            dataset=dataset._name,
            ds_version=dataset._version,
            cdc=to_cdc_proto(connector.cdc),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
        ),
    )


def _snowflake_conn_to_source_proto(
    connector: connectors.TableConnector,
    data_source: connectors.Snowflake,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for Snowflake source")
    ext_db = _snowflake_to_ext_db_proto(
        name=data_source.name,
        account=data_source.account,
        user=data_source.username,
        password=data_source.password,
        schema=data_source.src_schema,
        warehouse=data_source.warehouse,
        role=data_source.role,
        database=data_source.db_name,
    )
    ext_table = _snowflake_to_ext_table_proto(
        db=ext_db, table_name=connector.table_name
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset._name,
            ds_version=dataset._version,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            disorder=to_duration_proto(connector.disorder),
            timestamp_field=dataset.timestamp_field,
            cdc=to_cdc_proto(connector.cdc),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
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
) -> connector_proto.ExtDatabase:
    return connector_proto.ExtDatabase(
        name=name,
        snowflake=connector_proto.Snowflake(
            account=account,
            user=user,
            password=password,
            schema=schema,
            warehouse=warehouse,
            role=role,
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
    connector: connectors.TableConnector,
    data_source: connectors.MySQL,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for Mysql source")
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
            dataset=dataset._name,
            ds_version=dataset._version,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            disorder=to_duration_proto(connector.disorder),
            timestamp_field=dataset.timestamp_field,
            cdc=to_cdc_proto(connector.cdc),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
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
    connector: connectors.TableConnector,
    data_source: connectors.Postgres,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for Postgres source")
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
            dataset=dataset._name,
            ds_version=dataset._version,
            cursor=connector.cursor,
            every=to_duration_proto(connector.every),
            disorder=to_duration_proto(connector.disorder),
            timestamp_field=dataset.timestamp_field,
            cdc=to_cdc_proto(connector.cdc),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
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
    connector: connectors.KinesisConnector,
    dataset: Dataset,
) -> Tuple[connector_proto.ExtDatabase, connector_proto.Source]:
    if not connector.cdc:
        raise ValueError("CDC should always be set for Kinesis source")
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
        format=connector.format,
    )
    return (
        ext_db,
        connector_proto.Source(
            table=ext_table,
            dataset=dataset._name,
            ds_version=dataset._version,
            disorder=to_duration_proto(connector.disorder),
            cdc=to_cdc_proto(connector.cdc),
            starting_from=_to_timestamp_proto(connector.since),
            until=_to_timestamp_proto(connector.until),
            pre_proc=_pre_proc_to_proto(dataset, connector.pre_proc),
            bounded=connector.bounded,
            idleness=(
                to_duration_proto(connector.idleness)
                if connector.idleness
                else None
            ),
            filter=_preproc_filter_to_pycode(dataset, connector.where),
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
    # init_position is one of "latest", "trim_horizon" or a timestamp
    init_position: str | kinesis.at_timestamp,
    format: str,
) -> connector_proto.ExtTable:
    timestamp = None
    if init_position == "trim_horizon":
        ip = kinesis_proto.InitPosition.TRIM_HORIZON
    elif init_position == "latest":
        ip = kinesis_proto.InitPosition.LATEST
    else:
        # Maintain the `at_timestamp` function for backward compatibility but it's unnecessary
        at_ts = (
            init_position
            if isinstance(init_position, kinesis.at_timestamp)
            else kinesis.at_timestamp(init_position)
        )
        ip = kinesis_proto.InitPosition.AT_TIMESTAMP
        timestamp = Timestamp()
        timestamp.FromDatetime(at_ts())

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
# CDC
# ------------------------------------------------------------------------------


def to_cdc_proto(cdc: str) -> connector_proto.CDCStrategy.ValueType:
    if cdc == "append":
        return connector_proto.CDCStrategy.Append
    if cdc == "upsert":
        return connector_proto.CDCStrategy.Upsert
    if cdc == "debezium":
        return connector_proto.CDCStrategy.Debezium
    if cdc == "native":
        return connector_proto.CDCStrategy.Native
    raise ValueError(f"unknown cdc strategy {cdc}")


# ------------------------------------------------------------------------------
# Format
# ------------------------------------------------------------------------------


def to_pubsub_format_proto(format: str) -> connector_proto.PubSubFormat:
    if format == "json":
        return connector_proto.PubSubFormat(json=connector_proto.JsonFormat())
    raise ValueError(f"Unknown PubSub format: {format}")


def to_kafka_format_proto(
    format: Optional[str | connectors.Avro | connectors.Protobuf],
) -> Optional[connector_proto.KafkaFormat]:
    if format is None or format == "json":
        return connector_proto.KafkaFormat(json=connector_proto.JsonFormat())
    if isinstance(format, connectors.Avro):
        if format.registry != "confluent":
            raise ValueError(f"Registry unsupported: {format.registry}")
        return connector_proto.KafkaFormat(
            avro=connector_proto.AvroFormat(
                schema_registry=schema_registry_proto.SchemaRegistry(
                    registry_provider=schema_registry_proto.RegistryProvider.CONFLUENT,
                    url=format.url,
                    auth=to_auth_proto(
                        format.username, format.password, format.token
                    ),
                )
            )
        )
    if isinstance(format, connectors.Protobuf):
        if format.registry != "confluent":
            raise ValueError(f"Registry unsupported: {format.registry}")
        return connector_proto.KafkaFormat(
            protobuf=connector_proto.ProtobufFormat(
                schema_registry=schema_registry_proto.SchemaRegistry(
                    registry_provider=schema_registry_proto.RegistryProvider.CONFLUENT,
                    url=format.url,
                    auth=to_auth_proto(
                        format.username, format.password, format.token
                    ),
                )
            )
        )

    raise ValueError(f"Unknown kafka format: {format}")


def to_auth_proto(
    username: Optional[str], password: Optional[str], token: Optional[str]
) -> Optional[http_auth_proto.HTTPAuthentication]:
    if username is not None:
        return http_auth_proto.HTTPAuthentication(
            basic=http_auth_proto.BasicAuthentication(
                username=username, password=StringValue(value=password)
            )
        )
    if token is not None:
        return http_auth_proto.HTTPAuthentication(
            token=http_auth_proto.TokenAuthentication(token=token)
        )
    return None


# ------------------------------------------------------------------------------
# Duration
# ------------------------------------------------------------------------------


def to_duration_proto(
    duration: Optional[Duration],
) -> Optional[duration_proto.Duration]:
    if not duration:
        return None
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
    if extractor.extractor_type != ExtractorType.PY_FUNC:
        return None
    if not extractor.func:
        raise TypeError(
            f"extractor {extractor.name} has type PyFunc but no function defined"
        )
    dependencies = []
    extractor_fqn = f"{featureset._name}.{extractor.name}"
    gen_code = ""
    if hasattr(extractor.func, FENNEL_INCLUDED_MOD):
        for f in getattr(extractor.func, FENNEL_INCLUDED_MOD):
            dep = to_includes_proto(f)
            gen_code = "\n" + gen_code + "\n" + dedent(dep.generated_code)
            dependencies.append(dep)

    # Extractor code construction
    for dataset in extractor.get_dataset_dependencies():
        gen_code += get_dataset_core_code(dataset)

    input_fs_added = set()
    for input in extractor.inputs:
        if not isinstance(input, Feature):
            raise ValueError(
                f"Extractor `{extractor_fqn}` must have inputs "
                f"of type Feature, but got `{type(input)}`"
            )
        # Dont add the featureset of the extractor itself
        if input.featureset_name == featureset._name:
            continue
        if input.featureset_name not in input_fs_added:
            input_fs_added.add(input.featureset_name)
            if input.featureset_name not in fs_obj_map:
                raise ValueError(
                    f"Extractor `{extractor_fqn}` has an input "
                    f"feature `{input.name}` from featureset "
                    f"`{input.featureset_name}` which is not committed. "
                    f"Please add the featureset to the commit call."
                )
            gen_code = (
                gen_code
                + get_featureset_gen_code(
                    fs_obj_map[input.featureset_name], fs_obj_map
                )
                + "\n\n"
            )

    extractor_src_code = dedent(inspect.getsource(extractor.func))
    indented_code = indent(extractor_src_code, " " * 4)
    featureset_core_code = get_featureset_gen_code(featureset, fs_obj_map)
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


# ------------------------------------------------------------------------------
# Since / Until
# ------------------------------------------------------------------------------


def _to_timestamp_proto(dt: Optional[datetime]) -> Optional[Timestamp]:
    if dt is None:
        return None
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts
