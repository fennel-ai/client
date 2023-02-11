from __future__ import annotations

import inspect
from typing import (
    cast,
    Callable,
    Dict,
    Optional,
)

import fennel.gen.dataset_pb2 as ds_proto
import fennel.gen.featureset_pb2 as fs_proto
import fennel.gen.services_pb2 as services_proto
from fennel.datasets import Dataset, Pipeline, Field, OnDemand
from fennel.featuresets import (
    Featureset,
    Feature,
    Extractor,
    DEPENDS_ON_DATASETS_ATTR,
)
from fennel.lib.duration.duration import (
    timedelta_to_micros,
    duration_to_micros,
)
from fennel.lib.metadata import get_metadata_proto, get_meta_attr
from fennel.lib.schema import get_datatype
from fennel.lib.to_proto import Serializer
from fennel.sources import SOURCE_FIELD, SINK_FIELD


# ------------------------------------------------------------------------------
# Dataset
# ------------------------------------------------------------------------------


def dataset_to_proto(ds: Dataset) -> services_proto.CreateDatasetRequest:
    _check_owner_exists(ds)
    sources = []
    if hasattr(ds, SOURCE_FIELD):
        sources = getattr(ds, SOURCE_FIELD)
    sinks = []
    if hasattr(ds, SINK_FIELD):
        sinks = getattr(ds, SINK_FIELD)

    return services_proto.CreateDatasetRequest(
        name=ds.__name__,
        history=timedelta_to_micros(ds._history),
        pipelines=[_pipeline_to_proto(p, ds.__name__) for p in ds._pipelines],
        input_connectors=[s.to_proto() for s in sources],
        output_connectors=[s.to_proto() for s in sinks],
        mode="pandas",
        signature=ds.signature(),
        metadata=get_metadata_proto(ds),
        # Currently we don't support versioning of datasets.
        # Kept for future use.
        version=0,
        fields=[_field_to_proto(field) for field in ds._fields],
        on_demand=_on_demand_to_proto(ds._on_demand),
    )


# Field
def _field_to_proto(f: Field) -> ds_proto.Field:
    # Type is passed as part of the dataset schema
    if f.key + f.timestamp > 1:
        raise ValueError("Field cannot be both a key and a timestamp")

    if f.key:
        ftype = ds_proto.FieldType.Key
    elif f.timestamp:
        ftype = ds_proto.FieldType.Timestamp
    else:
        ftype = ds_proto.FieldType.Val

    return ds_proto.Field(
        name=f.name,
        ftype=ftype,
        dtype=get_datatype(f.dtype),
        metadata=get_metadata_proto(f),
    )


# Pipeline
def _pipeline_to_proto(
    pipeline: Pipeline, dataset_name: str
) -> ds_proto.Pipeline:
    serializer = Serializer()
    root, nodes = serializer.serialize(pipeline)
    signature = f"{dataset_name}.{root}"
    return ds_proto.Pipeline(
        id=pipeline.id,
        root=root,
        nodes=nodes,
        inputs=[i._name for i in pipeline.inputs],
        signature=signature,
        metadata=get_metadata_proto(pipeline.func),
        name=pipeline.name,
    )


def _on_demand_to_proto(od: Optional[OnDemand]) -> Optional[ds_proto.OnDemand]:
    if od is None:
        return None
    return ds_proto.OnDemand(
        function_source_code=inspect.getsource(cast(Callable, od.func)),
        function=od.pickled_func,
        expires_after=duration_to_micros(od.expires_after),
    )


# ------------------------------------------------------------------------------
# Featureset
# ------------------------------------------------------------------------------


def featureset_to_proto(fs: Featureset):
    _check_owner_exists(fs)
    return services_proto.CreateFeaturesetRequest(
        name=fs._name,
        features=[feature_to_proto(feature) for feature in fs._features],
        extractors=[
            _extractor_to_proto(extractor, fs._id_to_feature_fqn)
            for extractor in fs._extractors
        ],
        # Currently we don't support versioning of featuresets.
        # Kept for future use.
        version=0,
        metadata=get_metadata_proto(fs),
    )


# Feature
def feature_to_proto(f: Feature) -> fs_proto.Feature:
    return fs_proto.Feature(
        id=f.id,
        name=f.name,
        metadata=get_metadata_proto(f),
        dtype=get_datatype(f.dtype),
    )


# Feature as input
def feature_to_proto_as_input(f: Feature) -> fs_proto.Input.Feature:
    return fs_proto.Input.Feature(
        feature_set=fs_proto.Input.FeatureSet(
            name=f.featureset_name,
        ),
        name=f.name,
    )


# Featureset
def fs_to_proto(fs: Featureset):
    return fs_proto.Input.FeatureSet(
        name=fs._name,
    )


# Extractor
def _extractor_to_proto(
    extractor: Extractor, id_to_feature_name: Dict[int, str]
) -> fs_proto.Extractor:
    inputs = []
    for input in extractor.inputs:
        if isinstance(input, Feature):
            inputs.append(
                fs_proto.Input(feature=feature_to_proto_as_input(input))
            )
        elif isinstance(input, Featureset):
            inputs.append(fs_proto.Input(feature_set=fs_to_proto(input)))
        else:
            raise TypeError(
                f"Extractor input {input} is not a Feature or "
                f"Featureset but a {type(input)}"
            )
    depended_datasets = []
    if hasattr(extractor.func, DEPENDS_ON_DATASETS_ATTR):
        depended_datasets = getattr(extractor.func, DEPENDS_ON_DATASETS_ATTR)
    return fs_proto.Extractor(
        name=extractor.name,
        func=extractor.pickled_func,
        func_source_code=inspect.getsource(extractor.func),
        datasets=[dataset._name for dataset in depended_datasets],
        inputs=inputs,
        # Output features are stored as names and NOT FQN.
        features=[
            id_to_feature_name[id].split(".")[1]
            for id in extractor.output_feature_ids
        ],
        metadata=get_metadata_proto(extractor.func),
        version=extractor.version,
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
