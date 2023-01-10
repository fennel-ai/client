from __future__ import annotations

import inspect
from typing import (
    cast,
    Callable,
    Optional,
)

import fennel.gen.dataset_pb2 as proto
from fennel.datasets import Dataset, Pipeline, Field, OnDemand
from fennel.lib.duration.duration import (
    timedelta_to_micros,
    duration_to_micros,
)
from fennel.lib.metadata import (
    get_metadata_proto,
)
from fennel.lib.schema import get_datatype
from fennel.lib.to_proto import Serializer
from fennel.sources import SOURCE_FIELD, SINK_FIELD


def dataset_to_proto(ds: Dataset) -> proto.CreateDatasetRequest:
    ds._check_owner_exists()
    sources = []
    if hasattr(ds, SOURCE_FIELD):
        sources = getattr(ds, SOURCE_FIELD)
    sinks = []
    if hasattr(ds, SINK_FIELD):
        sinks = getattr(ds, SINK_FIELD)

    return proto.CreateDatasetRequest(
        name=ds.__name__,
        retention=timedelta_to_micros(ds._retention),
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
def _field_to_proto(f: Field) -> proto.Field:
    # Type is passed as part of the dataset schema
    if f.key + f.timestamp > 1:
        raise ValueError("Field cannot be both a key and a timestamp")

    if f.key:
        ftype = proto.FieldType.Key
    elif f.timestamp:
        ftype = proto.FieldType.Timestamp
    else:
        ftype = proto.FieldType.Val

    return proto.Field(
        name=f.name,
        ftype=ftype,
        dtype=get_datatype(f.dtype),
        metadata=get_metadata_proto(f),
    )


# Pipeline
def _pipeline_to_proto(self: Pipeline, dataset_name: str) -> proto.Pipeline:
    serializer = Serializer()
    root, nodes = serializer.serialize(self)
    signature = f"{dataset_name}.{root}"
    return proto.Pipeline(
        root=root,
        nodes=nodes,
        inputs=[i._name for i in self.inputs],
        signature=signature,
        metadata=get_metadata_proto(self.func),
        name=self.name,
    )


def _on_demand_to_proto(od: Optional[OnDemand]) -> Optional[proto.OnDemand]:
    if od is None:
        return None
    return proto.OnDemand(
        function_source_code=inspect.getsource(cast(Callable, od.func)),
        function=od.pickled_func,
        expires_after=duration_to_micros(od.expires_after),
    )
