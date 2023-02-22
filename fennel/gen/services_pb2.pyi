"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import dataset_pb2
import expectations_pb2
import featureset_pb2
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import metadata_pb2
import source_pb2
import status_pb2
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class CreateDatasetRequest(google.protobuf.message.Message):
    """
    The generated code needs to be hand modified to add the following
    import stream_pb2 as stream__pb2 to import fennel.gen.stream_pb2 as stream__pb2
    More info: https://github.com/protocolbuffers/protobuf/issues/1491 &
    https://github.com/protocolbuffers/protobuf/issues/881

    Use the following command from client ( root ) directory to generated the
    required files -
    https://gist.github.com/aditya-nambiar/87c8b55fe509c1c1cd06d212b8a8ded1
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    PIPELINES_FIELD_NUMBER: builtins.int
    INPUT_CONNECTORS_FIELD_NUMBER: builtins.int
    OUTPUT_CONNECTORS_FIELD_NUMBER: builtins.int
    SIGNATURE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    MODE_FIELD_NUMBER: builtins.int
    VERSION_FIELD_NUMBER: builtins.int
    HISTORY_FIELD_NUMBER: builtins.int
    MAX_STALENESS_FIELD_NUMBER: builtins.int
    ON_DEMAND_FIELD_NUMBER: builtins.int
    EXPECTATIONS_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def fields(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[dataset_pb2.Field]: ...
    @property
    def pipelines(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[dataset_pb2.Pipeline]: ...
    @property
    def input_connectors(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[source_pb2.DataConnector]: ...
    @property
    def output_connectors(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[source_pb2.DataConnector]: ...
    signature: builtins.str
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    mode: builtins.str
    """Default mode is pandas."""
    version: builtins.int
    history: builtins.int
    max_staleness: builtins.int
    @property
    def on_demand(self) -> dataset_pb2.OnDemand: ...
    @property
    def expectations(self) -> expectations_pb2.Expectations: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        fields: collections.abc.Iterable[dataset_pb2.Field] | None = ...,
        pipelines: collections.abc.Iterable[dataset_pb2.Pipeline] | None = ...,
        input_connectors: collections.abc.Iterable[source_pb2.DataConnector] | None = ...,
        output_connectors: collections.abc.Iterable[source_pb2.DataConnector] | None = ...,
        signature: builtins.str = ...,
        metadata: metadata_pb2.Metadata | None = ...,
        mode: builtins.str = ...,
        version: builtins.int = ...,
        history: builtins.int = ...,
        max_staleness: builtins.int = ...,
        on_demand: dataset_pb2.OnDemand | None = ...,
        expectations: expectations_pb2.Expectations | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["expectations", b"expectations", "metadata", b"metadata", "on_demand", b"on_demand"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["expectations", b"expectations", "fields", b"fields", "history", b"history", "input_connectors", b"input_connectors", "max_staleness", b"max_staleness", "metadata", b"metadata", "mode", b"mode", "name", b"name", "on_demand", b"on_demand", "output_connectors", b"output_connectors", "pipelines", b"pipelines", "signature", b"signature", "version", b"version"]) -> None: ...

global___CreateDatasetRequest = CreateDatasetRequest

@typing_extensions.final
class CreateDatasetResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    STATUS_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def status(self) -> status_pb2.Status: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        status: status_pb2.Status | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["status", b"status"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["name", b"name", "status", b"status"]) -> None: ...

global___CreateDatasetResponse = CreateDatasetResponse

@typing_extensions.final
class SyncRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATASET_REQUESTS_FIELD_NUMBER: builtins.int
    FEATURESET_REQUESTS_FIELD_NUMBER: builtins.int
    MODELS_FIELD_NUMBER: builtins.int
    @property
    def dataset_requests(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___CreateDatasetRequest]: ...
    @property
    def featureset_requests(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___CreateFeaturesetRequest]: ...
    @property
    def models(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[featureset_pb2.Model]: ...
    def __init__(
        self,
        *,
        dataset_requests: collections.abc.Iterable[global___CreateDatasetRequest] | None = ...,
        featureset_requests: collections.abc.Iterable[global___CreateFeaturesetRequest] | None = ...,
        models: collections.abc.Iterable[featureset_pb2.Model] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["dataset_requests", b"dataset_requests", "featureset_requests", b"featureset_requests", "models", b"models"]) -> None: ...

global___SyncRequest = SyncRequest

@typing_extensions.final
class CreateFeaturesetRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    FEATURES_FIELD_NUMBER: builtins.int
    EXTRACTORS_FIELD_NUMBER: builtins.int
    VERSION_FIELD_NUMBER: builtins.int
    SIGNATURE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    SCHEMA_FIELD_NUMBER: builtins.int
    EXPECTATIONS_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def features(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[featureset_pb2.Feature]: ...
    @property
    def extractors(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[featureset_pb2.Extractor]: ...
    version: builtins.int
    signature: builtins.str
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    schema: builtins.bytes
    """Serialized arrow schema."""
    @property
    def expectations(self) -> expectations_pb2.Expectations: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        features: collections.abc.Iterable[featureset_pb2.Feature] | None = ...,
        extractors: collections.abc.Iterable[featureset_pb2.Extractor] | None = ...,
        version: builtins.int = ...,
        signature: builtins.str = ...,
        metadata: metadata_pb2.Metadata | None = ...,
        schema: builtins.bytes = ...,
        expectations: expectations_pb2.Expectations | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["expectations", b"expectations", "metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["expectations", b"expectations", "extractors", b"extractors", "features", b"features", "metadata", b"metadata", "name", b"name", "schema", b"schema", "signature", b"signature", "version", b"version"]) -> None: ...

global___CreateFeaturesetRequest = CreateFeaturesetRequest

@typing_extensions.final
class CreateFeatureSetResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    STATUS_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def status(self) -> status_pb2.Status: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        status: status_pb2.Status | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["status", b"status"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["name", b"name", "status", b"status"]) -> None: ...

global___CreateFeatureSetResponse = CreateFeatureSetResponse

@typing_extensions.final
class SyncResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATASET_RESPONSES_FIELD_NUMBER: builtins.int
    FEATURE_SET_RESPONSES_FIELD_NUMBER: builtins.int
    @property
    def dataset_responses(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___CreateDatasetResponse]: ...
    @property
    def feature_set_responses(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___CreateFeatureSetResponse]: ...
    def __init__(
        self,
        *,
        dataset_responses: collections.abc.Iterable[global___CreateDatasetResponse] | None = ...,
        feature_set_responses: collections.abc.Iterable[global___CreateFeatureSetResponse] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["dataset_responses", b"dataset_responses", "feature_set_responses", b"feature_set_responses"]) -> None: ...

global___SyncResponse = SyncResponse
