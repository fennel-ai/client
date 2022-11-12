"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import metadata_pb2
import status_pb2
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class Feature(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    DTYPE_FIELD_NUMBER: builtins.int
    OWNER_FIELD_NUMBER: builtins.int
    DESCRIPTION_FIELD_NUMBER: builtins.int
    WIP_FIELD_NUMBER: builtins.int
    DEPRECATED_FIELD_NUMBER: builtins.int
    TAGS_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    id: builtins.int
    name: builtins.str
    dtype: builtins.str
    """Arrow type"""
    owner: builtins.str
    description: builtins.str
    wip: builtins.bool
    deprecated: builtins.bool
    @property
    def tags(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    def __init__(
        self,
        *,
        id: builtins.int = ...,
        name: builtins.str = ...,
        dtype: builtins.str = ...,
        owner: builtins.str = ...,
        description: builtins.str = ...,
        wip: builtins.bool = ...,
        deprecated: builtins.bool = ...,
        tags: collections.abc.Iterable[builtins.str] | None = ...,
        metadata: metadata_pb2.Metadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["deprecated", b"deprecated", "description", b"description", "dtype", b"dtype", "id", b"id", "metadata", b"metadata", "name", b"name", "owner", b"owner", "tags", b"tags", "wip", b"wip"]) -> None: ...

global___Feature = Feature

@typing_extensions.final
class Extractor(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    FUNC_FIELD_NUMBER: builtins.int
    FUNC_SOURCE_CODE_FIELD_NUMBER: builtins.int
    DATASETS_FIELD_NUMBER: builtins.int
    INPUTS_FIELD_NUMBER: builtins.int
    FEATURES_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    name: builtins.str
    func: builtins.bytes
    func_source_code: builtins.str
    @property
    def datasets(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def inputs(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Input]: ...
    @property
    def features(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]:
        """The features that this extractor produces"""
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        func: builtins.bytes = ...,
        func_source_code: builtins.str = ...,
        datasets: collections.abc.Iterable[builtins.str] | None = ...,
        inputs: collections.abc.Iterable[global___Input] | None = ...,
        features: collections.abc.Iterable[builtins.str] | None = ...,
        metadata: metadata_pb2.Metadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["datasets", b"datasets", "features", b"features", "func", b"func", "func_source_code", b"func_source_code", "inputs", b"inputs", "metadata", b"metadata", "name", b"name"]) -> None: ...

global___Extractor = Extractor

@typing_extensions.final
class CreateFeaturesetRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    FEATURES_FIELD_NUMBER: builtins.int
    EXTRACTORS_FIELD_NUMBER: builtins.int
    VERSION_FIELD_NUMBER: builtins.int
    SIGNATURE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def features(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Feature]: ...
    @property
    def extractors(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Extractor]: ...
    version: builtins.int
    signature: builtins.str
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        features: collections.abc.Iterable[global___Feature] | None = ...,
        extractors: collections.abc.Iterable[global___Extractor] | None = ...,
        version: builtins.int = ...,
        signature: builtins.str = ...,
        metadata: metadata_pb2.Metadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["extractors", b"extractors", "features", b"features", "metadata", b"metadata", "name", b"name", "signature", b"signature", "version", b"version"]) -> None: ...

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
class Input(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class FeatureSet(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        NAME_FIELD_NUMBER: builtins.int
        name: builtins.str
        def __init__(
            self,
            *,
            name: builtins.str = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["name", b"name"]) -> None: ...

    @typing_extensions.final
    class Feature(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        FEATURE_SET_FIELD_NUMBER: builtins.int
        NAME_FIELD_NUMBER: builtins.int
        @property
        def feature_set(self) -> global___Input.FeatureSet: ...
        name: builtins.str
        def __init__(
            self,
            *,
            feature_set: global___Input.FeatureSet | None = ...,
            name: builtins.str = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["feature_set", b"feature_set"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["feature_set", b"feature_set", "name", b"name"]) -> None: ...

    FEATURE_SET_FIELD_NUMBER: builtins.int
    FEATURE_FIELD_NUMBER: builtins.int
    @property
    def feature_set(self) -> global___Input.FeatureSet: ...
    @property
    def feature(self) -> global___Input.Feature: ...
    def __init__(
        self,
        *,
        feature_set: global___Input.FeatureSet | None = ...,
        feature: global___Input.Feature | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["feature", b"feature", "feature_set", b"feature_set", "input", b"input"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["feature", b"feature", "feature_set", b"feature_set", "input", b"input"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["input", b"input"]) -> typing_extensions.Literal["feature_set", "feature"] | None: ...

global___Input = Input
