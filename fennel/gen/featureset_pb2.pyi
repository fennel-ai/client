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
import pycode_pb2
import schema_pb2
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class CoreFeatureset(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        metadata: metadata_pb2.Metadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["metadata", b"metadata", "name", b"name"]) -> None: ...

global___CoreFeatureset = CoreFeatureset

@typing_extensions.final
class Feature(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    DTYPE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    FEATURE_SET_NAME_FIELD_NUMBER: builtins.int
    id: builtins.int
    name: builtins.str
    @property
    def dtype(self) -> schema_pb2.DataType: ...
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    feature_set_name: builtins.str
    def __init__(
        self,
        *,
        id: builtins.int = ...,
        name: builtins.str = ...,
        dtype: schema_pb2.DataType | None = ...,
        metadata: metadata_pb2.Metadata | None = ...,
        feature_set_name: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["dtype", b"dtype", "metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dtype", b"dtype", "feature_set_name", b"feature_set_name", "id", b"id", "metadata", b"metadata", "name", b"name"]) -> None: ...

global___Feature = Feature

@typing_extensions.final
class Extractor(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    DATASETS_FIELD_NUMBER: builtins.int
    INPUTS_FIELD_NUMBER: builtins.int
    FEATURES_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    VERSION_FIELD_NUMBER: builtins.int
    PYCODE_FIELD_NUMBER: builtins.int
    FEATURE_SET_NAME_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def datasets(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def inputs(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Input]: ...
    @property
    def features(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]:
        """The features that this extractor produces"""
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    version: builtins.int
    @property
    def pycode(self) -> pycode_pb2.ExtractorPyCode: ...
    feature_set_name: builtins.str
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        datasets: collections.abc.Iterable[builtins.str] | None = ...,
        inputs: collections.abc.Iterable[global___Input] | None = ...,
        features: collections.abc.Iterable[builtins.str] | None = ...,
        metadata: metadata_pb2.Metadata | None = ...,
        version: builtins.int = ...,
        pycode: pycode_pb2.ExtractorPyCode | None = ...,
        feature_set_name: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata", "pycode", b"pycode"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["datasets", b"datasets", "feature_set_name", b"feature_set_name", "features", b"features", "inputs", b"inputs", "metadata", b"metadata", "name", b"name", "pycode", b"pycode", "version", b"version"]) -> None: ...

global___Extractor = Extractor

@typing_extensions.final
class Input(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class Feature(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        FEATURE_SET_NAME_FIELD_NUMBER: builtins.int
        NAME_FIELD_NUMBER: builtins.int
        feature_set_name: builtins.str
        name: builtins.str
        def __init__(
            self,
            *,
            feature_set_name: builtins.str = ...,
            name: builtins.str = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["feature_set_name", b"feature_set_name", "name", b"name"]) -> None: ...

    FEATURE_FIELD_NUMBER: builtins.int
    @property
    def feature(self) -> global___Input.Feature: ...
    def __init__(
        self,
        *,
        feature: global___Input.Feature | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["feature", b"feature"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["feature", b"feature"]) -> None: ...

global___Input = Input

@typing_extensions.final
class Model(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    INPUTS_FIELD_NUMBER: builtins.int
    OUTPUTS_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def inputs(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Feature]: ...
    @property
    def outputs(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Feature]: ...
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        inputs: collections.abc.Iterable[global___Feature] | None = ...,
        outputs: collections.abc.Iterable[global___Feature] | None = ...,
        metadata: metadata_pb2.Metadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["inputs", b"inputs", "metadata", b"metadata", "name", b"name", "outputs", b"outputs"]) -> None: ...

global___Model = Model
