"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _ScalarType:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _ScalarTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_ScalarType.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    INT: _ScalarType.ValueType  # 0
    FLOAT: _ScalarType.ValueType  # 1
    STRING: _ScalarType.ValueType  # 2
    BOOLEAN: _ScalarType.ValueType  # 3
    TIMESTAMP: _ScalarType.ValueType  # 4

class ScalarType(_ScalarType, metaclass=_ScalarTypeEnumTypeWrapper): ...

INT: ScalarType.ValueType  # 0
FLOAT: ScalarType.ValueType  # 1
STRING: ScalarType.ValueType  # 2
BOOLEAN: ScalarType.ValueType  # 3
TIMESTAMP: ScalarType.ValueType  # 4
global___ScalarType = ScalarType

@typing_extensions.final
class DataType(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    IS_NULLABLE_FIELD_NUMBER: builtins.int
    SCALAR_TYPE_FIELD_NUMBER: builtins.int
    ARRAY_TYPE_FIELD_NUMBER: builtins.int
    MAP_TYPE_FIELD_NUMBER: builtins.int
    EMBEDDING_TYPE_FIELD_NUMBER: builtins.int
    is_nullable: builtins.bool
    scalar_type: global___ScalarType.ValueType
    @property
    def array_type(self) -> global___ArrayType: ...
    @property
    def map_type(self) -> global___MapType: ...
    @property
    def embedding_type(self) -> global___EmbeddingType: ...
    def __init__(
        self,
        *,
        is_nullable: builtins.bool = ...,
        scalar_type: global___ScalarType.ValueType = ...,
        array_type: global___ArrayType | None = ...,
        map_type: global___MapType | None = ...,
        embedding_type: global___EmbeddingType | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["array_type", b"array_type", "embedding_type", b"embedding_type", "map_type", b"map_type", "scalar_type", b"scalar_type", "type", b"type"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["array_type", b"array_type", "embedding_type", b"embedding_type", "is_nullable", b"is_nullable", "map_type", b"map_type", "scalar_type", b"scalar_type", "type", b"type"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["type", b"type"]) -> typing_extensions.Literal["scalar_type", "array_type", "map_type", "embedding_type"] | None: ...

global___DataType = DataType

@typing_extensions.final
class ArrayType(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> global___DataType: ...
    def __init__(
        self,
        *,
        of: global___DataType | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["of", b"of"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["of", b"of"]) -> None: ...

global___ArrayType = ArrayType

@typing_extensions.final
class EmbeddingType(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    EMBEDDING_SIZE_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> global___DataType: ...
    embedding_size: builtins.int
    def __init__(
        self,
        *,
        of: global___DataType | None = ...,
        embedding_size: builtins.int = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["of", b"of"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["embedding_size", b"embedding_size", "of", b"of"]) -> None: ...

global___EmbeddingType = EmbeddingType

@typing_extensions.final
class MapType(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    KEY_FIELD_NUMBER: builtins.int
    VALUE_FIELD_NUMBER: builtins.int
    @property
    def key(self) -> global___DataType: ...
    @property
    def value(self) -> global___DataType: ...
    def __init__(
        self,
        *,
        key: global___DataType | None = ...,
        value: global___DataType | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

global___MapType = MapType
