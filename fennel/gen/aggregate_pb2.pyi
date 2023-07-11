"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.message
import schema_pb2
import sys
import window_pb2

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class Spec(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SUM_FIELD_NUMBER: builtins.int
    AVERAGE_FIELD_NUMBER: builtins.int
    COUNT_FIELD_NUMBER: builtins.int
    LAST_K_FIELD_NUMBER: builtins.int
    MIN_FIELD_NUMBER: builtins.int
    MAX_FIELD_NUMBER: builtins.int
    @property
    def sum(self) -> global___Sum: ...
    @property
    def average(self) -> global___Average: ...
    @property
    def count(self) -> global___Count: ...
    @property
    def last_k(self) -> global___LastK: ...
    @property
    def min(self) -> global___Min: ...
    @property
    def max(self) -> global___Max: ...
    def __init__(
        self,
        *,
        sum: global___Sum | None = ...,
        average: global___Average | None = ...,
        count: global___Count | None = ...,
        last_k: global___LastK | None = ...,
        min: global___Min | None = ...,
        max: global___Max | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["average", b"average", "count", b"count", "last_k", b"last_k", "max", b"max", "min", b"min", "sum", b"sum", "variant", b"variant"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["average", b"average", "count", b"count", "last_k", b"last_k", "max", b"max", "min", b"min", "sum", b"sum", "variant", b"variant"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["variant", b"variant"]) -> typing_extensions.Literal["sum", "average", "count", "last_k", "min", "max"] | None: ...

global___Spec = Spec

@typing_extensions.final
class Sum(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> schema_pb2.Field: ...
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    def __init__(
        self,
        *,
        of: schema_pb2.Field | None = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["of", b"of", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Sum = Sum

@typing_extensions.final
class Average(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> schema_pb2.Field: ...
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    default: builtins.float
    def __init__(
        self,
        *,
        of: schema_pb2.Field | None = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: builtins.float = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["of", b"of", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Average = Average

@typing_extensions.final
class Count(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    UNIQUE_FIELD_NUMBER: builtins.int
    APPROX_FIELD_NUMBER: builtins.int
    OF_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    unique: builtins.bool
    approx: builtins.bool
    @property
    def of(self) -> schema_pb2.Field: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        unique: builtins.bool = ...,
        approx: builtins.bool = ...,
        of: schema_pb2.Field | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["of", b"of", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["approx", b"approx", "name", b"name", "of", b"of", "unique", b"unique", "window", b"window"]) -> None: ...

global___Count = Count

@typing_extensions.final
class LastK(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    LIMIT_FIELD_NUMBER: builtins.int
    DEDUP_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> schema_pb2.Field: ...
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    limit: builtins.int
    dedup: builtins.bool
    def __init__(
        self,
        *,
        of: schema_pb2.Field | None = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        limit: builtins.int = ...,
        dedup: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["of", b"of", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dedup", b"dedup", "limit", b"limit", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___LastK = LastK

@typing_extensions.final
class Min(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> schema_pb2.Field: ...
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    @property
    def default(self) -> schema_pb2.Value: ...
    def __init__(
        self,
        *,
        of: schema_pb2.Field | None = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: schema_pb2.Value | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["default", b"default", "of", b"of", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Min = Min

@typing_extensions.final
class Max(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    @property
    def of(self) -> schema_pb2.Field: ...
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    @property
    def default(self) -> schema_pb2.Value: ...
    def __init__(
        self,
        *,
        of: schema_pb2.Field | None = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: schema_pb2.Value | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["default", b"default", "of", b"of", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Max = Max