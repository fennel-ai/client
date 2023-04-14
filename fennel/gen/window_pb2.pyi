"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.duration_pb2
import google.protobuf.message
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class Window(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SLIDING_FIELD_NUMBER: builtins.int
    FOREVER_FIELD_NUMBER: builtins.int
    @property
    def sliding(self) -> global___Sliding: ...
    @property
    def forever(self) -> global___Forever: ...
    def __init__(
        self,
        *,
        sliding: global___Sliding | None = ...,
        forever: global___Forever | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["forever", b"forever", "sliding", b"sliding", "variant", b"variant"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["forever", b"forever", "sliding", b"sliding", "variant", b"variant"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["variant", b"variant"]) -> typing_extensions.Literal["sliding", "forever"] | None: ...

global___Window = Window

class Sliding(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DURATION_FIELD_NUMBER: builtins.int
    @property
    def duration(self) -> google.protobuf.duration_pb2.Duration: ...
    def __init__(
        self,
        *,
        duration: google.protobuf.duration_pb2.Duration | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["duration", b"duration"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["duration", b"duration"]) -> None: ...

global___Sliding = Sliding

class Forever(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___Forever = Forever
