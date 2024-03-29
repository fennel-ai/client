"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class Metadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OWNER_FIELD_NUMBER: builtins.int
    DESCRIPTION_FIELD_NUMBER: builtins.int
    TAGS_FIELD_NUMBER: builtins.int
    DEPRECATED_FIELD_NUMBER: builtins.int
    DELETED_FIELD_NUMBER: builtins.int
    owner: builtins.str
    description: builtins.str
    @property
    def tags(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    deprecated: builtins.bool
    deleted: builtins.bool
    def __init__(
        self,
        *,
        owner: builtins.str = ...,
        description: builtins.str = ...,
        tags: collections.abc.Iterable[builtins.str] | None = ...,
        deprecated: builtins.bool = ...,
        deleted: builtins.bool = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["deleted", b"deleted", "deprecated", b"deprecated", "description", b"description", "owner", b"owner", "tags", b"tags"]) -> None: ...

global___Metadata = Metadata
