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
class PyCode(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SOURCE_CODE_FIELD_NUMBER: builtins.int
    PICKLED_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    SOURCE_CODE_WO_COMMENTS_FIELD_NUMBER: builtins.int
    INCLUDES_FIELD_NUMBER: builtins.int
    source_code: builtins.str
    pickled: builtins.bytes
    name: builtins.str
    source_code_wo_comments: builtins.str
    @property
    def includes(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___PyCode]: ...
    def __init__(
        self,
        *,
        source_code: builtins.str = ...,
        pickled: builtins.bytes = ...,
        name: builtins.str = ...,
        source_code_wo_comments: builtins.str = ...,
        includes: collections.abc.Iterable[global___PyCode] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["includes", b"includes", "name", b"name", "pickled", b"pickled", "source_code", b"source_code", "source_code_wo_comments", b"source_code_wo_comments"]) -> None: ...

global___PyCode = PyCode
