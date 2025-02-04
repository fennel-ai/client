"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.message
import sys
import window_pb2

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class PreSpec(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SUM_FIELD_NUMBER: builtins.int
    AVERAGE_FIELD_NUMBER: builtins.int
    COUNT_FIELD_NUMBER: builtins.int
    LAST_K_FIELD_NUMBER: builtins.int
    MIN_FIELD_NUMBER: builtins.int
    MAX_FIELD_NUMBER: builtins.int
    STDDEV_FIELD_NUMBER: builtins.int
    DISTINCT_FIELD_NUMBER: builtins.int
    QUANTILE_FIELD_NUMBER: builtins.int
    EXP_DECAY_FIELD_NUMBER: builtins.int
    FIRST_K_FIELD_NUMBER: builtins.int
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
    @property
    def stddev(self) -> global___Stddev: ...
    @property
    def distinct(self) -> global___Distinct: ...
    @property
    def quantile(self) -> global___Quantile: ...
    @property
    def exp_decay(self) -> global___ExponentialDecayAggregate: ...
    @property
    def first_k(self) -> global___FirstK: ...
    def __init__(
        self,
        *,
        sum: global___Sum | None = ...,
        average: global___Average | None = ...,
        count: global___Count | None = ...,
        last_k: global___LastK | None = ...,
        min: global___Min | None = ...,
        max: global___Max | None = ...,
        stddev: global___Stddev | None = ...,
        distinct: global___Distinct | None = ...,
        quantile: global___Quantile | None = ...,
        exp_decay: global___ExponentialDecayAggregate | None = ...,
        first_k: global___FirstK | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["average", b"average", "count", b"count", "distinct", b"distinct", "exp_decay", b"exp_decay", "first_k", b"first_k", "last_k", b"last_k", "max", b"max", "min", b"min", "quantile", b"quantile", "stddev", b"stddev", "sum", b"sum", "variant", b"variant"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["average", b"average", "count", b"count", "distinct", b"distinct", "exp_decay", b"exp_decay", "first_k", b"first_k", "last_k", b"last_k", "max", b"max", "min", b"min", "quantile", b"quantile", "stddev", b"stddev", "sum", b"sum", "variant", b"variant"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["variant", b"variant"]) -> typing_extensions.Literal["sum", "average", "count", "last_k", "min", "max", "stddev", "distinct", "quantile", "exp_decay", "first_k"] | None: ...

global___PreSpec = PreSpec

@typing_extensions.final
class Sum(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Sum = Sum

@typing_extensions.final
class Average(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    DEFAULT_NULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    default: builtins.float
    default_null: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: builtins.float = ...,
        default_null: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "default_null", b"default_null", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Average = Average

@typing_extensions.final
class Count(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    UNIQUE_FIELD_NUMBER: builtins.int
    APPROX_FIELD_NUMBER: builtins.int
    OF_FIELD_NUMBER: builtins.int
    DROPNULL_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    unique: builtins.bool
    approx: builtins.bool
    of: builtins.str
    dropnull: builtins.bool
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        unique: builtins.bool = ...,
        approx: builtins.bool = ...,
        of: builtins.str = ...,
        dropnull: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["approx", b"approx", "dropnull", b"dropnull", "name", b"name", "of", b"of", "unique", b"unique", "window", b"window"]) -> None: ...

global___Count = Count

@typing_extensions.final
class LastK(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    LIMIT_FIELD_NUMBER: builtins.int
    DEDUP_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DROPNULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    limit: builtins.int
    dedup: builtins.bool
    @property
    def window(self) -> window_pb2.Window: ...
    dropnull: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        limit: builtins.int = ...,
        dedup: builtins.bool = ...,
        window: window_pb2.Window | None = ...,
        dropnull: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dedup", b"dedup", "dropnull", b"dropnull", "limit", b"limit", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___LastK = LastK

@typing_extensions.final
class FirstK(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    LIMIT_FIELD_NUMBER: builtins.int
    DEDUP_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DROPNULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    limit: builtins.int
    dedup: builtins.bool
    @property
    def window(self) -> window_pb2.Window: ...
    dropnull: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        limit: builtins.int = ...,
        dedup: builtins.bool = ...,
        window: window_pb2.Window | None = ...,
        dropnull: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dedup", b"dedup", "dropnull", b"dropnull", "limit", b"limit", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___FirstK = FirstK

@typing_extensions.final
class Min(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    DEFAULT_NULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    default: builtins.float
    default_null: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: builtins.float = ...,
        default_null: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "default_null", b"default_null", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Min = Min

@typing_extensions.final
class Max(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    DEFAULT_NULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    default: builtins.float
    default_null: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: builtins.float = ...,
        default_null: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "default_null", b"default_null", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Max = Max

@typing_extensions.final
class Stddev(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    DEFAULT_NULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    default: builtins.float
    default_null: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: builtins.float = ...,
        default_null: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["default", b"default", "default_null", b"default_null", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Stddev = Stddev

@typing_extensions.final
class Distinct(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DROPNULL_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    dropnull: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        dropnull: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dropnull", b"dropnull", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___Distinct = Distinct

@typing_extensions.final
class Quantile(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    DEFAULT_FIELD_NUMBER: builtins.int
    QUANTILE_FIELD_NUMBER: builtins.int
    APPROX_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    default: builtins.float
    quantile: builtins.float
    approx: builtins.bool
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        default: builtins.float | None = ...,
        quantile: builtins.float = ...,
        approx: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_default", b"_default", "default", b"default", "window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_default", b"_default", "approx", b"approx", "default", b"default", "name", b"name", "of", b"of", "quantile", b"quantile", "window", b"window"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_default", b"_default"]) -> typing_extensions.Literal["default"] | None: ...

global___Quantile = Quantile

@typing_extensions.final
class ExponentialDecayAggregate(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    WINDOW_FIELD_NUMBER: builtins.int
    HALF_LIFE_SECONDS_FIELD_NUMBER: builtins.int
    of: builtins.str
    name: builtins.str
    @property
    def window(self) -> window_pb2.Window: ...
    half_life_seconds: builtins.int
    def __init__(
        self,
        *,
        of: builtins.str = ...,
        name: builtins.str = ...,
        window: window_pb2.Window | None = ...,
        half_life_seconds: builtins.int = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["window", b"window"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["half_life_seconds", b"half_life_seconds", "name", b"name", "of", b"of", "window", b"window"]) -> None: ...

global___ExponentialDecayAggregate = ExponentialDecayAggregate
