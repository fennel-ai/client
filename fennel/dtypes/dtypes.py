import dataclasses
from functools import partial
import inspect
import sys
from dataclasses import dataclass
from datetime import datetime
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Union,
    List,
    get_args,
    ForwardRef,
    Any,
)

import google.protobuf.duration_pb2 as duration_proto  # type: ignore
import pandas as pd

import fennel.gen.schema_pb2 as schema_proto
import fennel.gen.window_pb2 as window_proto
from fennel.internal_lib import (
    FENNEL_STRUCT,
    FENNEL_STRUCT_DEPENDENCIES_SRC_CODE,
    FENNEL_STRUCT_SRC_CODE,
    META_FIELD,
)
from fennel.internal_lib.duration import duration_to_timedelta
from fennel.internal_lib.utils.utils import (
    get_origin,
    is_user_defined_class,
    as_json,
    dtype_to_string,
)


def _contains_user_defined_class(annotation) -> bool:
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        return all(_contains_user_defined_class(arg) for arg in args)
    else:
        if is_user_defined_class(annotation):
            return hasattr(annotation, FENNEL_STRUCT)
        return True


def _contains_forward_ref(annotation) -> bool:
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        return any(_contains_forward_ref(arg) for arg in args)
    else:
        return isinstance(annotation, ForwardRef)


def get_fennel_struct(annotation) -> Any:
    origin = get_origin(annotation)
    if origin is not None:
        args = get_args(annotation)
        ret = None
        for arg in args:
            tmp_ret = get_fennel_struct(arg)
            if tmp_ret is not None:
                if ret is not None:
                    return TypeError(
                        f"Multiple fennel structs found `{ret.__name__},"
                        f" {tmp_ret.__name__}`"
                    )
                ret = tmp_ret
        return ret

    else:
        if hasattr(annotation, FENNEL_STRUCT):
            return annotation
        return None


def make_struct_expr(cls, **kwargs):
    from fennel.expr.expr import Expr, make_expr, make_struct

    fields = {}
    for name, value in kwargs.items():
        fields[name] = make_expr(value)

    return make_struct(fields, cls)


def struct(cls):
    for name, member in inspect.getmembers(cls):
        if inspect.isfunction(member) and name in cls.__dict__:
            raise TypeError(
                f"Struct `{cls.__name__}` contains method `{member.__name__}`, "
                f"which is not allowed."
            )
        elif name in cls.__dict__ and name in cls.__annotations__:
            raise ValueError(
                f"Struct `{cls.__name__}` contains attribute `{name}` with a default value, "
                f"`{cls.__dict__[name]}` which is not allowed."
            )
    if hasattr(cls, META_FIELD):
        raise ValueError(
            f"Struct `{cls.__name__}` contains decorator @meta which is not "
            f"allowed."
        )
    for name, annotation in cls.__annotations__.items():
        if not _contains_user_defined_class(annotation):
            raise TypeError(
                f"Struct `{cls.__name__}` contains attribute `{name}` of a "
                f"non-struct type, which is not allowed."
            )
        elif _contains_forward_ref(annotation):
            # Dont allow forward references
            raise TypeError(
                f"Struct `{cls.__name__}` contains forward reference `{name}` "
                f"which is not allowed."
            )
    dependency_code = ""
    for name, annotation in cls.__annotations__.items():
        fstruct = get_fennel_struct(annotation)
        if fstruct is not None:
            if hasattr(fstruct, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE):
                dependency_code += "\n\n" + getattr(
                    fstruct, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE
                )
            if not hasattr(fstruct, FENNEL_STRUCT_SRC_CODE):
                raise TypeError(
                    f"Struct `{cls.__name__}` contains attribute `{name}` of a "
                    f"non-struct type, which is not allowed."
                )
            dependency_code += "\n\n" + getattr(fstruct, FENNEL_STRUCT_SRC_CODE)

    setattr(cls, FENNEL_STRUCT, True)
    try:
        src_code = inspect.getsource(cls)
        if sys.version_info < (3, 9):
            src_code = f"@struct\n{dedent(src_code)}"
        setattr(cls, FENNEL_STRUCT_SRC_CODE, src_code)
    except Exception:
        # In exec mode ( such as extractor code generation ) there is no file
        # to get the source from, so we let it pass.
        setattr(cls, FENNEL_STRUCT_SRC_CODE, "")
    setattr(cls, FENNEL_STRUCT_DEPENDENCIES_SRC_CODE, dependency_code)
    cls.as_json = as_json
    cls.expr = partial(make_struct_expr, cls)
    return dataclasses.dataclass(cls)


# ---------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class _Embedding:
    dim: int


if TYPE_CHECKING:
    # Some type that can take an integer and keep mypy happy :)
    Embedding = pd.Series
else:

    class Embedding:
        def __init__(self, dim: int):
            raise TypeError(
                "Embedding is a type only and is meant to be used as "
                "a type hint, for example: Embedding[32]"
            )

        def __class_getitem__(cls, dimensions: int):
            return _Embedding(dimensions)


# ---------------------------------------------------------------------
# Decimal
# ---------------------------------------------------------------------


@dataclass
class _Decimal:
    scale: int


if TYPE_CHECKING:
    # Some type that can take an integer and keep mypy happy :)
    Decimal = pd.Series
else:

    class Decimal:
        def __init__(self, scale: int):
            raise TypeError(
                "Decimal is a type only and is meant to be used as "
                "a type hint, for example: Decimal[2]"
            )

        def __class_getitem__(cls, scale: int):
            if scale == 0:
                raise TypeError(
                    "Scale defined in the decimal type cannot be 0. If you want to use decimal with zero scale then use int type instead."
                )
            if scale > 28:
                raise TypeError(
                    "Scale defined in the decimal type cannot be greater than 28."
                )
            return _Decimal(scale)


@dataclass
class between:
    dtype: type
    min: Union[int, float]
    max: Union[int, float]
    strict_min: bool = False
    strict_max: bool = False

    def to_proto(self):
        if self.dtype is not int and self.dtype is not float:
            raise TypeError("'between' type only accepts int or float types")
        if self.dtype is int:
            dtype = schema_proto.DataType(int_type=schema_proto.IntType())
            if type(self.min) is float:
                raise TypeError(
                    "Dtype of between is int and min param is " "float"
                )
            if type(self.max) is float:
                raise TypeError(
                    "Dtype of between is int and max param is " "float"
                )

            min = schema_proto.Value(int=int(self.min))
            max = schema_proto.Value(int=int(self.max))
        else:
            dtype = schema_proto.DataType(double_type=schema_proto.DoubleType())
            min = schema_proto.Value(float=float(self.min))
            max = schema_proto.Value(float=float(self.max))

        return schema_proto.DataType(
            between_type=schema_proto.Between(
                dtype=dtype,
                min=min,
                max=max,
                strict_min=self.strict_min,
                strict_max=self.strict_max,
            )
        )


@dataclass
class oneof:
    dtype: type
    options: List[Union[str, int]]

    def to_proto(self):
        if self.dtype is not int and self.dtype is not str:
            raise TypeError("'oneof' type only accepts int or str types")
        for x in self.options:
            if type(x) is not self.dtype:
                raise TypeError(
                    "'oneof' options should match the type of "
                    f"dtype, found '{dtype_to_string(type(x))}' "
                    f"expected '{dtype_to_string(self.dtype)}'."
                )

        if self.dtype is int:
            dtype = schema_proto.DataType(int_type=schema_proto.IntType())
            options = []
            for option in self.options:
                if type(option) is not int:
                    raise TypeError(
                        "Dtype of oneof is int and option is " "not int"
                    )
                options.append(schema_proto.Value(int=option))
            return schema_proto.DataType(
                one_of_type=schema_proto.OneOf(
                    of=dtype,
                    options=options,
                )
            )

        if self.dtype is str:
            dtype = schema_proto.DataType(string_type=schema_proto.StringType())
            options = []
            for option in self.options:
                if type(option) is not str:
                    raise TypeError(
                        "Dtype of oneof is str and option is " "not str"
                    )
                options.append(schema_proto.Value(string=option))
            return schema_proto.DataType(
                one_of_type=schema_proto.OneOf(
                    of=dtype,
                    options=options,
                )
            )

        raise ValueError(f"Invalid dtype {self.dtype} for oneof")


@dataclass
class regex:
    regex: str

    def to_proto(self):
        if type(self.regex) is not str:
            raise TypeError("'regex' type only accepts str types")
        return schema_proto.DataType(
            regex_type=schema_proto.RegexType(pattern=self.regex)
        )


@struct
class Window:
    """
    Represents a time window that encapsulates events in [begin, end).

    Attributes:
        begin (datetime): The beginning of the time window.
        end (datetime): The end of the time window.
    """

    begin: datetime
    end: datetime


@dataclass
class Continuous:
    duration: str

    def __post_init__(self):
        """
        Run validation on duration.
        """
        if self.duration == "forever":
            pass
        else:
            duration_to_timedelta(self.duration)

    def to_proto(self) -> window_proto.Window:
        if self.duration == "forever":
            return window_proto.Window(forever=window_proto.Forever())

        duration = duration_proto.Duration()
        duration.FromTimedelta(duration_to_timedelta(self.duration))
        return window_proto.Window(
            sliding=window_proto.Sliding(duration=duration)
        )

    def signature(self) -> str:
        return f"sliding-{self.duration}"


@dataclass
class Tumbling:
    duration: str
    lookback: str = "0s"

    def __post_init__(self):
        """
        Run validation on duration.
        """
        if self.duration == "forever":
            raise ValueError(
                "'forever' is not a valid duration value for Tumbling window."
            )
        if self.lookback:
            try:
                duration_to_timedelta(self.lookback)
            except Exception as e:
                raise ValueError(f"Failed when parsing lookback : {e}.")
        try:
            duration_to_timedelta(self.duration)
        except Exception as e:
            raise ValueError(f"Failed when parsing duration : {e}.")

    def to_proto(self) -> window_proto.Window:
        duration = duration_proto.Duration()
        duration.FromTimedelta(duration_to_timedelta(self.duration))
        lookback = duration_proto.Duration()
        lookback.FromTimedelta(duration_to_timedelta(self.lookback))
        return window_proto.Window(
            tumbling=window_proto.Tumbling(duration=duration, lookback=lookback)
        )

    def signature(self) -> str:
        return f"tumbling-{self.duration}"

    def duration_total_seconds(self) -> int:
        return int(duration_to_timedelta(self.duration).total_seconds())

    def stride_total_seconds(self) -> int:
        return int(duration_to_timedelta(self.duration).total_seconds())

    def lookback_total_seconds(self) -> int:
        if self.lookback is None:
            return 0
        return int(duration_to_timedelta(self.lookback).total_seconds())


@dataclass
class Hopping:
    duration: str
    stride: str
    lookback: str = "0s"

    def __post_init__(self):
        """
        Run validation on duration and stride.
        """
        try:
            stride_timedelta = duration_to_timedelta(self.stride)
        except Exception as e:
            raise ValueError(f"Failed when parsing stride : {e}.")
        if self.lookback:
            try:
                duration_to_timedelta(self.lookback)
            except Exception as e:
                raise ValueError(f"Failed when parsing lookback : {e}.")
        if self.duration != "forever":
            try:
                duration_timedelta = duration_to_timedelta(self.duration)
            except Exception as e:
                raise ValueError(f"Failed when parsing duration : {e}.")
            if stride_timedelta > duration_timedelta:
                raise ValueError(
                    "stride parameters is larger than duration parameters which is not supported in 'Hopping'"
                )

    def to_proto(self) -> window_proto.Window:
        stride = duration_proto.Duration()
        stride.FromTimedelta(duration_to_timedelta(self.stride))
        lookback = duration_proto.Duration()
        lookback.FromTimedelta(duration_to_timedelta(self.lookback))
        if self.duration != "forever":
            duration = duration_proto.Duration()
            duration.FromTimedelta(duration_to_timedelta(self.duration))
            return window_proto.Window(
                hopping=window_proto.Hopping(
                    duration=duration, stride=stride, lookback=lookback
                )
            )
        else:
            return window_proto.Window(
                forever_hopping=window_proto.ForeverHopping(
                    stride=stride, lookback=lookback
                )
            )

    def signature(self) -> str:
        return f"hopping-{self.duration}-{self.stride}"

    def duration_total_seconds(self) -> int:
        if self.duration == "forever":
            raise ValueError(
                "For forever hopping window 'duration_total_seconds' method is not supported."
            )
        return int(duration_to_timedelta(self.duration).total_seconds())

    def stride_total_seconds(self) -> int:
        return int(duration_to_timedelta(self.stride).total_seconds())

    def lookback_total_seconds(self) -> int:
        if self.lookback is None:
            return 0
        return int(duration_to_timedelta(self.lookback).total_seconds())


@dataclass
class Session:
    gap: str

    def __post_init__(self):
        """
        Run validation on gap.
        """
        try:
            duration_to_timedelta(self.gap)
        except Exception as e:
            raise ValueError(f"Failed when parsing gap : {e}.")

    def to_proto(self) -> window_proto.Window:
        gap = duration_proto.Duration()
        gap.FromTimedelta(duration_to_timedelta(self.gap))
        return window_proto.Window(session=window_proto.Session(gap=gap))

    def signature(self) -> str:
        return f"session-{self.gap}"

    def gap_total_seconds(self) -> int:
        return int(duration_to_timedelta(self.gap).total_seconds())
