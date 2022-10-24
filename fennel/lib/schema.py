import inspect
from typing import Any, List

import numpy as np
import pandas as pd

import fennel.gen.schema_pb2 as proto
from fennel.lib.expectations import Expectation

Now = -1


class Type(object):
    def __str__(self):
        raise NotImplementedError()

    def _type(self):
        return self.__class__.__name__

    def type_check(self, other: "Type") -> bool:
        raise NotImplementedError()

    def validate(self, value: Any) -> List[Exception]:
        return []


class FInt(Type):
    def __str__(self):
        return "Int"

    def to_proto(self) -> proto.DataType:
        return proto.DataType(scalar_type=proto.ScalarType.INT)

    def type_check(self, other: "Type") -> bool:
        return other == np.int64

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if not isinstance(value, int):
            exceptions.append(TypeError(f"Expected int, got {type(value)}"))
        return exceptions


Int = FInt()


class FBool(Type):
    def __str__(self):
        return "Bool"

    def to_proto(self) -> proto.DataType:
        return proto.DataType(scalar_type=proto.ScalarType.BOOL)

    def type_check(self, other: "Type") -> bool:
        return other == np.bool_

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if not isinstance(value, bool):
            exceptions.append(TypeError(f"Expected bool, got {type(value)}"))
        return exceptions


Bool = FBool()


class FDouble(Type):
    def __str__(self):
        return "Double"

    def to_proto(self) -> proto.DataType:
        return proto.DataType(scalar_type=proto.ScalarType.DOUBLE)

    def type_check(self, other: "Type") -> bool:
        return other == np.float64

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if not isinstance(value, float):
            exceptions.append(TypeError(f"Expected float, got {type(value)}"))
        return exceptions


Double = FDouble()


class FString(Type):
    def __str__(self):
        return "String"

    def to_proto(self) -> proto.DataType:
        return proto.DataType(scalar_type=proto.ScalarType.STRING)

    def type_check(self, other: "Type") -> bool:
        return other == np.dtype("O")

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if not isinstance(value, str):
            exceptions.append(TypeError(f"Expected str, got {type(value)}"))
        return exceptions


String = FString()


class FTimestamp(Type):
    def __str__(self):
        return "Timestamp"

    def to_proto(self) -> proto.DataType:
        return proto.DataType(scalar_type=proto.ScalarType.TIMESTAMP)

    def type_check(self, other: "Type") -> bool:
        return (
            other == pd.Timestamp
            or other == np.datetime64
            or other == np.dtype("datetime64[ns]")
        )

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if isinstance(value, pd.Timestamp):
            return exceptions
        if value != Now:
            exceptions.append(
                TypeError(
                    f"Expected pd.Timestamp, got value {value}, type :"
                    f" {type(value)}"
                )
            )
        return exceptions

    @staticmethod
    def get_timestamp_proto(value: pd.Timestamp) -> proto.Timestamp:
        return proto.Timestamp(
            seconds=int(value.timestamp()),
            nanos=int(value.nanosecond % 1 * 1e9),
        )


Timestamp = FTimestamp()


class Map(Type):
    def __init__(self, key: Type = None, val: Type = None):
        self.key = key
        self.value = val

    @property
    def key(self):
        return self._key

    @key.setter
    def key(self, key: Type):
        if not isinstance(key, Type) or not issubclass(type(key), Type):
            raise TypeError(f"invalid key type in map: {key}")
        self._key = key

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value: Type):
        if not isinstance(value, Type) or not issubclass(type(value), Type):
            raise TypeError(f"invalid value type in map: {value}")

        self._value = value

    def __str__(self):
        return f"Map({self.key}: {self.value})"

    def to_proto(self) -> proto.DataType:
        dt = proto.DataType()
        dt.map_type.SetInParent()
        dt.map_type.key.CopyFrom(self.key.to_proto())
        dt.map_type.value.CopyFrom(self.value.to_proto())
        return dt

    def type_check(self, other: "Type") -> bool:
        return other == np.dtype("O")

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if not isinstance(value, dict):
            exceptions.append(TypeError(f"Expected dict, got {type(value)}"))
            return exceptions

        for k, v in value.items():
            exceptions.extend(self.key.validate(k))
            exceptions.extend(self.value.validate(v))
        return exceptions


class Array(Type):
    def __init__(self, of: Type = None):
        self.of = of

    @property
    def of(self):
        return self._of

    @of.setter
    def of(self, of: Type = None):
        if of is None:
            raise TypeError("invalid array type: None")
        if not isinstance(of, Type) or not issubclass(type(of), Type):
            raise TypeError(f"invalid array type: {of}, expected a Type Object")
        self._of = of

    def __str__(self):
        return "Array(" + str(self.of) + ")"

    def to_proto(self) -> proto.DataType:
        dt = proto.DataType()
        dt.array_type.SetInParent()
        dt.array_type.of.CopyFrom(self.of.to_proto())
        return dt

    def type_check(self, other: "Type") -> bool:
        return other == np.dtype("O")

    def validate(self, value: Any) -> List[Exception]:
        exceptions = []
        if not isinstance(value, list):
            exceptions.append(TypeError(f"Expected list, got {type(value)}"))
            return exceptions

        for v in value:
            exceptions.extend(self.of.validate(v))
        return exceptions


class Field:
    def __init__(
        self,
        name: str,
        dtype: Type,
        default: Any,
        nullable: bool = False,
        expectations: List[Expectation] = None,
    ):
        self.name = name
        self.dtype = dtype
        self.nullable = nullable
        self.default = default
        self.expectations = expectations

    def to_value_proto(self, dtype: Type, value: Any) -> proto.Value:
        if type(dtype) == FInt:
            if not isinstance(value, int):
                raise TypeError(
                    f"Expected default value for field {self.name} to be int, "
                    f"got {value}"
                )
            return proto.Value(int_value=value)
        elif type(dtype) == FDouble:
            if not isinstance(value, float):
                raise TypeError(
                    f"Expected default value for field {self.name} to be "
                    f"float, got {value}"
                )
            return proto.Value(double_value=value)
        elif type(dtype) == FString:
            if not isinstance(value, str):
                raise TypeError(
                    f"Expected default value for field {self.name} to be str, "
                    f"got {value}"
                )
            return proto.Value(string_value=value)
        elif type(dtype) == FBool:
            if not isinstance(value, bool):
                raise TypeError(
                    f"Expected default value for field {self.name} to be "
                    f"bool, got {value}"
                )
            return proto.Value(bool_value=value)
        elif type(dtype) == FTimestamp:
            if value != Now and not isinstance(value, pd.Timestamp):
                raise TypeError(
                    f"Expected default value for field {self.name} to be "
                    f"pd.Timestamp, got {value}"
                )
            elif value == Now:
                return proto.Value(timestamp_value=proto.Timestamp(now=True))
            return proto.Value(
                timestamp_value=FTimestamp.get_timestamp_proto(value)
            )
        elif type(dtype) == Array:
            if not isinstance(value, list):
                raise TypeError(
                    f"Expected default value for field {self.name} to be "
                    f"list, got {value}"
                )
            arr = proto.Array()
            for v in value:
                arr.elements.append(self.to_value_proto(dtype.of, v))
            return proto.Value(array_value=arr)
        elif type(dtype) == Map:
            if not isinstance(value, dict):
                raise TypeError(
                    f"Expected default value for field {self.name} to be "
                    f"dict, got {value}"
                )
            m = proto.Map()
            for k, v in value.items():
                m.keys.append(self.to_value_proto(dtype.key, k))
                m.values.append(self.to_value_proto(dtype.value, v))
            return proto.Value(map_value=m)
        elif inspect.isclass(dtype):
            raise TypeError(
                f"Type for {self.name} should be a Fennel Type object such as "
                f"Int() and not a class such as Int/int"
            )
        else:
            raise TypeError(f"Unknown type {type(dtype)}")

    def validate_default_value(self, dtype: Type, value: Any) -> List:
        errors = []
        if type(dtype) == FInt:
            if not isinstance(value, int):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be int, "
                        f"got {value}"
                    )
                )
        elif type(dtype) == FDouble:
            if not isinstance(value, float):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be "
                        f"float, got {value}"
                    )
                )
        elif type(dtype) == FString:
            if not isinstance(value, str):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be str, "
                        f"got {value}"
                    )
                )
        elif type(dtype) == FBool:
            if not isinstance(value, bool):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be "
                        f"bool, got {value}"
                    )
                )
        elif type(dtype) == FTimestamp:
            if value != Now and not isinstance(value, pd.Timestamp):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be "
                        f"pandas timestamp, got {value}"
                    )
                )
        elif type(dtype) == Array:
            if not isinstance(value, list):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be "
                        f"list, got {value}"
                    )
                )
            else:
                for v in value:
                    errors.extend(self.validate_default_value(dtype.of, v))
        elif type(dtype) == Map:
            if not isinstance(value, dict):
                errors.append(
                    TypeError(
                        f"Expected default value for field {self.name} to be "
                        f"dict, got {value}"
                    )
                )
            else:
                for k, v in value.items():
                    errors.extend(self.validate_default_value(dtype.key, k))
                    errors.extend(self.validate_default_value(dtype.value, v))
        elif inspect.isclass(dtype):
            errors.append(
                TypeError(
                    f"Type for {self.name} should be a Fennel Type object "
                    f"such as Int() and not a class such as Int/int"
                )
            )
        else:
            errors.append(TypeError(f"Unknown type {type(dtype)}"))
        return errors

    @property
    def dtype(self):
        return self._dtype

    @dtype.setter
    def dtype(self, dtype: Type):
        self._dtype = dtype

    @property
    def default(self):
        return self._default

    @default.setter
    def default(self, value):
        self._default = value

    def validate(self) -> List[Exception]:
        errors: List[Exception] = []
        if self.expectations is not None:
            for e in self.expectations:
                errors.extend(e.validate(self))
        errors.extend(self.validate_default_value(self.dtype, self.default))
        return errors

    def __str__(self):
        return f"Field({self.name}, {self.dtype}, {self.default})"

    def to_proto(self):
        field = proto.Field(
            name=self.name,
            dtype=self.dtype.to_proto(),
            default_value=self.to_value_proto(self.dtype, self.default),
        )
        return field


class Schema:
    fields: List[Field]

    def __init__(self, fields: List[Field]):
        self.fields = fields

    def validate(self) -> List[Exception]:
        exceptions = []
        seen_so_far = set()
        for field in self.fields:
            exceptions.extend(field.validate())
            if field.name in seen_so_far:
                exceptions.append(
                    Exception(f"field {field.name} provided multiple times")
                )
            seen_so_far.add(field.name)
        return exceptions

    def check_timestamp_field_exists(self) -> List[Exception]:
        for field in self.fields:
            if type(field.dtype) == FTimestamp:
                return []
        return [Exception("No timestamp field provided")]

    def to_proto(self):
        return proto.Schema(fields=[field.to_proto() for field in self.fields])

    def __str__(self):
        return f'Schema({", ".join([str(field) for field in self.fields])})'

    def get_fields_and_types(self):
        return {field.name: field.dtype for field in self.fields}
