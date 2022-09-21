import enum
from typing import *

import expectations as exps


class Type:
    def __str__(self):
        raise NotImplementedError()


class Int(Type):
    def __str__(self):
        return str('int')


class Bool(Type):
    def __str__(self):
        return str('bool')


class Double(Type):
    def __str__(self):
        return str('double')


class String(Type):
    def __str__(self):
        return str('string')


class Json(Type):
    def __int__(self, types: Dict[str, Type]):
        self.types = types

    def __str__(self):
        strs = []
        for k, v in self.types.items():
            strs.append(f'{k}: {v}')
        return 'json(' + ', '.join(strs) + ')'


class List(Type):
    def __init__(self, of: Type):
        self.of = of

    def __str__(self):
        return 'list(' + str(self.of) + ')'


class Dict(Type):
    def __int__(self, from_: Type, to_: Type):
        self.from_ = from_
        self.to_ = to_

    def __str__(self):
        return f'dict({self.from_}: {self.to_})'

@enum.Enum
class FieldType:
    None_ = 0
    Key = 1
    Value = 2
    Timestamp = 3


class Field:
    def __init__(self, name: str, datatype: Type, default: Any, expectations: List[exps.Expectation],
                 type_: FieldType=FieldType.None_):
        self.name = name
        self.datatype = datatype
        self.default = default
        self.type = type_
        self.expectations = expectations

    def validate(self) -> List[str]:
        # TODO(aditya): figure out what happens here
        pass


# TODO(aditya): what are the options for 'onerror'
class Schema:
    def __init__(self, *fields: List[Field]):
        self.fields = fields

    def validate(self, aggregate: bool) -> List[str]:
        errors = []
        key_fields = []
        value_fields = []
        timestamp_fields = []
        seen_so_far = set()
        for field in self.fields:
            errors.append(field.validate())
            if field.name in seen_so_far:
                errors.append(f'field {field.name} provided multiple times')
            seen_so_far.add(field.name)

            if not aggregate and field.type != FieldType.None_:
                errors.append(f'field {field.name} provides type when not needed')
                continue

            if field.type == FieldType.None_:
                errors.append(f'field {field.name} does not specify valid type - key, value, or timestamp')
            elif field.type == FieldType.Key:
                key_fields.append(field.name)
            elif field.type == FieldType.Value:
                value_fields.append(field.name)
            elif field.type == FieldType.Timestamp:
                timestamp_fields.append(field.name)
            else:
                errors.append(f'invalid field type: {field.type}')

        if aggregate:
            if len(key_fields) <= 0:
                errors.append("no field with type 'key' provided")
            if len(value_fields) <= 0:
                errors.append("no field with type 'value' provided")
            if len(timestamp_fields) != 1:
                errors.append(f'expected exactly one timestamp field but got: {len(timestamp_fields)} instead')

        return errors

    # TODO(aditya): figure out how to send it over the wire
    def __str__(self):
        pass

