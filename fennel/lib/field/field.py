from __future__ import annotations

from dataclasses import dataclass
from typing import (Dict, List, Optional, Tuple, Type,
                    TypeVar, Union, cast)

import pyarrow

import fennel.gen.dataset_pb2 as proto
from fennel.lib.schema import get_pyarrow_field
from fennel.utils import fhash

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar('T')


@dataclass
class Field:
    name: str
    key: bool
    timestamp: bool
    owner: str
    description: str
    pa_field: pyarrow.lib.Field
    dtype: Type

    def signature(self) -> str:
        if self.dtype is None:
            raise ValueError("dtype is not set")
        return fhash(self.name, f"{self.dtype}",
            f"{self.pa_field.nullable}:{self.key}:{self.timestamp}")

    def to_proto(self) -> proto.Field:
        # Type is passed as part of the dataset schema
        return proto.Field(
            name=self.name,
            is_key=self.key,
            is_timestamp=self.timestamp,
            owner=self.owner,
            description=self.description,
            is_nullable=self.pa_field.nullable,
        )


def get_field(
        cls: Type,
        annotation_name: str,
        dtype: Type,
        field2comment_map: Dict[str, str],
) -> Field:
    field = getattr(cls, annotation_name, None)
    if isinstance(field, Field):
        field.name = annotation_name
        field.dtype = dtype
    else:
        field = Field(name=annotation_name, key=False, timestamp=False,
            owner=None, description='', pa_field=None, dtype=dtype)

    if field.description is None or field.description == '':
        field.description = field2comment_map.get(annotation_name, '')
    field.pa_field = get_pyarrow_field(annotation_name, dtype)
    return field


def field(
        key: bool = False,
        timestamp: bool = False,
        description: Optional[str] = None,
        owner: Optional[str] = None,
) -> T:
    return cast(
        T,
        Field(
            key=key,
            timestamp=timestamp,
            owner=owner,
            description=description,
            # These fields will be filled in later.
            name='',
            pa_field=None,
            dtype=None,
        ),
    )
