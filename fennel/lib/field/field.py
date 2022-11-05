from __future__ import annotations

from dataclasses import dataclass
from typing import (List, Optional, Tuple, Type,
                    TypeVar, Union, cast)

import pyarrow

import fennel.gen.dataset_pb2 as proto
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

    def to_proto(self) -> proto.DatasetField:
        # Type is passed as part of the dataset schema
        return proto.DatasetField(
            name=self.name,
            is_key=self.key,
            is_timestamp=self.timestamp,
            owner=self.owner,
            description=self.description,
            is_nullable=self.pa_field.nullable,
        )


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
