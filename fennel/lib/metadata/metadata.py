"""
Meta flags

We explicitly categorize some flags to be metaflags and define the semantics
that they can be changed without creating immutability issues. Hence, they will
never be part of the signature.

These meta flags are available to all the constructs
(datasets, pipelines, fields, featuresets, extractors, individual features,
pipelines etc.)

Meta flags are used to indicate the status of the object. They are used to
indicate whether the object is deprecated, deleted, or a work in progress.
"""

import functools
import re
from typing import Any, List, Optional

from pydantic import BaseModel, validator

import fennel.gen.metadata_pb2 as proto

EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
META_FIELD = "__fennel_metadata__"


class Metadata(BaseModel):
    owner: str
    tags: List[str]
    description: str
    deprecated: bool = False
    deleted: bool = False
    wip: bool = False

    @validator("owner")
    def owner_is_valid_email_address(cls, owner):
        if owner is None or owner == "":
            return owner
        if re.match(EMAIL_REGEX, owner) is None:
            raise ValueError(f"Invalid email '{owner}'")
        return owner

    @validator("wip")
    def meta_is_in_one_state(cls, wip, values):
        # Meta can only be in one of the states
        if wip + values["deleted"] + values["deprecated"] > 1:
            raise ValueError(
                "Meta can only be in one of the states wip, deleted, deprecated"
            )
        return wip


def meta(
    owner: str = "",
    description: str = "",
    tags: List[str] = [],
    deprecated: bool = False,
    deleted: bool = False,
    wip: bool = False,
):
    """meta decorator

    meta decorator is used to add a metadata object to the decorated object.
    It takes in the following arguments:
    Parameters
    ----------
    owner : str
        The owner of the object
    description : str
        A description of the object
    tags : List[str]
        A list of tags associated with the object
    deprecated : bool
        Whether the object is deprecated
    deleted : bool
        Whether the object is deleted
    wip : bool
        Whether the object is a work in progress
    """

    @functools.wraps(meta)
    def decorator(obj: Any):
        meta = Metadata(
            owner=owner,
            description=description,
            tags=tags,
            deprecated=deprecated,
            deleted=deleted,
            wip=wip,
        )
        setattr(obj, META_FIELD, meta)
        return obj

    return decorator


def get_meta(obj: Any) -> Optional[Metadata]:
    if not hasattr(obj, META_FIELD):
        return None
    return getattr(obj, META_FIELD)


def get_meta_attr(obj: Any, attr: str) -> Any:
    meta = get_meta(obj)
    if meta is None:
        return None
    return getattr(meta, attr)


def set_meta_attr(obj: Any, attr: str, value: Any):
    meta = get_meta(obj)
    if meta is None:
        meta = Metadata(owner="", description="", tags=[])
    setattr(meta, attr, value)
    setattr(obj, META_FIELD, meta)


def get_metadata_proto(obj: Any) -> Optional[proto.Metadata]:
    meta = get_meta(obj)
    if meta is None:
        return proto.Metadata()
    return proto.Metadata(
        owner=meta.owner,
        description=meta.description,
        tags=meta.tags,
        deprecated=meta.deprecated,
        deleted=meta.deleted,
        wip=meta.wip,
    )
