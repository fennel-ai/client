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

import fennel.gen.metadata_pb2 as proto
from fennel._vendor.pydantic import BaseModel, validator  # type: ignore
from fennel.internal_lib import EMAIL_REGEX, META_FIELD, OWNER


def desc(obj):
    return get_meta_attr(obj, "description") or ""


class Metadata(BaseModel):
    owner: str
    tags: List[str]
    description: str
    deprecated: bool = False
    deleted: bool = False

    @validator("owner")
    def owner_is_valid_email_address(cls, owner):
        if owner is None or owner == "":
            return owner
        if re.match(EMAIL_REGEX, owner) is None:
            raise ValueError(f"Invalid email '{owner}'")
        return owner


def meta(
    owner: str = "",
    description: str = "",
    tags: List[str] = [],
    deprecated: bool = False,
    deleted: bool = False,
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
    """

    @functools.wraps(meta)
    def decorator(obj: Any):
        meta = Metadata(
            owner=owner,
            description=description,
            tags=tags,
            deprecated=deprecated,
            deleted=deleted,
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


def get_metadata_proto(
    obj: Any,
) -> proto.Metadata:
    owner = getattr(obj, OWNER, "")
    meta = get_meta(obj)
    if meta is None:
        return proto.Metadata(owner=owner)
    if meta.owner != "":
        owner = meta.owner
    return proto.Metadata(
        owner=owner,
        description=meta.description,
        tags=meta.tags,
        deprecated=meta.deprecated,
        deleted=meta.deleted,
    )
