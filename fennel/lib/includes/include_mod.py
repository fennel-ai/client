from __future__ import annotations

import functools
from typing import Callable, Any, List, Optional, Union
from fennel._vendor.pydantic import BaseModel, validator, ValidationError  # type: ignore

FENNEL_INCLUDED_MOD = "__fennel_included_module__"
FENNEL_TIER_SELECTOR = "__fennel_tier_selector__"


def includes(*args: Any):
    """
    Decorator to mark a function as a lib function for fennel. These
    functions are stored in the fennel backend and can be called as part of
    internal fennel modules.

    """

    @functools.wraps(includes)
    def decorator(func: Callable) -> Callable:
        setattr(func, FENNEL_INCLUDED_MOD, list(args))
        return func

    return decorator


class TierSelector(BaseModel):
    """
    TierSelector is a feature that can be added to entities to specify the tiers an entity supports.
    """

    tiers: Optional[Union[str, List[str]]] = None

    def __init__(self, tiers):
        super().__init__(tiers=tiers)

    @validator("tiers", pre=True, each_item=True, allow_reuse=True)
    def check_string(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            if " " in v:
                raise ValidationError(
                    "Tier string must not contain spaces, found", v
                )
            if len(v.strip()) == 0:
                raise ValidationError("Tier string must not be empty, found", v)
            if v == "~":
                raise ValidationError("Tier string must not be ~, found", v)
        return v

    @validator("tiers", allow_reuse=True)
    def validate_tiers(cls, v):
        if isinstance(v, str) or v is None:
            return v
        # Cannot contain tiers with ~ and without ~, should be one or the other
        if (
            v
            and any(tier.startswith("~") for tier in v)
            and any(not tier.startswith("~") for tier in v)
        ):
            raise ValidationError(
                "Cannot contain tiers with ~ and without ~, should be one or the other, found",
                v,
            )
        return v

    def is_entity_selected(self, tier: Optional[str] = None) -> bool:
        if self.tiers is None or tier is None:
            return True
        if tier[0] == "~":
            raise ValueError(
                "Tier selector cannot start with ~, found", tier[0]
            )

        if isinstance(self.tiers, str):
            if self.tiers[0] == "~":
                return self.tiers[1:] != tier
            return self.tiers == tier

        if any(t.startswith("~") for t in self.tiers):
            if any(t[1:] == tier for t in self.tiers):
                return False
            return True
        return tier in self.tiers

    class Config:
        arbitrary_types_allowed = True
