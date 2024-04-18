from __future__ import annotations

import functools
from typing import Callable, Any, List, Optional, Union
from fennel._vendor.pydantic import BaseModel, validator, ValidationError  # type: ignore

FENNEL_INCLUDED_MOD = "__fennel_included_module__"
FENNEL_ENV_SELECTOR = "__fennel_env_selector__"


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


class EnvSelector(BaseModel):
    """
    EnvSelector is a feature that can be added to entities to specify the envs an entity supports.
    """

    environments: Optional[Union[str, List[str]]] = None

    def __init__(self, envs):
        super().__init__(environments=envs)

    @validator("environments", pre=True, each_item=True, allow_reuse=True)
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

    @validator("environments", allow_reuse=True)
    def validate_environments(cls, v):
        if isinstance(v, str) or v is None:
            return v
        # Cannot contain envs with ~ and without ~, should be one or the other
        if (
            v
            and any(env.startswith("~") for env in v)
            and any(not env.startswith("~") for env in v)
        ):
            raise ValidationError(
                "Cannot contain envs with ~ and without ~, should be one or the other, found",
                v,
            )
        return v

    def is_entity_selected(self, env: Optional[str] = None) -> bool:
        if self.environments is None or env is None:
            return True
        if env[0] == "~":
            raise ValueError("Tier selector cannot start with ~, found", env[0])

        if isinstance(self.environments, str):
            if self.environments[0] == "~":
                return self.environments[1:] != env
            return self.environments == env

        if any(t.startswith("~") for t in self.environments):
            if any(t[1:] == env for t in self.environments):
                return False
            return True
        return env in self.environments

    class Config:
        arbitrary_types_allowed = True
