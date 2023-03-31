from __future__ import annotations

import functools
from typing import Callable, Any

FENNEL_INCLUDED_MOD = "__fennel_included_module__"


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
