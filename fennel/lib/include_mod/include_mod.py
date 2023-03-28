from __future__ import annotations

import functools
from typing import Callable, TypeVar, Any

T = TypeVar("T")
FENNEL_INCLUDED_MOD = "__fennel_included_module__"


def includes(*args: Any) -> Callable[[T], T]:
    """
    Decorator to mark a function as a lib function for fennel. These
    functions are stored in the fennel backend and can be called as part of
    internal fennel modules.

    """

    print(args)

    @functools.wraps(includes)
    def decorator(func: Callable) -> Callable:
        setattr(func, FENNEL_INCLUDED_MOD, list(args))
        return func

    return decorator


"""
4 modes
@lib
@lib(include=[..], version=int)
@lib(include=[])
@lib(version=int)

"""
