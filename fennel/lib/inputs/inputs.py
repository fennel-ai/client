from typing import Any

FENNEL_INPUTS = "__fennel_inputs__"


def inputs(*inps: Any):
    if len(inps) == 0:
        raise ValueError("No inputs specified")

    def decorator(func):
        setattr(func, FENNEL_INPUTS, inps)
        return func

    return decorator
