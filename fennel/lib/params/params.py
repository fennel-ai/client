from typing import Any

FENNEL_INPUTS = "__fennel_inputs__"
FENNEL_OUTPUTS = "__fennel_outputs__"


def inputs(*inps: Any):
    if len(inps) == 0:
        raise ValueError("No inputs specified")

    def decorator(func):
        setattr(func, FENNEL_INPUTS, inps)
        return func

    return decorator


def outputs(*outs: Any):
    if len(outs) == 0:
        raise ValueError("No outputs specified")

    def decorator(func):
        setattr(func, FENNEL_OUTPUTS, outs)
        return func

    return decorator
