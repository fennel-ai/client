from typing import Any

FENNEL_OUTPUTS = "__fennel_outputs__"


def outputs(*outs: Any):
    if len(outs) == 0:
        raise ValueError("No outputs specified")

    def decorator(func):
        setattr(func, FENNEL_OUTPUTS, outs)
        return func

    return decorator
