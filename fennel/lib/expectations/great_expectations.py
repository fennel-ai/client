from typing import cast, Callable, List, Optional

GE_ATTR_FUNC = "__fennel_great_expectations_func__"
FENNEL_GE_SUITE = "fennel_expectation_suite"


class Expectations:
    suite: str
    version: int
    func: Callable
    expectations: List

    def __init__(self, version: int, func: Callable):
        self.version = version
        self.func = func  # type: ignore


def expectations(func: Optional[Callable] = None, version: int = 0):
    """
    expectations is a decorator for a function that defines Great
    Expectations on a dataset or featureset.
    """

    def _create_expectations(c: Callable, version: int):
        if not callable(c):
            raise TypeError("expectations can only be applied to functions")

        setattr(c, GE_ATTR_FUNC, Expectations(version, c))
        return c

    def wrap(c: Callable):
        return _create_expectations(c, version)

    if func is None:
        # We're being called as @expectations(version=int).
        if version is None:
            raise TypeError("version must be specified as an integer.")
        if not isinstance(version, int):
            raise TypeError("version for expectations must be an int.")
        return wrap

    func = cast(Callable, func)
    # @expectations decorator was used without arguments
    return wrap(func)
