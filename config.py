import typing


def depends_on(names: typing.List[str]):
    def decorator(fn: typing.Callable):
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapper.config_vars = names
    return decorator


# TODO: figure out how this will be implemented
def get(name: str) -> typing.Any:
    pass