import typing


# TODO(aditya): explore if we can specify multiple dependencies in a single line
def pip_install(name: str, version: str):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper()

    return decorator


def depends_on(names: typing.List[str]):
    def decorator(fn: typing.Callable):
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper.config_vars = names

    return decorator


# TODO: figure out how this will be implemented
def get(name: str) -> typing.Any:
    pass
