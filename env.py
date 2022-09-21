# TODO(aditya): explore if we can specify multiple dependencies in a single line
def pip_install(name: str, version: str):
    def decorator(fn):
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper()
    return decorator
