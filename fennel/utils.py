from typing import Any

import cloudpickle

from fennel.gen.status_pb2 import Status


def check_response(response: Status):
    """Check the response from the server and raise an exception if the response is not OK"""
    if response.code != 200:
        raise Exception(response.message)


def del_namespace(obj):
    if not hasattr(obj, "__dict__"):
        return
    if "namespace" in obj.__dict__:
        obj.__dict__.pop("namespace")
    for k, v in obj.__dict__.items():
        if isinstance(v, dict):
            for k1, v1 in v.items():
                del_namespace(v1)
        elif isinstance(v, list):
            for v1 in v:
                del_namespace(v1)
        else:
            del_namespace(v)


def fennel_pickle(obj: Any) -> bytes:
    """Pickle an object using the Fennel protocol"""
    del_namespace(obj)
    return cloudpickle.dumps(obj)


def square(x):
    return x * x
