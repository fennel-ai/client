import dataclasses
import datetime as datetime
import hashlib
import inspect
import json
from typing import Any


def _json_default(thing: Any):
    try:
        return dataclasses.asdict(thing)
    except TypeError:
        pass
    if isinstance(thing, datetime.datetime):
        return thing.isoformat(timespec='microseconds')

    if isinstance(thing, bytes):
        return thing.hex()

    if callable(thing) and not inspect.isclass(thing):
        return hashlib.md5(inspect.getsource(thing).encode('utf-8')).hexdigest()

    if isinstance(thing, datetime.timedelta):
        return str(thing.total_seconds())
    raise TypeError(f"object of type {type(thing).__name__} not hashable")


def _json_dumps(thing: Any):
    return json.dumps(
        thing,
        default=_json_default,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(',', ':'),
    )


def fhash(thing: Any):
    """Fennel Hash that is used to uniquely identify any python object."""
    return hashlib.md5(_json_dumps(thing).encode('utf-8')).hexdigest()
