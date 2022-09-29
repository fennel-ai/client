from fennel.gen.status_pb2 import Status

_ENABLE_LOGGING_TRACE_IDS = "ENABLE_LOGGING_TRACE_ID"
_REXER_TRACER_KEY = "rexer-traceid"

API_VERSION = "/internal/v1"


class Singleton(object):
    """Use to create a singleton"""

    def __new__(cls, *args, **kwds):
        it_id = "__it__"
        # getattr will dip into base classes, so __dict__ must be used
        it = cls.__dict__.get(it_id, None)
        if it is not None:
            return it
        it = object.__new__(cls)
        setattr(cls, it_id, it)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass


def check_response(response: Status):
    """Check the response from the server and raise an exception if the response is not OK"""
    if response.code != 200:
        raise Exception(response.message)
