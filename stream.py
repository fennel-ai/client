import datetime

import schema


class Stream:
    name = None
    version = 1
    retention = None
    # by default, get all the data
    start = datetime.datetime(1990, 1, 1)

    manual_logging = True # TODO: find a better name for this option

    @classmethod
    def schema(cls) -> schema.Schema:
        raise NotImplementedError()

    def _populate(self):
        pass

    @classmethod
    def version(cls) -> str:
        pass
