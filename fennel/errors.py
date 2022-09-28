from typing import Any, List, Optional, Union


class HTTPError(Exception):
    pass


class IncorrectSourceException(Exception):
    def __init__(self, table=None):
        if table:
            super().__init__(
                "Incorrect source, table must be None since it supports only a single stream."
            )
        else:
            super().__init__(
                "Incorrect source, table must be provided since it supports multiple streams/tables."
            )


class SchemaException(Exception):
    def __init__(self, errors: List[Exception]):
        super().__init__(f"Schema errors: {errors}")
