from abc import ABC
import pandas as pd
from typing import *

import schema


class Aggregate:
    name = None
    version = 1
    stream: str = None
    mode = 'pandas'
    windows = None

    @classmethod
    def schema(cls) -> schema.Schema:
        raise NotImplementedError()

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()

    @classmethod
    def version(cls) -> str:
        pass


class Count(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Min(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Max(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class KeyValue(Aggregate, ABC):
    static = False

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Rate(Aggregate, ABC):
    windows: List[int] = None

    @classmethod
    def _validate(cls) -> List[Exception]:
        raise NotImplementedError()


class Average(Aggregate, ABC):
    windows: List[int] = None

    def _validate(self) -> List[Exception]:
        raise NotImplementedError()
