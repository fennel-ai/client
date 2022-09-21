import dataclasses
import typing
from typing import *


@dataclasses.dataclass
class _Connector:
    type_: str
    config: Dict[str, Any]
    transformer: typing.Callable


def mysql(host: str, username: str, password: str, table: str, mode: str = 'pandas'):
    def decorator(fn):
        if mode == 'pandas':
            def wrapper(df: pd.DataFrame) -> pd.DataFrame:
                pass