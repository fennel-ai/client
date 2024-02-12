import json
import gzip
import time
from typing import Dict, Any, List, Optional, Tuple, Union
from functools import partial

import pandas as pd
from fennel.client import Client
from fennel.datasets import Dataset
from fennel.featuresets import Featureset

try:
    import pyarrow as pa
    import sys

    sys.path.insert(
        0,
        "/nix/store/m5w8ccghyqai64lvbr28pj65barcgskf-python3-3.11.7-env/lib/python3.11/site-packages"
    )
    from fennel_client_lib import HttpServer  # type: ignore
    from fennel_dataset import lookup  # type: ignore
except ImportError:
    pass
import fennel.datasets.datasets
from fennel._vendor.requests import Response  # type: ignore


def lookup_wrapper(
    branch: str,
    ds_name: str,
    ts: pd.Series,
    fields: List[str],
    keys: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    # convert to pyarrow datastructures
    ts_pa = pa.Array.from_pandas(ts)
    keys_pa = pa.RecordBatch.from_pandas(keys)
    ret_pa, found_pa = lookup(branch, ds_name, ts_pa, fields, keys_pa)

    # convert back to pandas
    return ret_pa.to_pandas(), found_pa.to_pandas()


class IntegrationClient(Client):
    def __init__(
        self,
        url: str | None = None,
        token: str | None = None,
        branch: str | None = None,
    ):
        url = url or "dummy"
        token = token or "caput-draconis"
        branch = branch or "main"
        super().__init__(url, token, branch)
        self._http = HttpServer()
        fennel.datasets.datasets.dataset_lookup = partial(lookup_wrapper, branch)  # type: ignore

    def is_integration_client(self):
        return True

    def sleep(self, seconds: float = 7):
        time.sleep(seconds)

    def commit(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        tier: Optional[str] = None,
    ):
        resp = super().commit(datasets, featuresets, preview, tier)
        # It takes a while to setup the server
        time.sleep(10)
        return resp

    def checkout(self, name: str):
        self._branch = name
        fennel.datasets.datasets.dataset_lookup = partial(lookup_wrapper, name)  # type: ignore

    def _url(self, path: str):
        return path

    def _get(self, path: str):
        headers = None

        if self.token:
            headers = {}
            headers["Authorization"] = "Bearer " + self.token
        code, content = self._http.get(self._url(path), headers=headers)
        # HTTP sever returns code as a string
        code = int(code)
        return FakeResponse(code, content)

    def _post(
        self,
        path: str,
        data: Any,
        headers: Dict[str, str],
        compress: bool = False,
        timeout: float = 30,
    ):
        if compress:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        headers = list(headers.items())
        code, content = self._http.post(self._url(path), headers, data)
        # HTTP sever returns code as a string
        code = int(code)
        return FakeResponse(code, content)

    def close(self):
        self._http.close()
    def __del__(self):
        self._http.close()
        time.sleep(7)

class FakeResponse(Response):
    def __init__(self, status_code: int, content: str):
        self.status_code = int(status_code)

        self.encoding = "utf-8"
        self.headers = {"Content-Type": "application/json"}
        if status_code == 200:
            self._ok = True
            self._content = json.dumps(content).encode("utf-8")
            self.headers = {"Content-Type": "application/json"}
        else:
            self._ok = False
            self._content = json.dumps(
                {"error": f"{content}"}, indent=2
            ).encode("utf-8")
