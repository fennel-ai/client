import gzip
import json
import time
from functools import partial
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd

from fennel.client import Client
from fennel.datasets import Dataset
from fennel.featuresets import Featureset

try:
    import pyarrow as pa
    import sys

    sys.path.insert(
        0,
        "/nix/store/39900ybniz4fb8y3wn6vfh7vj8p8d0bv-python3-3.11.5-env/lib/python3.11/site-packages",
    )
    from fennel_client_lib import HttpServer  # type: ignore
    from fennel_dataset import lookup, get_secret  # type: ignore
except ImportError:
    pass

import fennel.datasets.datasets
from fennel._vendor.requests import Response  # type: ignore


def lookup_wrapper(
    branch: str,
    use_as_of: bool,
    ds_name: str,
    ts: pd.Series,
    fields: List[str],
    keys: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    # convert to pyarrow datastructures
    ts_pa = pa.Array.from_pandas(ts)
    keys_pa = pa.RecordBatch.from_pandas(keys)
    ret_pa, found_pa = lookup(
        branch, use_as_of, ds_name, ts_pa, fields, keys_pa
    )

    # convert back to pandas
    return ret_pa.to_pandas(types_mapper=pd.ArrowDtype), found_pa.to_pandas(
        types_mapper=pd.ArrowDtype
    )


class IntegrationClient(Client):
    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        branch: Optional[str] = None,
    ):
        url = url or "dummy"
        token = token or "caput-draconis"
        branch = branch or "main"
        use_as_of = False
        super().__init__(url, token, branch)
        self._http = HttpServer()
        fennel.datasets.datasets.dataset_lookup = partial(lookup_wrapper, branch, use_as_of)  # type: ignore
        fennel.lib.secrets.get = get_secret  # type: ignore

    def is_integration_client(self):
        return True

    def sleep(self, seconds: float = 7):
        time.sleep(seconds)

    def commit(
        self,
        message: str,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        env: Optional[str] = None,
        incremental: bool = False,
    ):
        resp = super().commit(
            message, datasets, featuresets, preview, env, incremental
        )
        # It takes a while to setup the server
        time.sleep(10)
        return resp

    def checkout(self, name: str, init: bool = False):
        if init and name not in self.list_branches():
            self.init_branch(name)
        self._branch = name
        fennel.datasets.datasets.dataset_lookup = partial(lookup_wrapper, name, False)  # type: ignore

    def init_branch(self, name: str):
        resp = super().init_branch(name)
        fennel.datasets.datasets.dataset_lookup = partial(
            lookup_wrapper, name, False
        )
        # Time taken by view to refresh
        time.sleep(7)
        return resp

    def clone_branch(self, name: str, from_branch: str):
        resp = super().clone_branch(name, from_branch)
        fennel.datasets.datasets.dataset_lookup = partial(
            lookup_wrapper, name, False
        )
        # Time taken by view to refresh
        time.sleep(7)
        return resp

    def _url(self, path: str):
        return path

    def _get(self, path: str):
        headers = self._add_branch_name_header({})
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        headers = list(headers.items())  # type: ignore
        code, content, content_type = self._http.get(
            self._url(path), headers=headers
        )
        if content_type == "application/json":
            content = json.loads(content)
        # HTTP sever returns code as a string
        code = int(code)
        if code != 200:
            raise Exception(f"Server returned: {code}, {content}")
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
        headers = self._add_branch_name_header(headers)
        headers = list(headers.items())  # type: ignore
        code, content, content_type = self._http.post(
            self._url(path), headers, data
        )
        # If response content type is json, parse it
        if content_type == "application/json":
            content = json.loads(content)
        # HTTP sever returns code as a string
        code = int(code)
        if code != 200:
            raise Exception(f"Server returned: {code}, {content}")
        return FakeResponse(code, content)

    def _patch(
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
        headers = self._add_branch_name_header(headers)
        headers = list(headers.items())  # type: ignore
        code, content, content_type = self._http.patch(
            self._url(path), headers, data
        )
        # If response content type is json, parse it
        if content_type == "application/json":
            content = json.loads(content)
        # HTTP sever returns code as a string
        code = int(code)
        if code != 200:
            raise Exception(f"Server returned: {code}, {content}")
        return FakeResponse(code, content)

    def _delete(
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
        headers = self._add_branch_name_header(headers)
        headers = list(headers.items())  # type: ignore
        code, content, content_type = self._http.delete(
            self._url(path), headers, data
        )
        # If response content type is json, parse it
        if content_type == "application/json":
            content = json.loads(content)
        # HTTP sever returns code as a string
        code = int(code)
        if code != 200:
            raise Exception(f"Server returned: {code}, {content}")
        return FakeResponse(code, content)


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
