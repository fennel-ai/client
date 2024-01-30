import json
import gzip
from typing import Dict, Any

from fennel.client import Client

try:
    from fennel_client_lib import HttpServer  # type: ignore
except ImportError:
    pass
from fennel._vendor.requests import Response  # type: ignore


class IntegrationClient(Client):
    def __init__(
        self,
        url: str | None = None,
        branch: str | None = None,
        token: str | None = None,
    ):
        url = url or "dummy"
        token = token or "test-token"
        super().__init__(url, branch, token)
        self._http = HttpServer()

    def is_integration_client(self):
        return True

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
