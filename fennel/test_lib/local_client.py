from typing import Any, Optional
from urllib.parse import urljoin

from fennel.client import Client


class LocalClient(Client):
    def __init__(self, grpc_url: str, rest_url: str):
        Client.__init__(self, grpc_url)
        self.rest_url = rest_url

    def _url(self, path):
        return urljoin(self.rest_url, path)
