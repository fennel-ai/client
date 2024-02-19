from typing import Any, Optional
from urllib.parse import urljoin

from fennel.client import Client


class LocalClient(Client):
    def __init__(self, url: str):
        Client.__init__(self, url)

    def _url(self, path):
        return urljoin(self.url, path)
