from urllib.parse import urljoin

from fennel.client import Client


class LocalClient(Client):
    def __init__(self, url: str, token: str):
        Client.__init__(self, url, token)

    def _url(self, path):
        return urljoin(self.url, path)
