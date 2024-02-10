# noinspection PyUnresolvedReferences
from fennel.client import Client


class InternalTestClient(Client):
    def __init__(self):
        super().__init__(url="http://localhost:50051", token="token")
