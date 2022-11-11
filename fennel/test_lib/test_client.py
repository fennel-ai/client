# noinspection PyUnresolvedReferences
from fennel.client import Client


class ClientTestClient(Client):
    def __init__(self, stub):
        super().__init__(url="localhost:50051")
        self.stub = stub


class InternalTestClient(Client):
    def __init__(self, stub):
        super().__init__(url="localhost:50051")
        self.stub = stub
