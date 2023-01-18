# noinspection PyUnresolvedReferences
from fennel.client import Client


class InternalTestClient(Client):
    def __init__(self, stub):
        super().__init__(url="http://localhost:50051")
        self.stub = stub
