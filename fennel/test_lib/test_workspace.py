# noinspection PyUnresolvedReferences
from fennel.workspace import Client


class ClientTestWorkspace(Client):
    def __init__(self, stub):
        super().__init__(url="localhost:50051")
        self.stub = stub


class InternalTestView(Client):
    def __init__(self, stub):
        super().__init__(url="localhost:50051")
        self.stub = stub
 