# noinspection PyUnresolvedReferences
from fennel.workspace import View


class ClientTestWorkspace(View):
    def __init__(self, stub):
        super().__init__(name="test", url="localhost:50051")
        self.stub = stub


class InternalTestView(View):
    def __init__(self, stub):
        super().__init__(name="test", url="localhost:50051")
        self.stub = stub
