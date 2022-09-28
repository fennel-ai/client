import grpc
import pytest

from fennel.workspace import Workspace

class WorkspaceTest(Workspace):
    def __init__(self, stub):
        super().__init__(name='test', url='localhost:8080')
        self.stub = stub