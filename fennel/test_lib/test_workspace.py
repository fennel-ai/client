import grpc
import pytest

from typing import List, Optional, Union
from fennel.workspace import Workspace
from fennel.stream import Stream
from fennel.aggregate import Aggregate
from fennel.gen.status_pb2 import Status


class WorkspaceTest(Workspace):
    def __init__(self, stub):
        super().__init__(name='test', url='localhost:8080')
        self.stub = stub

    def register_streams(self, *streams: List[Stream]):
        exceptions = []
        for stream in streams:
            exceptions.extend(stream.validate())

        if len(exceptions) > 0:
            raise Exception(exceptions)

        for stream in streams:
            resp = stream.register(self.stub)
        print("Registered streams:", [stream.name for stream in streams])

    def register_aggregates(self, *aggregates: List[Aggregate]) -> List[Status]:
        exceptions = []
        for agg in aggregates:
            exceptions.extend(agg.validate())

        if len(exceptions) > 0:
            raise Exception(exceptions)

        responses = []
        for agg in aggregates:
            resp = agg.register(self.stub)
            responses.append(resp)
        print("Registered aggregates:", [agg.name for agg in aggregates])
        return responses
