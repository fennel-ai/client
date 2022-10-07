from typing import List

from fennel.aggregate import Aggregate
from fennel.gen.status_pb2 import Status
from fennel.stream import Stream
from fennel.workspace import Workspace


class ClientTestWorkspace(Workspace):
    def __init__(self, stub, mocker):
        super().__init__(name='test', url='localhost:8080')
        self.stub = stub
        self.mocker = mocker


class InternalTestWorkspace(Workspace):
    def __init__(self, stub):
        super().__init__(name='test', url='localhost:8080')
        self.stub = stub

    def register_streams(self, *streams: List[Stream]):
        exceptions = []
        for stream in streams:
            exceptions.extend(stream.validate())

        if len(exceptions) > 0:
            raise Exception(exceptions)

        responses = []
        for stream in streams:
            resp = stream.register(self.stub)
            responses.append(resp)

        print("Registered test streams:", [stream.name for stream in streams])
        return responses

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

        print("Registered test aggregates:", [agg.name for agg in aggregates])
        return responses

    def register_features(self, *features):
        exceptions = []
        for feature in features:
            exceptions.extend(feature.validate())
        if len(exceptions) > 0:
            raise Exception(exceptions)

        responses = []
        for feature in features:
            print("Registering feature:", feature.name)
            resp = feature.register(self.stub)
            responses.append(resp)

        return responses
