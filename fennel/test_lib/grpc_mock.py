import pandas as pd
import pytest
from fennel.gen.status_pb2 import Status
from fennel.gen.services_pb2_grpc import FennelFeatureStoreServicer


@pytest.fixture(scope='module')
def grpc_add_to_server():
    from fennel.gen.services_pb2_grpc import add_FennelFeatureStoreServicer_to_server
    return add_FennelFeatureStoreServicer_to_server


@pytest.fixture(scope='module')
def grpc_servicer():
    return Servicer()


@pytest.fixture(scope='module')
def grpc_stub_cls(grpc_channel):
    from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
    return FennelFeatureStoreStub


class Servicer(FennelFeatureStoreServicer):
    def RegisterStream(self, request, context) -> Status:
        return Status(code=200, message=f'test')

    def RegisterAggregate(self, request, context) -> Status:
        print(request)
        print(context)
        return Status(code=200, message=f'test')
