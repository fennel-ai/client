import pytest
from google.protobuf import any_pb2

from fennel.gen.services_pb2_grpc import FennelFeatureStoreServicer
from fennel.gen.status_pb2 import Status


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
        resp = Status(code=200, message=request.name)
        msg = any_pb2.Any()
        msg.Pack(request)
        # For testing we add the request to the response, so we can check it.
        resp.details.append(msg)
        return resp

    def RegisterAggregate(self, request, context) -> Status:
        resp = Status(code=200, message=request.name)
        msg = any_pb2.Any()
        msg.Pack(request)
        resp.details.append(msg)
        return resp

    def RegisterFeature(self, request, context) -> Status:
        resp = Status(code=200, message=request.name)
        msg = any_pb2.Any()
        msg.Pack(request)
        resp.details.append(msg)
        return resp
