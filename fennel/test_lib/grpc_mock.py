# noinspection PyUnresolvedReferences
import pandas as pd
import pytest

# noinspection PyUnresolvedReferences
from fennel.gen.services_pb2_grpc import (
    add_FennelFeatureStoreServicer_to_server,
    FennelFeatureStoreServicer,
    FennelFeatureStoreStub,
)


@pytest.fixture(scope="module")
def grpc_add_to_server():
    return add_FennelFeatureStoreServicer_to_server


@pytest.fixture(scope="module")
def grpc_servicer():
    return Servicer()


@pytest.fixture(scope="module")
def grpc_stub_cls(grpc_channel):
    return FennelFeatureStoreStub


class Servicer(FennelFeatureStoreServicer):
    def Sync(self, request, context):
        pass
