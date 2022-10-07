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


#
# # create_test_workspace fixture allows you to mock aggregates and features
# # You need to provide a dictionary of aggregate/feature and the return value.
# # Future work: Add support for providing ability to create mocks depending on the input.
# @pytest.fixture
# def create_test_workspace(grpc_stub, mocker):
#     def workspace(mocks):
#         def agg_side_effect(agg_name, *args, **kwargs):
#             for k, v in mocks.items():
#                 if type(k) == str:
#                     continue
#                 if agg_name == k.instance().name:
#                     return v
#             raise Exception(f'Mock for {agg_name} not found')
#
#         mocker.patch(__name__ + '.aggregate_lookup', side_effect=agg_side_effect)
#
#         def feature_side_effect(feature_name, *args, **kwargs):
#             for k, v in mocks.items():
#                 if type(k) != str:
#                     continue
#                 if feature_name == k:
#                     return v
#             raise Exception(f'Mock for {feature_name} not found')
#
#         mocker.patch(__name__ + '.feature_extract', side_effect=feature_side_effect)
#         return ClientTestWorkspace(grpc_stub, mocker)
#
#     return workspace


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
