from concurrent import futures
from functools import partial
from unittest.mock import patch

import grpc
# noinspection PyUnresolvedReferences
import pandas as pd
import pytest
from google.protobuf import any_pb2

# noinspection PyUnresolvedReferences
from fennel.aggregate import aggregate_lookup
# noinspection PyUnresolvedReferences
from fennel.feature import feature_extract
from fennel.gen.services_pb2_grpc import (
    add_FennelFeatureStoreServicer_to_server,
    FennelFeatureStoreServicer,
    FennelFeatureStoreStub,
)
from fennel.gen.status_pb2 import Status
from fennel.test_lib.test_workspace import ClientTestWorkspace


@pytest.fixture(scope="module")
def grpc_add_to_server():
    return add_FennelFeatureStoreServicer_to_server


@pytest.fixture(scope="module")
def grpc_servicer():
    return Servicer()


@pytest.fixture(scope="module")
def grpc_stub_cls(grpc_channel):
    return FennelFeatureStoreStub


class workspace:
    port = 50051

    def __init__(self, aggregate_mock=None, feature_mock=None):
        self.aggregate_mock = aggregate_mock
        self.feature_mock = feature_mock

    def _start_a_test_server(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_FennelFeatureStoreServicer_to_server(Servicer(), self.server)
        self.server.add_insecure_port(f"[::]:{self.port}")
        self.server.start()

    @staticmethod
    def mock_aggregate(aggregate_mock, agg_name, *args, **kwargs):
        for k, v in aggregate_mock.items():
            if type(k) == str:
                continue
            if agg_name == k.name:
                return v
        raise Exception(f"Mock for {agg_name} not found")

    @staticmethod
    def mock_feature(feature_mock, feature_name, *args, **kwargs):
        print("$", feature_mock)
        print("$", feature_name)
        for k, v in feature_mock.items():
            if type(k) != str:
                continue
            if feature_name == k:
                return v
        raise Exception(f"Mock for {feature_name} not found")

    def agg_mock_method(self):
        if "mock_aggregate" not in self.__class__.__dict__:
            raise Exception("static function mock_aggregate not found")
        return self.__class__.__dict__["mock_aggregate"].__func__

    def feature_mock_method(self):
        if "mock_feature" not in self.__class__.__dict__:
            raise Exception("static function mock_feature not found")
        return self.__class__.__dict__["mock_feature"].__func__

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            self._start_a_test_server()
            with grpc.insecure_channel(f"localhost:{self.port}") as channel:
                with patch("fennel.aggregate.aggregate.aggregate_lookup") \
                        as agg_mock:
                    agg_mock.side_effect = partial(
                        self.agg_mock_method(), self.aggregate_mock
                    )
                    with patch(
                            "fennel.feature.feature.feature_extract"
                    ) as feature_mock:
                        feature_mock.side_effect = partial(
                            self.feature_mock_method(), self.feature_mock
                        )
                        stub = FennelFeatureStoreStub(channel)
                        workspace = ClientTestWorkspace(stub)
                        return func(*args, **kwargs, workspace=workspace)
            self.server.stop(None)

        return wrapper


class Servicer(FennelFeatureStoreServicer):
    def RegisterStream(self, request, context) -> Status:
        resp = Status(code=0, message=request.name)
        msg = any_pb2.Any()
        msg.Pack(request)
        # For testing we add the request to the response, so we can check it.
        resp.details.append(msg)
        return resp

    def RegisterAggregate(self, request, context) -> Status:
        resp = Status(code=0, message=request.name)
        msg = any_pb2.Any()
        msg.Pack(request)
        resp.details.append(msg)
        return resp

    def RegisterFeature(self, request, context) -> Status:
        resp = Status(code=0, message=request.name)
        msg = any_pb2.Any()
        msg.Pack(request)
        resp.details.append(msg)
        return resp
