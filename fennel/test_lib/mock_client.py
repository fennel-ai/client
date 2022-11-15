import json
from concurrent import futures
from typing import Dict, List

import grpc
import pandas as pd
import pyarrow as pa
from requests import Response

from fennel.client import Client
from fennel.datasets import Dataset
from fennel.featuresets import Featureset
from fennel.gen.dataset_pb2 import CreateDatasetRequest
from fennel.gen.services_pb2_grpc import (
    add_FennelFeatureStoreServicer_to_server,
    FennelFeatureStoreStub,
    FennelFeatureStoreServicer,
)

TEST_PORT = 50051
TEST_DATA_PORT = 50052


class FakeResponse(Response):
    def __init__(self, status_code: int, content: str):
        self.status_code = status_code
        if status_code == 200:
            self._ok = True
            return
        self._content = json.dumps({"error": f"{content}"}, indent=2).encode(
            "utf-8"
        )
        self.encoding = "utf-8"


class MockClient(Client):
    def __init__(self, stub):
        super().__init__(url=f"localhost:{TEST_PORT}")
        self.stub = stub
        self.datasets: Dict[str, CreateDatasetRequest] = {}

    def log(self, dataset_name: str, df: pd.DataFrame):
        if dataset_name not in self.datasets:
            return FakeResponse(404, f"Dataset {dataset_name} not found")
        dataset = self.datasets[dataset_name]
        with pa.ipc.open_stream(dataset.schema) as reader:
            dataset_schema = reader.schema
            try:
                pa.RecordBatch.from_pandas(df, schema=dataset_schema)
            except Exception as e:
                return FakeResponse(status_code=400, content=str(e))
        return FakeResponse(200, "OK")

    def sync(
        self, datasets: List[Dataset] = [], featuresets: List[Featureset] = []
    ):
        for dataset in datasets:
            self.datasets[dataset.name] = dataset.create_dataset_request_proto()


class Servicer(FennelFeatureStoreServicer):
    def Sync(self, request, context):
        pass


def mock_client(test_func):
    def _start_a_test_server():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_FennelFeatureStoreServicer_to_server(Servicer(), server)
        server.add_insecure_port(f"[::]:{TEST_PORT}")
        server.start()
        return server

    def wrapper(*args, **kwargs):
        server = _start_a_test_server()
        with grpc.insecure_channel(f"localhost:{TEST_PORT}") as channel:
            stub = FennelFeatureStoreStub(channel)
            client = MockClient(stub)
            f = test_func(*args, **kwargs, client=client)
            server.stop(0)
            return f

    return wrapper
