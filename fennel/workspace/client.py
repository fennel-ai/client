from typing import *

import grpc
import pandas as pd

import fennel.gen.services_pb2 as services_pb2
import fennel.gen.services_pb2_grpc as services_pb2_grpc
from fennel.dataset import Dataset
from fennel.featureset import Featureset
from fennel.utils import check_response

REST_API_VERSION = "/v1"


class Client:
    def __init__(self, url: str, rest_url: Optional[str] =
    None):
        self.rest_url = rest_url if rest_url is not None else url
        self.url = url
        self.channel = grpc.insecure_channel(url)
        self.stub = services_pb2_grpc.FennelFeatureStoreStub(self.channel)
        self.to_register = set()
        self.to_register_objects = []

    def add(self, obj: Union[Dataset, Featureset]):
        if isinstance(obj, Dataset):
            if obj.name in self.to_register:
                raise ValueError(f"Dataset {obj.name} already registered")
            self.to_register.add(obj.name)
            self.to_register_objects.append(obj)
        elif isinstance(obj, Featureset):
            if obj.name in self.to_register:
                raise ValueError(f"Featureset {obj.name} already registered")
            self.to_register.add(obj.name)
            self.to_register_objects.append(obj)
        else:
            raise NotImplementedError

    def to_proto(self):
        datasets = []
        featuresets = []
        for obj in self.to_register_objects:
            if isinstance(obj, Dataset):
                datasets.append(obj.create_dataset_request_proto())
            elif isinstance(obj, Featureset):
                featuresets.append(obj.create_featureset_request_proto())
        return services_pb2.SyncRequest(
            dataset_requests=datasets,
            featureset_requests=featuresets
        )

    def sync(self, datasets: List[Dataset] = [], featuresets: List[
        Featureset] = []):
        for dataset in datasets:
            self.add(dataset)
        for featureset in featuresets:
            self.add(featureset)
        sync_request = self.to_proto()
        self.stub.Sync(sync_request)

    def log(self, dataset_name: str, data: pd.DataFrame):
        """log api uses a REST endpoint to log data to a dataset rather than
        using a gRPC endpoint."""
        req = {
            "dataset": dataset_name,
            "data": data.to_json(orient="records")
        }
        response = self.http.post(self._url("log"), json=req)
        check_response(response)

    def _url(self, path):
        return self.rest_url + REST_API_VERSION + "/" + path
