from typing import *

import grpc

import fennel.gen.services_pb2 as services_pb2
import fennel.gen.services_pb2_grpc as services_pb2_grpc
from fennel.dataset import Dataset
from fennel.featureset import Featureset


# TODO(aditya): how will auth work?
class Client:
    def __init__(self, name: str, url: str):
        self.name = name
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
