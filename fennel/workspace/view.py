import grpc
from typing import *

import fennel.gen.services_pb2 as services_pb2
import fennel.gen.services_pb2_grpc as services_pb2_grpc
from fennel.dataset import Dataset


# TODO(aditya): how will auth work?
class View:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.channel = grpc.insecure_channel(url)
        self.stub = services_pb2_grpc.FennelFeatureStoreStub(self.channel)
        self.to_register = set()
        self.to_register_objects = []

    def add(self, obj: Union[Dataset]):
        if isinstance(obj, Dataset):
            if obj.name in self.to_register:
                return
            self.to_register.add(obj.name)
            self.to_register_objects.append(obj)
        else:
            raise NotImplementedError

    def to_proto(self):
        datasets = []
        for obj in self.to_register_objects:
            if isinstance(obj, Dataset):
                datasets.append(obj.create_dataset_request_proto())
        return services_pb2.SyncRequest(
            dataset_requests=datasets
        )
