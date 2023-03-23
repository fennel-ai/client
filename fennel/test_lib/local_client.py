from typing import Any, List, Optional
from urllib.parse import urlparse

import grpc

import fennel.gen.services_pb2_grpc as services_pb2_grpc
from fennel.client import Client
from fennel.datasets import Dataset
from fennel.featuresets import Featureset

_DEFAULT_GRPC_TIMEOUT = 60


class LocalClient(Client):
    def __init__(
        self, grpc_url: str, rest_url: str, http_session: Optional[Any] = None
    ):
        Client.__init__(self, grpc_url, http_session)
        self.rest_url = rest_url
        self.channel = grpc.insecure_channel(urlparse(grpc_url).netloc)
        self.stub = services_pb2_grpc.FennelFeatureStoreStub(self.channel)

    def sync(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
    ):
        """
        Sync the client with the server. This will register any datasets or
        featuresets that have been added to the client or passed in as arguments.

        :param datasets: List of datasets to register with the server.
        :param featuresets:  List of featuresets to register with the server.
        :return: Response from the server.
        """
        # Reset state.
        self.to_register_objects = []
        self.to_register = set()

        if datasets is not None:
            for dataset in datasets:
                if not isinstance(dataset, Dataset):
                    raise TypeError(
                        f"Expected a list of datasets, got `{dataset.__name__}`"
                        f" of type `{type(dataset)}` instead."
                    )
                self.add(dataset)
        if featuresets is not None:
            for featureset in featuresets:
                if not isinstance(featureset, Featureset):
                    raise TypeError(
                        f"Expected a list of featuresets, got `{featureset.__name__}`"
                        f" of type `{type(featureset)}` instead."
                    )
                self.add(featureset)
        sync_request = self._get_sync_request_proto()
        self.stub.Sync(sync_request, timeout=_DEFAULT_GRPC_TIMEOUT)

    def _url(self, path):
        return self.rest_url + path
