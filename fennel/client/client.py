import functools
from typing import *

import grpc
import pandas as pd
import requests  # type: ignore

import fennel.gen.services_pb2 as services_pb2
import fennel.gen.services_pb2_grpc as services_pb2_grpc
from fennel.dataset import Dataset
from fennel.featureset import Featureset
from fennel.utils import check_response

REST_API_VERSION = "/api/v1"

# Connection timeout i.e. the time spent by the client to establish a connection to the remote machine.
#
# NOTE: This should be slightly larger than a multiple of 3 (this is the default TCP retransmission window)
_DEFAULT_CONNECT_TIMEOUT = 10
# Default request timeout.
_DEFAULT_TIMEOUT = 30


class Client:
    def __init__(self, url: str, rest_url: Optional[str] = None):
        self.rest_url = rest_url if rest_url is not None else url
        self.url = url
        self.channel = grpc.insecure_channel(url)
        self.stub = services_pb2_grpc.FennelFeatureStoreStub(self.channel)
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []
        self.http = self._get_session()

    @staticmethod
    def _get_session():
        http = requests.Session()
        http.request = functools.partial(
            http.request, timeout=(_DEFAULT_CONNECT_TIMEOUT, _DEFAULT_TIMEOUT)
        )
        return http

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
            dataset_requests=datasets, featureset_requests=featuresets
        )

    def sync(
        self, datasets: List[Dataset] = [], featuresets: List[Featureset] = []
    ):
        for dataset in datasets:
            self.add(dataset)
        for featureset in featuresets:
            self.add(featureset)
        sync_request = self.to_proto()
        self.stub.Sync(sync_request)

    def log(self, dataset_name: str, data: pd.DataFrame):
        """log api uses a REST endpoint to log data to a dataset rather than
        using a gRPC endpoint."""
        req = {"dataset": dataset_name, "data": data.to_json(orient="records")}
        response = self.http.post(self._url("log"), json=req)
        check_response(response)

    def extract_features(
        self,
        input_feature_list: List[str],
        output_feature_list: List[str],
        input_df: pd.DataFrame,
        timestamps: Optional[pd.Series] = None,
    ) -> pd.DataFrame:
        """Extract features from a dataframe."""
        ts = []
        if timestamps is not None:
            ts = timestamps.to_json(orient="records")

        req = {
            "input_features": input_feature_list,
            "output_features": output_feature_list,
            "input_df": input_df.to_json(orient="records"),
            "timestamps": ts,
        }
        response = self.http.post(self._url("extract_features"), json=req)
        check_response(response)
        return response.json()

    def _url(self, path):
        return self.rest_url + REST_API_VERSION + "/" + path
