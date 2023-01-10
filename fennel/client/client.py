import functools
from typing import *

import grpc
import pandas as pd
import requests  # type: ignore

import fennel.gen.services_pb2 as services_pb2
import fennel.gen.services_pb2_grpc as services_pb2_grpc
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature
from fennel.lib.to_proto import dataset_to_proto, featureset_to_proto
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
            if obj._name in self.to_register:
                raise ValueError(f"Dataset {obj._name} already registered")
            self.to_register.add(obj._name)
            self.to_register_objects.append(obj)
        elif isinstance(obj, Featureset):
            if obj._name in self.to_register:
                raise ValueError(f"Featureset {obj._name} already registered")
            self.to_register.add(obj._name)
            self.to_register_objects.append(obj)
        else:
            raise NotImplementedError

    def _get_sync_request_proto(self):
        datasets = []
        featuresets = []
        for obj in self.to_register_objects:
            if isinstance(obj, Dataset):
                datasets.append(dataset_to_proto(obj))
            elif isinstance(obj, Featureset):
                featuresets.append(featureset_to_proto(obj))
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
        sync_request = self._get_sync_request_proto()
        self.stub.Sync(sync_request)

    def log(self, dataset_name: str, data: pd.DataFrame):
        """log api uses a REST endpoint to log data to a dataset rather than
        using a gRPC endpoint."""
        req = {
            "dataset_name": dataset_name,
            "payload": data.to_json(orient="records"),
        }
        response = self.http.post(self._url("log"), json=req)
        return response

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_df: pd.DataFrame,
    ) -> Union[pd.DataFrame, pd.Series]:
        """Extract features from a dataframe."""
        if input_df.empty:
            return pd.DataFrame()

        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn)
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn for f in input_feature.features]
                )

        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_df.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_df.columns}"
            )

        output_feature_names = []
        for output_feature in output_feature_list:
            if isinstance(output_feature, Feature):
                output_feature_names.append(output_feature.fqn)
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn for f in output_feature.features]
                )

        req = {
            "input_features": input_feature_names,
            "output_features": output_feature_names,
            "data": input_df.to_json(orient="records"),
        }
        response = self.http.post(
            self._url("extract_features"),
            json=req,
        )
        check_response(response)
        if len(output_feature_list) > 1:
            return pd.DataFrame(response.json())
        else:
            return pd.Series(response.json())

    def _url(self, path):
        return self.rest_url + REST_API_VERSION + "/" + path
