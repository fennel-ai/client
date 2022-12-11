from client_lib import RustClient  # type: ignore

from dataclasses import dataclass
from typing import Dict, List, Tuple, Set, Union

import pandas as pd
from fennel.client import Client
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature
import fennel.gen.services_pb2 as services_pb2


class IntegrationClient:
    def __init__(self):
        self._client = RustClient(tier_id=1234)
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []

    def log(self, dataset_name: str, df: pd.DataFrame):
        self._client.log(dataset_name)

    def sync(
        self, datasets: List[Dataset] = [], featuresets: List[Featureset] = []
    ):
        for dataset in datasets:
            self.add(dataset)
        for featureset in featuresets:
            self.add(featureset)
        sync_request = self._get_sync_request_proto()
        self._client.sync(sync_request.SerializeToString())

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_df: pd.DataFrame,
    ) -> pd.DataFrame:
        raise NotImplementedError

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
                datasets.append(obj.create_dataset_request_proto())
            elif isinstance(obj, Featureset):
                featuresets.append(obj.create_featureset_request_proto())
        return services_pb2.SyncRequest(
            dataset_requests=datasets, featureset_requests=featuresets
        )
