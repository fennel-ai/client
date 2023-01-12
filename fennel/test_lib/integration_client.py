import json
import time
from typing import List, Set, Tuple, Union

import pandas as pd
import pyarrow as pa

try:
    from fennel_client_lib import RustClient  # type: ignore
    from fennel_dataset import lookup  # type: ignore
except ImportError:
    pass

from requests import Response

import fennel.datasets.datasets
import fennel.gen.services_pb2 as services_pb2
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature
from fennel.lib.to_proto import dataset_to_proto, featureset_to_proto


class FakeResponse(Response):
    def __init__(self, status_code: int, content: str):
        self.status_code = status_code

        self.encoding = "utf-8"
        if status_code == 200:
            self._ok = True
            self._content = json.dumps({}).encode("utf-8")
            return
        self._content = json.dumps({"error": f"{content}"}, indent=2).encode(
            "utf-8"
        )


def lookup_wrapper(
    ds_name: str, ts: pd.Series, properties: List[str], keys: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.Series]:
    # convert to pyarrow datastructures
    ts_pa = pa.Array.from_pandas(ts)
    keys_pa = pa.RecordBatch.from_pandas(keys)
    ret_pa, found_pa = lookup(ds_name, ts_pa, properties, keys_pa)

    # convert back to pandas
    return ret_pa.to_pandas(), found_pa.to_pandas()


class IntegrationClient:
    def __init__(self, tier_id, reset_db, is_airbyte_test):
        self._client = RustClient(
            tier_id=tier_id, reset_db=reset_db, airbyte=is_airbyte_test
        )
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []
        fennel.datasets.datasets.dataset_lookup = lookup_wrapper

    def is_integration_client(self):
        return True

    def log(self, dataset_name: str, df: pd.DataFrame):
        df_json = df.to_json(orient="records")
        try:
            self._client.log(dataset_name, df_json)
        except Exception as e:
            return FakeResponse(400, str(e))
        return FakeResponse(200, "OK")

    def sync(
        self, datasets: List[Dataset] = [], featuresets: List[Featureset] = []
    ):
        for dataset in datasets:
            self.add(dataset)
        for featureset in featuresets:
            self.add(featureset)
        sync_request = self._get_sync_request_proto()
        self._client.sync(sync_request.SerializeToString())
        time.sleep(1.1)
        return FakeResponse(200, "OK")

    def __del__(self):
        self._client.close()

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_df: pd.DataFrame,
    ) -> pd.DataFrame:
        if input_df.empty:
            return pd.DataFrame()

        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn_)
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn_ for f in input_feature.features]
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
                output_feature_names.append(output_feature.fqn_)
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn_ for f in output_feature.features]
                )

        input_df_json = input_df.to_json(orient="records")
        output_record_batch = self._client.extract_features(
            input_feature_names, output_feature_names, input_df_json
        )
        output_df = output_record_batch[0].to_pandas()
        return output_df

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
