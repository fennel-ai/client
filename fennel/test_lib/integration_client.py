import json
import time

import pandas as pd
from typing import List, Set, Tuple, Union

from fennel.lib.schema import parse_json

try:
    import pyarrow as pa
    from fennel_client_lib import RustClient  # type: ignore
    from fennel_dataset import lookup  # type: ignore
except ImportError:
    pass

from fennel._vendor.requests import Response  # type: ignore

import fennel.datasets.datasets
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature
from fennel.lib.to_proto import to_sync_request_proto


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
    ds_name: str, ts: pd.Series, fields: List[str], keys: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.Series]:
    # convert to pyarrow datastructures
    ts_pa = pa.Array.from_pandas(ts)
    keys_pa = pa.RecordBatch.from_pandas(keys)
    ret_pa, found_pa = lookup(ds_name, ts_pa, fields, keys_pa)

    # convert back to pandas
    return ret_pa.to_pandas(), found_pa.to_pandas()


class IntegrationClient:
    def __init__(self, mode: str):
        self._client = RustClient()
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []
        self.mode = mode
        if self.mode == "local":
            self.sleep_time = 15
        else:
            self.sleep_time = 3
        fennel.datasets.datasets.dataset_lookup = lookup_wrapper  # type: ignore

    def log(self, webhook: str, endpoint: str, df: pd.DataFrame):
        df_json = df.to_json(orient="records")
        try:
            self._client.log(webhook, endpoint, df_json)
        except Exception as e:
            return FakeResponse(400, str(e))
        return FakeResponse(200, "OK")

    def sync(
        self, datasets: List[Dataset] = [], featuresets: List[Featureset] = []
    ):
        self.to_register_objects = []
        self.to_register = set()
        for dataset in datasets:
            self.add(dataset)
        for featureset in featuresets:
            self.add(featureset)

        sync_request = self._get_sync_request_proto()
        self._client.sync(sync_request.SerializeToString())
        time.sleep(1.1)
        return FakeResponse(200, "OK")

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: str = "default",
        sampling_rate: float = 1.0,
    ) -> pd.DataFrame:
        if input_dataframe.empty:
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
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )
        output_feature_names = []
        output_feature_name_to_type = {}
        for output_feature in output_feature_list:
            if isinstance(output_feature, Feature):
                output_feature_names.append(output_feature.fqn_)
                output_feature_name_to_type[
                    output_feature.fqn()
                ] = output_feature.dtype
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn_ for f in output_feature.features]
                )
                for f in output_feature.features:
                    output_feature_name_to_type[f.fqn()] = f.dtype

        input_df_json = input_dataframe.to_json(orient="records")
        output_record_batch = self._client.extract_features(
            input_feature_names,
            output_feature_names,
            input_df_json,
            log,
            workflow,
            sampling_rate,
        )
        df = output_record_batch.to_pandas()
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: parse_json(output_feature_name_to_type[col], x)
                )
        return df

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

    def is_integration_client(self):
        return True

    def integration_mode(self):
        return self.mode

    def sleep(self, seconds: float = 0):
        if seconds > 0:
            time.sleep(seconds)
        else:
            time.sleep(self.sleep_time)

    def log_to_dataset(self, dataset_name: str, df: pd.DataFrame):
        df_json = df.to_json(orient="records")
        try:
            self._client.log_to_dataset(dataset_name, df_json)
        except Exception as e:
            return FakeResponse(400, str(e))
        return FakeResponse(200, "OK")

    def _get_sync_request_proto(self):
        return to_sync_request_proto(self.to_register_objects)
