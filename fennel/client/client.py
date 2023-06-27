import functools
import gzip
import json
import math
from urllib.parse import urljoin

import pandas as pd
from typing import Dict, Optional, Any, Set, List, Union

import fennel._vendor.requests as requests  # type: ignore
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature
from fennel.lib.to_proto import to_sync_request_proto
from fennel.utils import check_response

V1_API = "/api/v1"

# Connection timeout i.e. the time spent by the client to establish a
# connection to the remote machine. NOTE: This should be slightly larger than
# a multiple of 3 (this is the default TCP retransmission window)
_DEFAULT_CONNECT_TIMEOUT = 10
# Default request timeout(s).
_DEFAULT_TIMEOUT = 30


class Client:
    def __init__(self, url: str, token: Optional[str] = None):
        self.url = url
        self.token = token
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []
        self.http = self._get_session()

    def add(self, obj: Union[Dataset, Featureset]):
        """
        Add a dataset or featureset to the client. This will not register the
        dataset or featureset with the server until the sync method is called.

        Parameters:
        obj (Union[Dataset, Featureset]): Dataset or Featureset to add to the client.

        Returns:
        None
        """
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

    def sync(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
    ):
        """
        Sync the client with the server. This will register any datasets or
        featuresets that have been added to the client or passed in as arguments.

        Parameters:
        datasets (Optional[List[Dataset]]): List of datasets to register with the server.
        featuresets (Optional[List[Featureset]]):  List of featuresets to register with the server.

        Returns:
        None
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
        response = self._post_bytes(
            "{}/sync".format(V1_API),
            sync_request.SerializeToString(),
            False,
            300,
        )
        check_response(response)

    def log(
        self,
        webhook: str,
        endpoint: str,
        dataframe: pd.DataFrame,
        batch_size: int = 1000,
    ):
        """
        Log data to a webhook.

        Parameters:
        webhook (str): Name of the webhook to log data to.
        endpoint (str): Endpoint within the webhook to log data to.
        dataframe (pd.DataFrame): Dataframe to log to the dataset.
        batch_size (int): Batch size to use when logging data.

        Returns:
        Dict: response from the server
        """
        num_rows = dataframe.shape[0]
        if num_rows == 0:
            print(f"No rows to log to webhook {webhook}:{endpoint}")
            return

        for i in range(math.ceil(num_rows / batch_size)):
            start = i * batch_size
            end = min((i + 1) * batch_size, num_rows)
            mini_df = dataframe[start:end]
            payload = mini_df.to_json(orient="records")
            req = {
                "webhook": webhook,
                "endpoint": endpoint,
                "rows": payload,
            }
            response = self._post_json("{}/log".format(V1_API), req)
        return response

    def list_datasets(self) -> List[Dict]:
        """
        List definitions of all the datasets.

        Returns:
        List[Dict]: A list of dataset definitions.
        """
        return self._get("{}/definitions/datasets".format(V1_API)).json()

    def dataset_definition(self, dataset_name: str) -> Dict:
        """
        Returns a single dataset definition.

        Parameters:
        dataset_name (str):  Name of the dataset.

        Returns:
        Dict: The dataset definition.
        """
        return self._get(
            "{}/definitions/datasets/{}".format(V1_API, dataset_name)
        ).json()

    def list_pipelines(self, dataset_name: str) -> List[Dict]:
        """
        List definitions of pipelines of a dataset.

        Parameters:
        dataset_name (str):  Name of the dataset.

        Returns:
        List[Dict]: A list of dataset definitions.
        """
        return self._get(
            "{}/definitions/datasets/{}/pipelines".format(V1_API, dataset_name)
        ).json()

    def pipeline_definition(
        self, dataset_name: str, pipeline_name: str
    ) -> Dict:
        """
        Returns the definition of a single pipeline in the dataset.

        Parameters:
        dataset_name (str):  Name of the dataset.
        pipeline_name (str): Name of the pipeline.

        Returns:
        Dict: The pipeline definition.
        """
        return self._get(
            "{}/definitions/datasets/{}/pipelines/{}".format(
                V1_API, dataset_name, pipeline_name
            )
        ).json()

    def list_featuresets(self) -> List[Dict]:
        """
        List definitions of all the featuresets.

        Returns:
        List[Dict]: A list of featureset definitions.
        """
        return self._get("{}/definitions/featuresets".format(V1_API)).json()

    def featureset_definition(self, featureset_name: str) -> Dict:
        """
        Returns the definition of a featureset.

        Parameters:
        featureset_name (str): Name of the featureset.

        Returns:
        Dict: The featureset definition.
        """
        return self._get(
            "{}/definitions/featuresets/{}".format(V1_API, featureset_name)
        ).json()

    def list_extractors(self, featureset_name: str) -> List[Dict]:
        """
        List definitions of extractors of a featureset.

        Parameters:
        featureset_name (str): Name of the featureset.

        Returns:
        List[Dict]: A list of extractor definitions.
        """
        return self._get(
            "{}/definitions/featuresets/{}/extractors".format(
                V1_API, featureset_name
            )
        ).json()

    def extractor_definition(
        self, featureset_name: str, extractor_name: str
    ) -> Dict:
        """
        Returns the definition of a extractor in the featureset.

        Parameters:
        featureset_name (str): Name of the featureset.
        extractor_name (str): Name of the extractor.

        Returns:
        Dict: The extractor definition.
        """
        return self._get(
            "{}/definitions/featuresets/{}/extractors/{}".format(
                V1_API, featureset_name, extractor_name
            )
        ).json()

    def list_features(self, featureset_name: str) -> List[Dict]:
        """
        List definitions of features of a featureset.

        Parameters:
        featureset_name (str): Name of the featureset.

        Returns:
        List[Dict]: A list of feature definitions.
        """
        return self._get(
            "{}/definitions/featuresets/{}/features".format(
                V1_API, featureset_name
            )
        ).json()

    def feature_definition(
        self, featureset_name: str, feature_name: str
    ) -> Dict:
        """
        Returns the definition of a feature in the featureset.

        Parameters:
        featureset_name (str): Name of the featureset.
        feature_name (str): Name of the feature.

        Returns:
        Dict: The feature definition.
        """
        return self._get(
            "{}/definitions/featuresets/{}/features/{}".format(
                V1_API, featureset_name, feature_name
            )
        ).json()

    def list_sources(self) -> List[Dict]:
        """
        List definitions of sources.

        Returns:
        List[Dict]: A list of source definitions.
        """
        return self._get("{}/definitions/sources".format(V1_API)).json()

    def source_definition(self, source_uuid: str) -> Dict:
        """
        Returns the definition of a source.

        Parameters:
        source_uuid (str): The uuid of the source.

        Returns:
        Dict: The source definition.
        """
        return self._get(
            "{}/definitions/sources/{}".format(V1_API, source_uuid)
        ).json()

    def _get(self, path: str):
        headers = None
        if self.token:
            headers = {}
            headers["Authorization"] = "Bearer " + self.token
        response = self.http.request(
            "GET",
            self._url(path),
            headers=headers,
            timeout=_DEFAULT_TIMEOUT,
        )
        check_response(response)
        return response

    def _post_json(
        self, path: str, req: Dict[str, Any], compress: bool = False
    ):
        payload = json.dumps(req).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
        }
        return self._post(path, payload, headers, compress)

    def _post_bytes(
        self,
        path: str,
        req: bytes,
        compress: bool = False,
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        headers = {
            "Content-Type": "application/octet-stream",
        }
        return self._post(path, req, headers, compress, timeout)

    def _post(
        self,
        path: str,
        data: Any,
        headers: Dict[str, str],
        compress: bool = False,
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        if compress:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        response = self.http.request(
            "POST",
            self._url(path),
            data=data,
            headers=headers,
            timeout=timeout,
        )
        check_response(response)
        return response

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = None,
        sampling_rate: Optional[float] = None,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Extract features for a given output feature list from an input
        feature list. The features are computed for the current time.

        Parameters:
        input_feature_list (List[Union[Feature, Featureset]]): List of features or featuresets to use as input.
        output_feature_list (List[Union[Feature, Featureset]]): List of features or featuresets to compute.
        input_dataframe (pd.DataFrame): Dataframe containing the input features.
        log (bool): Boolean which indicates if the extracted features should also be logged (for log-and-wait approach to training data generation). Default is False.
        workflow (Optional[str]): The name of the workflow associated with the feature extraction. Only relevant when log is set to True.
        sampling_rate (float): The rate at which feature data should be sampled before logging. Only relevant when log is set to True. The default value is 1.0.

        Returns
        Union[pd.DataFrame, pd.Series]: Pandas dataframe or series containing the output features.
        """
        if input_dataframe.empty or len(output_feature_list) == 0:
            return pd.DataFrame()

        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn())
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn() for f in input_feature.features]
                )

        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )

        output_feature_names = []
        for output_feature in output_feature_list:
            if isinstance(output_feature, Feature):
                output_feature_names.append(output_feature.fqn())
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn() for f in output_feature.features]
                )

        req = {
            "input_features": input_feature_names,
            "output_features": output_feature_names,
            "data": input_dataframe.to_json(orient="records"),
            "log": log,
        }
        if workflow is not None:
            req["workflow"] = workflow
        if sampling_rate is not None:
            req["sampling_rate"] = sampling_rate

        response = self._post_json("{}/extract_features".format(V1_API), req)
        check_response(response)
        if len(output_feature_list) > 1 or isinstance(
            output_feature_list[0], Featureset
        ):
            return pd.DataFrame(response.json())
        else:
            return pd.Series(response.json())

    def extract_historical_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        input_dataframe: pd.DataFrame,
        timestamps: pd.Series,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Extract point in time correct features from a dataframe, where the
        timestamps are provided by the timestamps parameter.

        Parameters:
        input_feature_list (List[Union[Feature, Featureset]]): List of features or featuresets to use as input.
        output_feature_list (List[Union[Feature, Featureset]]): List of features or featuresets to compute.
        input_dataframe (pd.DataFrame): Dataframe containing the input features.
        timestamps (pd.Series): Timestamps for each row in the input dataframe.

        Returns:
        Union[pd.DataFrame, pd.Series]: Pandas dataframe or series containing the output features.
        """
        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn())
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn() for f in input_feature.features]
                )

        if input_dataframe.empty:
            raise Exception("Input dataframe is empty")
            # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )

        output_feature_names = []
        for output_feature in output_feature_list:
            if isinstance(output_feature, Feature):
                output_feature_names.append(output_feature.fqn())
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn() for f in output_feature.features]
                )

        req = {
            "input_features": input_feature_names,
            "output_features": output_feature_names,
            "data": input_dataframe.to_json(orient="records"),
            "timestamps": timestamps.to_json(orient="records"),
        }

        response = self._post_json(
            "{}/extract_historical_features".format(V1_API), req
        )
        if len(output_feature_list) > 1 or isinstance(
            output_feature_list[0], Featureset
        ):
            return pd.DataFrame(response.json())
        else:
            return pd.Series(response.json())

    # ----------------------- Private methods -----------------------

    def _url(self, path):
        return urljoin(self.url, path)

    @staticmethod
    def _get_session():
        http = requests.Session()
        http.request = functools.partial(
            http.request,
            timeout=(_DEFAULT_CONNECT_TIMEOUT, _DEFAULT_TIMEOUT),
        )
        return http

    def _get_sync_request_proto(self):
        return to_sync_request_proto(self.to_register_objects)
