import functools
import gzip
import json
import math
from urllib.parse import urljoin

import pandas as pd
from typing import Dict, Optional, Any, Set, List, Union, Tuple

import fennel._vendor.requests as requests  # type: ignore
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature
from fennel.lib.schema import parse_json
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

        Returns:
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
        output_feature_name_to_type = {}
        for output_feature in output_feature_list:
            if isinstance(output_feature, Feature):
                output_feature_names.append(output_feature.fqn())
                output_feature_name_to_type[
                    output_feature.fqn()
                ] = output_feature.dtype
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn() for f in output_feature.features]
                )
                for f in output_feature.features:
                    output_feature_name_to_type[f.fqn()] = f.dtype

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
        df = pd.DataFrame(response.json())
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: parse_json(output_feature_name_to_type[col], x)
                )
        return df

    def extract_historical_features(
        self,
        input_feature_list: List[Union[Feature, Featureset]],
        output_feature_list: List[Union[Feature, Featureset]],
        timestamp_column: str,
        format: str = "pandas",
        input_dataframe: Optional[pd.DataFrame] = None,
        input_bucket: Optional[str] = None,
        input_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Extract point in time correct features from a dataframe, where the
        timestamps are provided by the timestamps parameter.

        Parameters:
        input_feature_list (List[Union[Feature, Featureset]]): List of features or featuresets to use as input.
        output_feature_list (List[Union[Feature, Featureset]]): List of features or featuresets to compute.
        timestamp_column (str): The name of the column containing the timestamps.
        format (str): The format of the input data. Can be either "pandas",
            "csv", "json" or "parquet". Default is "pandas".
        input_dataframe (Optional[pd.DataFrame]): Dataframe containing the input features. Only relevant when format is "pandas".
        input_bucket (Optional[str]): The name of the S3 bucket containing the input data. Only relevant when format is "csv", "json" or "parquet".
        input_prefix (Optional[str]): The prefix of the S3 key containing the input data. Only relevant when format is "csv", "json" or "parquet".


        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
        """

        if format not in ["pandas", "csv", "json", "parquet"]:
            raise Exception(
                "Invalid input format. "
                "Please provide one of the following formats: "
                "'pandas', 'csv', 'json' or 'parquet'."
            )

        input_feature_names = []
        for input_feature in input_feature_list:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn())
            elif isinstance(input_feature, Featureset):
                input_feature_names.extend(
                    [f.fqn() for f in input_feature.features]
                )

        input_info = {}
        extract_historical_input = {}
        if format == "pandas":
            if input_dataframe is None:
                raise Exception(
                    "Input dataframe not found in input dictionary. "
                    "Please provide a dataframe as value for the key 'input_dataframe'."
                )
            if not isinstance(input_dataframe, pd.DataFrame):
                raise Exception(
                    "Input dataframe is not of type pandas.DataFrame, "
                    f"found {type(input_dataframe)}. "
                    "Please provide a dataframe as value for the key 'input_dataframe'."
                )
            if input_dataframe.empty:
                raise Exception(
                    "Input dataframe is empty. Please provide a non-empty dataframe."
                )
            if not set(input_feature_names).issubset(
                set(input_dataframe.columns)
            ):
                raise Exception(
                    f"Input dataframe does not contain all the required features. "
                    f"Required features: {input_feature_names}. "
                    f"Input dataframe columns: {input_dataframe.columns}"
                )
            if timestamp_column not in input_dataframe.columns:
                raise Exception(
                    f"Timestamp column {timestamp_column} not found in input dataframe."
                )
            extract_historical_input["Pandas"] = input_dataframe.to_dict(
                orient="records"
            )
        else:
            if input_bucket is None:
                raise Exception(
                    "Input bucket not found in input dictionary. "
                    "Please provide a bucket name as value for the key 'input_bucket'."
                )
            if input_prefix is None:
                raise Exception(
                    "Input prefix not found in input dictionary. "
                    "Please provide a prefix as value for the key 'input_prefix'."
                )
            input_info["input_bucket"] = input_bucket
            input_info["input_prefix"] = input_prefix
            input_info["format"] = format.upper()
            input_info["compression"] = "None"
            extract_historical_input["S3"] = input_info

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
            "input": extract_historical_input,
            "timestamp_column": timestamp_column,
        }

        return self._post_json(
            "{}/extract_historical_features".format(V1_API), req
        )

    def extract_historical_features_progress(self, request_id):
        """
        Get progress of extract historical features request.

        :param request_id: The request id returned by extract_historical_features.

        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
        """
        req = {"request_id": request_id}
        return self._post_json(
            "{}/extract_historical_progress".format(V1_API), req
        )

    def lookup(
        self, dataset_name: str, keys: List[Dict[str, Any]], fields: List[str]
    ) -> Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        """
        Look up values of fields in a dataset given keys.

        Parameters:
        dataset_name (str): The name of the dataset.
        keys (List[Dict[str, Any]]): A list of keys, key(s), and its value, is a represented in a dictionary.
        fields: A subset of fields of the dataset.

        Returns:
        Tuple[Union[pd.DataFrame, pd.Series], pd.Series]: A pair. The first is a Pandas dataframe or series containing values; the second is a series indicating whether the corresponding key(s) is found in the dataset.
        """
        req = {
            "keys": keys,
            "fields": fields,
        }
        response = self._post_json(
            "{}/inspect/datasets/{}/lookup".format(V1_API, dataset_name), req
        )
        resp_json = response.json()
        found = pd.Series(resp_json["found"])
        if len(fields) > 1:
            return pd.DataFrame(resp_json["data"]), found
        return pd.Series(resp_json["data"]), found

    def inspect_lastn(
        self, dataset_name: str, n: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Inspect the last n rows of a dataset.

        Parameters:
        dataset_name (str): The dataset name.
        n (int): The number of rows, default is 10.

        Returns:
        List[Dict[str, Any]]: A list of dataset rows.
        """
        return self._get(
            "{}/inspect/datasets/{}/lastn?n={}".format(V1_API, dataset_name, n)
        ).json()

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
