import functools
import gzip
import json
import math
import warnings
from datetime import datetime
from typing import Dict, Optional, Any, Set, List, Union, Tuple
from urllib.parse import urljoin

import pandas as pd

import fennel._vendor.requests as requests  # type: ignore
from fennel.datasets import Dataset
from fennel.featuresets import Featureset, Feature, is_valid_feature
from fennel.lib.schema import parse_json
from fennel.lib.to_proto import to_sync_request_proto
from fennel.sources import S3Connector
from fennel.utils import check_response, to_columnar_json

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
        preview=False,
        tier: Optional[str] = None,
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
        sync_request = self._get_sync_request_proto(tier)
        response = self._post_bytes(
            "{}/sync?preview={}".format(V1_API, str(preview).lower()),
            sync_request.SerializeToString(),
            False,
            300,
        )
        if response.headers.get("content-type") == "application/json":
            res_json = response.json()
            if response.status_code != 200:
                if "diffs" in res_json:
                    diffs = res_json["diffs"]
                    for line in diffs:
                        print(line, end="")
            elif "summary" in res_json:
                summary = res_json["summary"]
                for line in summary:
                    print(line, end="")

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
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
        num_rows = df.shape[0]
        if num_rows == 0:
            print(f"No rows to log to webhook {webhook}:{endpoint}")
            return

        for i in range(math.ceil(num_rows / batch_size)):
            start = i * batch_size
            end = min((i + 1) * batch_size, num_rows)
            mini_df = df[start:end]
            payload = to_columnar_json(mini_df)
            req = {
                "webhook": webhook,
                "endpoint": endpoint,
                "data": payload,
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

    def extract(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = None,
        sampling_rate: Optional[float] = None,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Extract features for a given output feature list from an input
        feature list. The features are computed for the current time.

        Parameters:
        inputs (List[Union[Feature, str]]): List of feature objects or fully qualified feature names (when providing a str) can be used as input. We don't allow adding featureset as input because if an engineer adds a new feature to the featureset it would break all extract calls running in production.
        outputs (List[Union[Feature, Featureset, str]]): List of feature or featureset objects or fully qualified feature names (when providing a str) to compute.
        input_dataframe (pd.DataFrame): Dataframe containing the input features.
        log (bool): Boolean which indicates if the extracted features should also be logged (for log-and-wait approach to training data generation). Default is False.
        workflow (Optional[str]): The name of the workflow associated with the feature extraction. Only relevant when log is set to True.
        sampling_rate (float): The rate at which feature data should be sampled before logging. Only relevant when log is set to True. The default value is 1.0.

        Returns:
        Union[pd.DataFrame, pd.Series]: Pandas dataframe or series containing the output features.
        """
        if input_dataframe.empty or len(outputs) == 0:
            return pd.DataFrame()

        input_feature_names = []
        for inp_feature in inputs:
            if isinstance(inp_feature, Feature):
                input_feature_names.append(inp_feature.fqn())
            elif isinstance(inp_feature, str) and is_valid_feature(inp_feature):
                input_feature_names.append(inp_feature)
            elif isinstance(inp_feature, Featureset):
                raise Exception(
                    "Providing a featureset as input is deprecated. "
                    f"List the features instead. {[f.fqn() for f in inp_feature.features]}."
                )

        # Check if the input dataframe has all the required features
        if not set(input_feature_names).issubset(set(input_dataframe.columns)):
            raise Exception(
                f"Input dataframe does not contain all the required features. "
                f"Required features: {input_feature_names}. "
                f"Input dataframe columns: {input_dataframe.columns}"
            )

        output_feature_names = []
        output_feature_name_to_type: Dict[str, Any] = {}
        for out_feature in outputs:
            if isinstance(out_feature, Feature):
                output_feature_names.append(out_feature.fqn())
                output_feature_name_to_type[out_feature.fqn()] = (
                    out_feature.dtype
                )
            elif isinstance(out_feature, str) and is_valid_feature(out_feature):
                output_feature_names.append(out_feature)
                output_feature_name_to_type[out_feature] = Any
            elif isinstance(out_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn() for f in out_feature.features]
                )
                for f in out_feature.features:
                    output_feature_name_to_type[f.fqn()] = f.dtype

        req = {
            "inputs": input_feature_names,
            "outputs": output_feature_names,
            "data": to_columnar_json(input_dataframe),
            "log": log,
        }  # type: Dict[str, Any]
        if workflow is not None:
            req["workflow"] = workflow
        if sampling_rate is not None:
            req["sampling_rate"] = sampling_rate

        response = self._post_json("{}/extract".format(V1_API), req)
        df = pd.DataFrame(response.json())
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].apply(
                    lambda x: parse_json(output_feature_name_to_type[col], x)
                )
        return df

    def extract_features(
        self,
        input_feature_list: List[Union[Feature, str]],
        output_feature_list: List[Union[Feature, Featureset, str]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = None,
        sampling_rate: Optional[float] = None,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        Going to be deprecated in favor of extract and will be removed in the future.
        Extract features for a given output feature list from an input
        feature list. The features are computed for the current time.

        Parameters:
        input_feature_list (List[Union[Feature, str]]): List of feature objects or fully qualified feature names (when providing a str) can be used as input. We don't allow adding featureset as input because if an engineer adds a new feature to the featureset it would break all extract calls running in production.
        output_feature_list (List[Union[Feature, Featureset, str]]): List of feature or featureset objects or fully qualified feature names (when providing a str) to compute.
        input_dataframe (pd.DataFrame): Dataframe containing the input features.
        log (bool): Boolean which indicates if the extracted features should also be logged (for log-and-wait approach to training data generation). Default is False.
        workflow (Optional[str]): The name of the workflow associated with the feature extraction. Only relevant when log is set to True.
        sampling_rate (float): The rate at which feature data should be sampled before logging. Only relevant when log is set to True. The default value is 1.0.

        Returns:
        Union[pd.DataFrame, pd.Series]: Pandas dataframe or series containing the output features.
        """
        warnings.warn(
            "This method is going to be deprecated in favor of extract."
        )
        return self.extract(
            inputs=input_feature_list,
            outputs=output_feature_list,
            input_dataframe=input_dataframe,
            log=log,
            workflow=workflow,
            sampling_rate=sampling_rate,
        )

    def extract_historical(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        timestamp_column: str,
        format: str = "pandas",
        input_dataframe: Optional[pd.DataFrame] = None,
        input_s3: Optional[S3Connector] = None,
        output_s3: Optional[S3Connector] = None,
        feature_to_column_map: Optional[Dict[Feature, str]] = None,
    ) -> Dict[str, Any]:
        """
        Extract point in time correct features from a dataframe, where the
        timestamps are provided by the timestamps parameter.

        Parameters:
        inputs (List[Union[Feature, str]]): List of feature objects or fully qualified feature names (when providing a str) can be used as input. We don't allow adding featureset as input because if an engineer adds a new feature to the featureset it would break all extract calls running in production.
        outputs (List[Union[Feature, Featureset, str]]): List of feature or featureset objects or fully qualified feature names (when providing a str) to compute.
        timestamp_column (str): The name of the column containing the timestamps.
        format (str): The format of the input data. Can be either "pandas",
            "csv", "json" or "parquet". Default is "pandas".
        input_dataframe (Optional[pd.DataFrame]): Dataframe containing the input features. Only relevant when format is "pandas".
        output_s3 (Optional[S3Connector]): Contains the S3 info -- bucket, prefix, and optional access key id
            and secret key -- used for storing the output of the extract historical request

        The following parameters are only relevant when format is "csv", "json" or "parquet".

        input_s3 (Optional[sources.S3Connector]): The info for the input S3 data, containing bucket, prefix, and optional access key id
            and secret key
        feature_to_column_map (Optional[Dict[Feature, str]]): A dictionary that maps columns in the S3 data to the required features.


        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
                        The status of the request.
        """
        if format not in ["pandas", "csv", "json", "parquet"]:
            raise Exception(
                "Invalid input format. "
                "Please provide one of the following formats: "
                "'pandas', 'csv', 'json' or 'parquet'."
            )

        input_feature_names = []
        for input_feature in inputs:
            if isinstance(input_feature, Feature):
                input_feature_names.append(input_feature.fqn())
            elif isinstance(input_feature, str):
                if "." not in input_feature:
                    raise Exception(
                        f"Invalid input feature name {input_feature}. "
                        "Please provide the feature name in the format <featureset>.<feature>."
                    )
                input_feature_names.append(input_feature)
            elif isinstance(input_feature, Featureset):
                raise Exception(
                    "Providing a featureset as input is deprecated. "
                    f"List the features instead. {[f.fqn() for f in input_feature.features]}."
                )

        input_info: Dict[str, Any] = {}
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
                    f"Required features: `{input_feature_names}`. "
                    f"Input dataframe columns: `{input_dataframe.columns}`"
                )
            if timestamp_column not in input_dataframe.columns:
                raise Exception(
                    f"Timestamp column `{timestamp_column}` not found in input dataframe."
                )
            # Convert timestamp column to string to make it JSON serializable
            input_dataframe[timestamp_column] = input_dataframe[
                timestamp_column
            ].astype(str)
            extract_historical_input["Pandas"] = input_dataframe.to_dict(
                orient="list"
            )
        else:
            if (
                input_s3 is None
                or input_s3.bucket_name is None
                or input_s3.path_prefix is None
            ):
                raise Exception(
                    "Input bucket and prefix not found in input dictionary. "
                    "Please provide a bucket name as value for the key 'input_bucket'."
                )

            input_info["s3_table"] = _s3_connector_dict(input_s3)
            input_info["compression"] = "None"

            if feature_to_column_map is not None:
                if len(feature_to_column_map) != len(input_feature_names):
                    raise Exception(
                        "Column mapping does not contain all the required features. "
                        f"Required features: {input_feature_names}. "
                        f"Column mapping: {feature_to_column_map}"
                    )
                for input_feature_name in input_feature_names:
                    if input_feature_name not in feature_to_column_map:
                        raise Exception(
                            f"Column mapping does not contain all the required features. Feature: {input_feature_name},"
                            f" not found in column mapping: {feature_to_column_map}"
                        )
                input_info["feature_to_column_map"] = feature_to_column_map  # type: ignore

            extract_historical_input["S3"] = input_info

        output_feature_names = []
        for output_feature in outputs:
            if isinstance(output_feature, Feature):
                output_feature_names.append(output_feature.fqn())
            elif isinstance(output_feature, str):
                if "." not in output_feature:
                    raise Exception(
                        f"Invalid output feature name {output_feature}. "
                        "Please provide the feature name in the format <featureset>.<feature>."
                    )
                output_feature_names.append(output_feature)
            elif isinstance(output_feature, Featureset):
                output_feature_names.extend(
                    [f.fqn() for f in output_feature.features]
                )

        req = {
            "input_features": input_feature_names,
            "output_features": output_feature_names,
            "input": extract_historical_input,
            "timestamp_column": timestamp_column,
            "s3_output": _s3_connector_dict(output_s3) if output_s3 else None,
        }
        return self._post_json("{}/extract_historical".format(V1_API), req)

    def extract_historical_features(
        self,
        input_feature_list: List[Union[Feature, str]],
        output_feature_list: List[Union[Feature, Featureset, str]],
        timestamp_column: str,
        format: str = "pandas",
        input_dataframe: Optional[pd.DataFrame] = None,
        input_s3: Optional[S3Connector] = None,
        output_s3: Optional[S3Connector] = None,
        feature_to_column_map: Optional[Dict[Feature, str]] = None,
    ) -> Dict[str, Any]:
        """
        Going to be deprecated in favor of extract_historical and will be removed in the future.
        Extract point in time correct features from a dataframe, where the
        timestamps are provided by the timestamps parameter.

        Parameters:
        input_feature_list (List[Union[Feature, str]]): List of feature objects or fully qualified feature names (when providing a str) can be used as input. We don't allow adding featureset as input because if an engineer adds a new feature to the featureset it would break all extract calls running in production.
        output_feature_list (List[Union[Feature, Featureset, str]]): List of feature or featureset objects or fully qualified feature names (when providing a str) to compute.
        timestamp_column (str): The name of the column containing the timestamps.
        format (str): The format of the input data. Can be either "pandas",
            "csv", "json" or "parquet". Default is "pandas".
        input_dataframe (Optional[pd.DataFrame]): Dataframe containing the input features. Only relevant when format is "pandas".
        output_s3 (Optional[S3Connector]): Contains the S3 info -- bucket, prefix, and optional access key id
            and secret key -- used for storing the output of the extract historical request

        The following parameters are only relevant when format is "csv", "json" or "parquet".

        input_s3 (Optional[sources.S3Connector]): The info for the input S3 data, containing bucket, prefix, and optional access key id
            and secret key
        feature_to_column_map (Optional[Dict[Feature, str]]): A dictionary that maps columns in the S3 data to the required features.


        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
                        The status of the request.
        """
        warnings.warn(
            "This method is going to be deprecated in favor of extract_historical."
        )
        return self.extract_historical(
            inputs=input_feature_list,
            outputs=output_feature_list,
            timestamp_column=timestamp_column,
            format=format,
            input_dataframe=input_dataframe,
            input_s3=input_s3,
            output_s3=output_s3,
            feature_to_column_map=feature_to_column_map,
        )

    def extract_historical_progress(self, request_id):
        """
        Get the status of extract historical features request.

        :param request_id: The request id returned by extract_historical.

        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
                        The status of the request.
        """
        return self._get(
            f"{V1_API}/extract_historical_request/status?request_id={request_id}"
        )

    def extract_historical_features_progress(self, request_id):
        """
        Going to be deprecated in favor of extract_historical_progress and will be removed in the future.
        Get the status of extract historical features request.

        :param request_id: The request id returned by extract_historical.

        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
                        The status of the request.
        """
        return self._get(
            f"{V1_API}/extract_historical_request/status?request_id={request_id}"
        )

    def extract_historical_cancel_request(self, request_id):
        """
        Cancel the extract historical features request.

        :param request_id: The request id returned by extract_historical.

        Returns:
        Dict[str, Any]: A dictionary containing the request_id, the output s3 bucket and prefix, the completion rate and the failure rate.
                        A completion rate of 1.0 indicates that all processing has been completed.
                        A failure rate of 0.0 indicates that all processing has been completed successfully.
                        The status of the request.
        """
        req = {"request_id": request_id}
        return self._post_json(
            "{}/extract_historical_request/cancel".format(V1_API), req
        )

    def lookup(
        self,
        dataset_name: str,
        keys: List[Dict[str, Any]],
        fields: List[str],
        timestamps: List[Union[int, str, datetime]] = None,
    ) -> Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        """
        Look up values of fields in a dataset given keys.
        Example:
              We have a dataset named Test which has two keyed fields named A and B,
              other fields are timestamp and C. Suppose we want to lookup this dataset against
              two values as of some time and now.

              keys = [
                {"A": "valueA1", "B": "valueB1"},
                {"A": "valueA2", "B": "valueB2"}
              ]
              fields = ["C"]
              timestamp = ["2019-02-01T00:00:00Z", "2019-02-01T00:00:00Z"] if want to do as of else None

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
        if timestamps:
            for idx in range(len(timestamps)):
                if isinstance(timestamps[idx], datetime):
                    timestamps[idx] = str(timestamps[idx])
            req["timestamp"] = timestamps
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

    def _get_sync_request_proto(self, tier: Optional[str] = None):
        if tier is not None and not isinstance(tier, str):
            raise ValueError(f"Expected tier to be a string, got {tier}")
        return to_sync_request_proto(self.to_register_objects, tier)

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


def _s3_connector_dict(s3: S3Connector) -> Dict[str, Any]:
    creds_json = None
    access_key_id, secret_access_key = s3.creds()
    if access_key_id is not None and len(access_key_id) > 0:
        if secret_access_key is None or len(secret_access_key) == 0:
            raise Exception("Access key id specified but secret key not found.")
        creds_json = {
            "access_key": access_key_id,
            "secret_key": secret_access_key,
        }
    elif secret_access_key is not None and len(secret_access_key) > 0:
        raise Exception("Secret key specified but access key id not found.")

    s3_table: Dict[str, Any] = {}
    s3_table["bucket"] = s3.bucket_name
    s3_table["path_prefix"] = s3.path_prefix
    s3_table["pre_sorted"] = s3.presorted
    if s3.format == "csv":
        s3_table["format"] = {"csv": {"delimiter": ord(s3.delimiter)}}
    else:
        s3_table["format"] = s3.format.lower()
    s3_table["db"] = {
        "name": "extract_historical_s3_input",
        "db": {"S3": {"creds": creds_json}},
    }
    return s3_table
