import functools
import gzip
import json
import math
import re
from datetime import datetime
from typing import Dict, Optional, Any, Set, List, Union, Tuple
from urllib.parse import urljoin

import numpy as np
import pandas as pd

import fennel._vendor.requests as requests  # type: ignore
from fennel.connectors import S3Connector, CSV
from fennel.datasets import Dataset
from fennel.featuresets import (
    Featureset,
    Feature,
    is_valid_feature,
    is_valid_featureset,
)
from fennel.internal_lib.schema import (
    parse_json,
    get_datatype,
    fennel_is_optional,
)
from fennel.internal_lib.to_proto import to_sync_request_proto
from fennel.internal_lib.utils import cast_col_to_pandas
from fennel.utils import check_response, to_columnar_json

V1_API = "/api/v1"

# Connection timeout i.e. the time spent by the client to establish a
# connection to the remote machine. NOTE: This should be slightly larger than
# a multiple of 3 (this is the default TCP retransmission window)
_DEFAULT_CONNECT_TIMEOUT = 10
# Default request timeout(s).
_DEFAULT_TIMEOUT = 30
# Name of the default branch
_MAIN_BRANCH = "main"
_BRANCH_HEADER_NAME = "X-FENNEL-BRANCH"


class Client:
    def __init__(
        self,
        url: str,
        token: str,
        branch: Optional[str] = None,
    ):
        self.url = url
        self.token = token
        self.to_register: Set[str] = set()
        self.to_register_objects: List[Union[Dataset, Featureset]] = []
        self.http = self._get_session()
        if branch is not None:
            self._branch = branch
        else:
            self._branch = _MAIN_BRANCH

    def add(self, obj: Union[Dataset, Featureset]):
        """Add a dataset or featureset to the client.

        This will not register the dataset or featureset with the server until
        the sync method is called.

        Parameters:
        ----------
        obj (Union[Dataset, Featureset]): Dataset or Featureset to add to the client.

        Returns:
        ----------
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

    def commit(
        self,
        message: str,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        env: Optional[str] = None,
        incremental: bool = False,
    ):
        """Commit the changes to the branch pointed to by the client.

        This will register any datasets or featuresets that have been added to
        the client or passed in as arguments.

        Parameters:
        ----------
        message (str): human readable description of the changes in the commit.
        datasets (Optional[List[Dataset]]): List of datasets to register with
            the server.
        featuresets (Optional[List[Featureset]]):  List of featuresets to
            register with the server.
        preview (bool): If True, the commit will be a preview and no changes will be applied.
        env (Optional[str]): The environment to register the datasets and featuresets in.
        incremental (bool):  If the commit is only used for adding datasets and featuresets and not changing
            anything existing.

        Returns:
        ----------
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
        sync_request = self._get_sync_request_proto(message, env)
        response = self._post_bytes(
            f"{V1_API}/commit?preview={str(preview).lower()}&incremental={str(incremental).lower()}",
            sync_request.SerializeToString(),
            False,
            300,
        )
        if response.headers.get("Content-Type") == "application/json":
            res_json = response.json()
        else:
            res_json = {}
        if "summary" in res_json:
            summary = res_json["summary"]
            for line in summary:
                print(line, end="")
        else:
            print("Commit was successful however summary generation failed.")
        return response

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        batch_size: int = 1000,
    ):
        """Log data to a webhook.

        Parameters:
        ----------
        webhook (str): Name of the webhook to log data to.
        endpoint (str): Endpoint within the webhook to log data to.
        dataframe (pd.DataFrame): Dataframe to log to the dataset.
        batch_size (int): Batch size to use when logging data.

        Returns: Dict
        ----------
        Returns the response from the server.

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

    def query(
        self,
        inputs: List[Union[Feature, str]],
        outputs: List[Union[Feature, Featureset, str]],
        input_dataframe: pd.DataFrame,
        log: bool = False,
        workflow: Optional[str] = None,
        sampling_rate: Optional[float] = None,
    ) -> Union[pd.DataFrame, pd.Series]:
        """Query current value of output features given values of input feature.

        Parameters:
        ----------
        inputs (List[Union[Feature, str]]): List of feature objects or fully
            qualified feature names (when providing a str) can be used as input.
            Unlike `outputs`, it's not allowed to pass full Featuresets as
            inputs.

        outputs (List[Union[Feature, Featureset, str]]): List of feature or
            featureset objects or fully qualified feature names (when providing
            a str) or featureset names to compute.

        input_dataframe (pd.DataFrame): Dataframe containing the input features.

        log (bool): Boolean which indicates if the extracted features should
            also be logged (for log-and-wait approach to training data
            generation). Default is False.

        workflow (Optional[str]): The name of the workflow associated with the
            feature extraction. Only relevant when log is set to True.
        sampling_rate (float): The rate at which feature data should be sampled
            before logging. Only relevant when log is set to True. The default
            value is 1.0.

        Returns: Union[pd.DataFrame, pd.Series]:
        ----------
        Pandas dataframe or series containing the output features.

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
            elif isinstance(out_feature, str):
                if is_valid_feature(out_feature):
                    output_feature_names.append(out_feature)
                    output_feature_name_to_type[out_feature] = Any
                elif is_valid_featureset(out_feature):
                    output_feature_names.append(out_feature)
                else:
                    raise Exception(
                        f"Please provide a valid string for outputs, got : `{out_feature}`."
                    )
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

        response = self._post_json(
            "{}/query".format(
                V1_API,
            ),
            req,
        )
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())
        if isinstance(response.json(), str):
            # Assuming the issue is double-encoding
            decoded_once = json.loads(response.text)
            actual_data = json.loads(decoded_once)
            # Now, `actual_data` should be a dictionary
        else:
            actual_data = response.json()
        df = self._parse_dataframe(
            pd.DataFrame(actual_data), output_feature_name_to_type
        )
        return df

    def erase(
        self, dataset_name: Union[str, Dataset], erase_keys: pd.DataFrame
    ):
        """
        Issue erasure of data that associated with the erase key, any new data that
        associated with the key will be purge and won't be reflected in downstream dataset.

        Example:
            We have a dataset named Test which has two erase keyed fields named A and B
            Suppose we want to erase these key from this dataset

            keys = [
                {"A": "valueA1", "B": "valueB1"},
                {"A": "valueA2", "B": "valueB2"}
            ]
            dataset_name = "dataset_name"

        Parameters:
        dataset_name (Union[str, Dataset]): The name of the dataset.
        erase_keys (pd.DataFrame): A list of keys, key(s), and its value, is a represented in a dictionary.

        Returns:
        Response status
        """
        req = {
            "erase_keys": erase_keys.to_dict(orient="records"),
        }
        if isinstance(dataset_name, Dataset):
            dataset_name = dataset_name.__name__
        elif isinstance(dataset_name, str):
            dataset_name = dataset_name
        else:
            raise TypeError(
                f"Expected a list of datasets, got `{dataset_name.__name__}`"
                f" of type `{type(dataset_name)}` instead."
            )
        return self._post_json(
            "{}/dataset/{}/erase".format(V1_API, dataset_name), req
        )

    # ----------------------- Branch API's -----------------------------------

    def init_branch(self, name: str):
        """Create a new empty branch.

        The client will be checked out to the new branch.
        Raises an exception if a branch with the same name already exists.

        Parameters:
        ----------
        name (str): The name of the branch to create.

        Raises an exception if a branch with the same name already exists.

        """
        _validate_branch_name(name)
        self._branch = name
        return self._post_json(f"{V1_API}/init", {})

    def clone_branch(self, name: str, from_branch: str):
        """Clone a branch from another branch.

        After cloning, the client will be checked out to the new branch.

        Raises an exception if the branch to clone from does not exist or
        if the branch to create already exists.

        Parameters:
        ----------
        name (str): The name of the branch to create.
        from_branch (str): The name of the branch to clone from.

        """
        req = {"clone_from": from_branch}
        self._branch = name
        return self._post_json(f"{V1_API}/init", req)

    def delete_branch(self, name: str):
        """Delete a branch.

        If successful, the client will be checked out to the
        main branch. Raises an exception if the branch does not exist.

        Parameters:
        ----------
        name (str): The name of the branch to delete.

        """
        if name == _MAIN_BRANCH:
            raise Exception("Cannot delete the main branch.")

        cur_branch = self._branch
        self.checkout(name)
        response = self._post_json(f"{V1_API}/delete", {})
        if name == cur_branch:
            self.checkout(_MAIN_BRANCH)
        else:
            self.checkout(cur_branch)
        return response

    def list_branches(self) -> List[str]:
        """List all branches that the user has access to.

        Returns:
        List[str]: A list of branch names.
        ----------

        """
        resp = self._get(f"{V1_API}/branch/list")
        resp = resp.json()
        if "branches" not in resp:
            raise Exception(
                "Server returned invalid response for list branches."
            )
        return [branch.get("name") for branch in resp["branches"]]

    def checkout(self, name: str):
        """Checkouts the client to another branch.

        Parameters:
        ----------
        name (str): The name of the branch to checkout.

        """
        self._branch = name

    def branch(self) -> str:
        """Get the name of the branch that the client is pointing to.

        Returns: str
        ----------
        The current branch name.

        """
        return self._branch

    # ----------------------- Extract historical API's -----------------------

    def query_offline(
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
        """Extract point in time correct values of output features.

        Extracts feature values using the input features given as a dataframe.
        The timestamps are provided by the timestamps parameter.

        Parameters:
        ----------
        inputs (List[Union[Feature, str]]): List of feature objects or fully
            qualified feature names (when providing a str) can be used as input.
            Unlike `outputs`, it's not allowed to pass full Featuresets as
            inputs.

        outputs (List[Union[Feature, Featureset, str]]): List of feature or
            featureset objects or fully qualified feature names (when providing
            a str) or featureset names to compute.

        timestamp_column (str): The name of the column containing timestamps.

        format (str): The format of the input data. Can be either "pandas",
            "csv", "json" or "parquet". Default is "pandas".

        input_dataframe (Optional[pd.DataFrame]): Dataframe containing the input
            features. Only relevant when format is "pandas".

        output_s3 (Optional[S3Connector]): Contains the S3 info -- bucket,
            prefix, and optional access key id and secret key -- used for
            storing the output of the extract historical request

        The following parameters are only relevant when format is "csv", "json"
            or "parquet".

        input_s3 (Optional[connectors.S3Connector]): The info for the input S3
            data, containing bucket, prefix, and optional access key id and
            secret key

        feature_to_column_map (Optional[Dict[Feature, str]]): A dictionary that
            maps columns in the S3 data to the required features.


        Returns: Dict[str, Any]:
        ----------
        A dictionary containing the request_id, the output s3 bucket and prefix,
        the completion rate and the failure rate.  A completion rate of 1.0
        indicates that all processing has been completed. A failure rate of 0.0
        indicates that all processing has been completed successfully.
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
                if is_valid_feature(output_feature):
                    output_feature_names.append(output_feature)
                elif is_valid_featureset(output_feature):
                    output_feature_names.append(output_feature)
                else:
                    raise Exception(
                        f"Please provide a valid string for output_feature, got : `{output_feature}`."
                    )
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
        return self._post_json(
            "{}/query_offline".format(
                V1_API,
            ),
            req,
        )

    def track_offline_query(self, request_id):
        """Get the progress of query offline run.

        Parameters:
        ----------
        :param request_id: The request id returned by query_offline.

        Returns: Dict[str, Any]:
        ----------
        A dictionary containing the request_id, the output s3 bucket and prefix,
        the completion rate and the failure rate.  A completion rate of 1.0
        indicates that all processing has been completed. A failure rate of 0.0
        indicates that all processing has been completed successfully.

        """
        return self._get(
            f"{V1_API}/query_offline/status?request_id={request_id}"
        )

    def cancel_offline_query(self, request_id):
        """Cancel the query offline run.

        Parameters:
        ----------
        :param request_id: The request id returned by query_offline.

        Returns: Dict[str, Any]:
        ----------
        A dictionary containing the request_id, the output s3 bucket and prefix,
        the completion rate and the failure rate.  A completion rate of 1.0
        indicates that all processing has been completed. A failure rate of 0.0
        indicates that all processing has been completed successfully.

        """
        return self._post_json(
            f"{V1_API}/query_offline/cancel?request_id={request_id}",
            {},
        )

    # ----------------------- Debug API's --------------------------------------

    def lookup(
        self,
        dataset: Union[str, Dataset],
        keys: pd.DataFrame,
        fields: Optional[List[str]] = None,
        timestamps: Optional[pd.Series] = None,
    ) -> Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        """Look up values of fields in a dataset given keys.

        Parameters:
        ----------
        dataset (Union[str, Dataset]): The name of the dataset or Dataset object.
        keys (pd.DataFrame): All the keys to lookup.
        fields: (Optional[List[str]]): The fields to lookup. If None, all
            fields are returned.
        timestamps (Optional[pd.Series]): The timestamps to lookup. If None,
            the current time is used.

        Returns: Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        -------------
        Returns a pair - The first element is a Pandas dataframe or series
        containing values; the second is a boolean series indicating whether the
        corresponding key(s) is found in the dataset.

        """
        dataset_name = dataset if isinstance(dataset, str) else dataset._name
        keys = keys.to_dict(orient="records")
        fields = fields if fields else []
        req = {
            "keys": keys,
            "fields": fields,
        }
        if isinstance(timestamps, pd.Series):
            timestamps = timestamps.tolist()
        if timestamps:
            for idx in range(len(timestamps)):
                if isinstance(timestamps[idx], datetime):
                    timestamps[idx] = str(timestamps[idx])
            req["timestamps"] = timestamps
        response = self._post_json(
            "{}/dataset/{}/lookup".format(V1_API, dataset_name),
            req,
        )
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())
        resp_json = response.json()
        found = pd.Series(resp_json["found"])
        if len(fields) == 1:
            return (
                pd.Series(
                    name=fields[0], data=resp_json["data"][fields[0]]
                ).fillna(pd.NA),
                found,
            )
        result = pd.DataFrame(resp_json["data"])

        # Get the schema
        output_dtypes = {}
        for column in result.columns:
            if isinstance(dataset, Dataset):
                for field in dataset.fields:
                    if field.name == column:
                        dtype = field.dtype
                        if fennel_is_optional(dtype):
                            output_dtypes[column] = dtype
                        else:
                            output_dtypes[column] = Optional[dtype]
            else:
                output_dtypes[column] = Any

        return self._parse_dataframe(result, output_dtypes), found

    def inspect(
        self, dataset: Union[str, Dataset], n: int = 10
    ) -> pd.DataFrame:
        """Inspect the last n rows of a dataset.

        Parameters:
        ----------
        dataset (str): The name of the dataset or Dataset object.
        n (int): The number of rows, default is 10.

        Returns: pd.DataFrame
        ----------
        Dataset in pd.DataFrame.

        """
        dataset_name = dataset if isinstance(dataset, str) else dataset._name
        response = self._get(
            "{}/dataset/{}/inspect?n={}".format(V1_API, dataset_name, n)
        )
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())

        result = pd.DataFrame(response.json())

        # Get the schema
        output_dtypes = {}
        for column in result.columns:
            if isinstance(dataset, Dataset):
                for field in dataset.fields:
                    if field.name == column:
                        output_dtypes[column] = field.dtype
            else:
                output_dtypes[column] = Any

        return self._parse_dataframe(result, output_dtypes)

    # ----------------------- Secret API's -----------------------------------

    def get_secret(self, secret_name: str) -> Optional[str]:
        response = self._get("{}/secret/{}".format(V1_API, secret_name))
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())
        return response.json().get("value")

    def add_secret(self, secret_name: str, secret_value: str):
        data = {"value": secret_value}
        response = self._post_json(
            "{}/secret/{}".format(V1_API, secret_name),
            data,
        )
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())
        return response

    def update_secret(self, secret_name: str, secret_value: str):
        data = {"value": secret_value}
        response = self._patch_json(
            "{}/secret/{}".format(V1_API, secret_name),
            data,
        )
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())
        return response

    def delete_secret(self, secret_name: str):
        data: Dict[str, Any] = dict()
        response = self._delete_json(
            "{}/secret/{}".format(V1_API, secret_name),
            data,
        )
        if response.status_code != requests.codes.OK:
            raise Exception(response.json())
        return response

    # ----------------------- Private methods -----------------------

    def _url(self, path):
        return urljoin(self.url, path)

    def _add_branch_name_header(
        self, headers: Dict[str, str]
    ) -> Dict[str, str]:
        headers[_BRANCH_HEADER_NAME] = self._branch
        return headers

    @staticmethod
    def _get_session():
        http = requests.Session()
        http.request = functools.partial(
            http.request,
            timeout=(_DEFAULT_CONNECT_TIMEOUT, _DEFAULT_TIMEOUT),
        )
        return http

    def _get_sync_request_proto(
        self, message: str = "", env: Optional[str] = None
    ):
        if env is not None and not isinstance(env, str):
            raise ValueError(f"Expected env to be a string, got {env}")
        return to_sync_request_proto(self.to_register_objects, message, env)

    def _get(self, path: str):
        headers = {}

        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        response = self.http.request(
            "GET",
            self._url(path),
            headers=self._add_branch_name_header(headers),
            timeout=_DEFAULT_TIMEOUT,
        )
        check_response(response)
        return response

    def _patch_json(
        self, path: str, req: Dict[str, Any], compress: bool = False
    ):
        """
        Internal function for making PATCH calls to an endpoint with assumption that payload is json.
        """
        payload = json.dumps(req).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
        }
        return self._patch(path, payload, headers, compress)

    def _delete_json(
        self, path: str, req: Dict[str, Any], compress: bool = False
    ):
        """
        Internal function for making PATCH calls to an endpoint with assumption that payload is json.
        """
        payload = json.dumps(req).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
        }
        return self._delete(path, payload, headers, compress)

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
            headers=self._add_branch_name_header(headers),
            timeout=timeout,
        )
        check_response(response)
        return response

    def _patch(
        self,
        path: str,
        data: Any,
        headers: Dict[str, str],
        compress: bool = False,
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        """
        Internal function for making PATCH calls to an endpoint.
        """
        if compress:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        response = self.http.request(
            "PATCH",
            self._url(path),
            data=data,
            headers=self._add_branch_name_header(headers),
            timeout=timeout,
        )
        check_response(response)
        return response

    def _delete(
        self,
        path: str,
        data: Any,
        headers: Dict[str, str],
        compress: bool = False,
        timeout: float = _DEFAULT_TIMEOUT,
    ):
        """
        Internal function for making PATCH calls to an endpoint.
        """
        if compress:
            data = gzip.compress(data)
            headers["Content-Encoding"] = "gzip"
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        response = self.http.request(
            "DELETE",
            self._url(path),
            data=data,
            headers=self._add_branch_name_header(headers),
            timeout=timeout,
        )
        check_response(response)
        return response

    @staticmethod
    def _parse_dataframe(
        df: pd.DataFrame, schema: Dict[str, Any]
    ) -> pd.DataFrame:
        """
        This function parses the dataframe coming from server into correct dtype. The server returns response in
        JSON format. The response is first converted into a dataframe then this function is called.:
        1. Making struct from dict.
        2. Casting columns into pandas dtype.
        3. Converting None into pd.NA.
        If user passes string in either query, lookup, inspect then there's no casting just converting None to pd.NA
        """
        for column in df.columns:
            dtype = schema.get(column, Any)
            if dtype != Any:
                proto_dtype = get_datatype(dtype)
                df[column] = cast_col_to_pandas(df[column], proto_dtype)
            else:
                df[column] = df[column].fillna(pd.NA).replace({np.nan: pd.NA})
            if df[column].dtype == "object":
                df[column] = df[column].apply(lambda x: parse_json(dtype, x))
        return df


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

    role_arn = s3.role_arn()
    if access_key_id and secret_access_key and role_arn:
        raise Exception(
            "Both access key credentials and role arn should not be set"
        )

    s3_table: Dict[str, Any] = {}
    s3_table["bucket"] = s3.bucket_name
    s3_table["path_prefix"] = s3.path_prefix
    s3_table["pre_sorted"] = s3.presorted
    if isinstance(s3.format, CSV):
        s3_table["format"] = {"csv": {"delimiter": ord(s3.format.delimiter)}}
        if s3.format.headers is not None:
            s3_table["format"]["csv"]["headers"] = s3.format.headers
    else:
        s3_table["format"] = s3.format.lower()
    s3_table["db"] = {
        "name": "extract_historical_s3_input",
        "db": {"S3": {"creds": creds_json, "role_arn": s3.role_arn()}},
    }
    return s3_table


def _validate_branch_name(branch: str):
    """
    Branch name should only contain alphanumeric characters, hyphens or underscores or a period.
    :param branch:
    """
    pattern = r"^[a-zA-Z0-9_.\-/]+$"
    if not re.match(pattern, branch):
        raise ValueError(
            f"Branch name should only contain alphanumeric characters, hyphens, underscores or a period, found {branch}."
        )
