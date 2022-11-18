import json
from collections import defaultdict
from concurrent import futures
from functools import partial
from typing import Dict, List

import grpc
import pandas as pd
import pyarrow as pa
from requests import Response

import fennel.datasets.datasets
from fennel.client import Client
from fennel.datasets import Dataset, Pipeline
from fennel.featuresets import Featureset
from fennel.gen.dataset_pb2 import CreateDatasetRequest
from fennel.gen.services_pb2_grpc import (
    add_FennelFeatureStoreServicer_to_server,
    FennelFeatureStoreStub,
    FennelFeatureStoreServicer,
)
from fennel.test_lib.executor import Executor

TEST_PORT = 50051
TEST_DATA_PORT = 50052


class FakeResponse(Response):
    def __init__(self, status_code: int, content: str):
        self.status_code = status_code
        if status_code == 200:
            self._ok = True
            return
        self._content = json.dumps({"error": f"{content}"}, indent=2).encode(
            "utf-8"
        )
        self.encoding = "utf-8"


def lookup_fn(
        data: Dict[str, pd.DataFrame],
        datasets: Dict[str, Dataset],
        cls_name: str,
        ts: pa.Array,
        properties: List[str],
        keys: pa.RecordBatch,
):
    key_df = keys.to_pandas()
    print(data[cls_name])
    if cls_name not in data:
        timestamp_col = datasets[cls_name].timestamp_field
        # Create a dataframe with all nulls
        val_cols = [x.name for x in datasets[cls_name].fields]
        if len(properties) > 0:
            val_cols = [
                x for x in val_cols if x in properties or x in key_df.columns
            ]
        val_cols.remove(timestamp_col)
        empty_df = pd.DataFrame(
            columns=val_cols, data=[[None] * len(val_cols)] * len(key_df)
        )
        return pa.RecordBatch.from_pandas(empty_df)

    right_df = data[cls_name]
    timestamp_field = datasets[cls_name].timestamp_field
    join_columns = key_df.columns.tolist()
    key_df[timestamp_field] = ts.to_pandas()
    # Sort the keys by timestamp
    key_df = key_df.sort_values(timestamp_field)
    df = pd.merge_asof(
        left=key_df,
        right=right_df,
        on=timestamp_field,
        by=join_columns,
        direction="backward",
    )
    # drop the timestamp column
    df = df.drop(columns=[timestamp_field])
    if len(properties) > 0:
        df = df[properties]
    return pa.RecordBatch.from_pandas(df)


class MockClient(Client):
    def __init__(self, stub):
        super().__init__(url=f"localhost:{TEST_PORT}")
        self.stub = stub

        self.dataset_requests: Dict[str, CreateDatasetRequest] = {}
        self.datasets: Dict[str, Dataset] = {}
        # Map of dataset name to the dataframe
        self.data: Dict[str, pd.DataFrame] = {}
        # Map of datasets to pipelines it is an input to
        self.listeners: Dict[str, List[Pipeline]] = defaultdict(list)
        fennel.datasets.datasets.dataset_lookup = partial(
            lookup_fn, self.data, self.datasets
        )

    def _merge_df(self, df: pd.DataFrame, dataset_name: str):
        # Filter the dataframe to only include the columns in the schema
        columns = [x.name for x in self.datasets[dataset_name].fields]
        input_columns = df.columns.tolist()
        # Check that input columns are a subset of the dataset columns
        if not set(columns).issubset(set(input_columns)):
            raise ValueError(
                f"Dataset columns {columns} are not a subset of "
                f"Input columns {input_columns}"
            )
        df = df[columns]
        if dataset_name not in self.data:
            self.data[dataset_name] = df
        else:
            self.data[dataset_name] = pd.concat([self.data[dataset_name], df])
        # Sort by timestamp
        timestamp_field = self.datasets[dataset_name].timestamp_field
        self.data[dataset_name] = self.data[dataset_name].sort_values(
            timestamp_field
        )

    def log(self, dataset_name: str, df: pd.DataFrame, direct=True):
        print(f"Logging {dataset_name}")
        if dataset_name not in self.dataset_requests and direct:
            return FakeResponse(404, f"Dataset {dataset_name} not found")
        dataset = self.dataset_requests[dataset_name]
        with pa.ipc.open_stream(dataset.schema) as reader:
            dataset_schema = reader.schema
            print(f"Logging {dataset_name}")
            try:
                pa.RecordBatch.from_pandas(df, schema=dataset_schema)
            except Exception as e:
                content = (
                    f"Dataframe does not match schema for dataset "
                    f"{dataset_name}: {str(e)}"
                )
                return FakeResponse(status_code=400, content=content)

            print("Logging data", dataset_name)
            print(df)
            self._merge_df(df, dataset_name)

            for pipeline in self.listeners[dataset_name]:
                print(
                    "Executing pipeline", pipeline.name, pipeline.dataset_name
                )
                executor = Executor(self.data)
                ret = executor.execute(pipeline)
                if ret is None:
                    continue
                print("Pipeline", pipeline.name, "dumps", pipeline.dataset_name)
                print(ret.df)
                print("*" * 80)
                # Recursively log the output of the pipeline to the datasets
                resp = self.log(pipeline.dataset_name, ret.df, direct=False)
                if resp.status_code != 200:
                    return resp
        return FakeResponse(200, "OK")

    def sync(
            self, datasets: List[Dataset] = [],
            featuresets: List[Featureset] = []
    ):
        # TODO: Test for cycles in the graph

        for dataset in datasets:
            self.dataset_requests[
                dataset.name
            ] = dataset.create_dataset_request_proto()
            self.datasets[dataset.name] = dataset
            for pipeline in dataset._pipelines:
                for input in pipeline.inputs:
                    self.listeners[input.name].append(pipeline)


class Servicer(FennelFeatureStoreServicer):
    def Sync(self, request, context):
        pass


def mock_client(test_func):
    def _start_a_test_server():
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_FennelFeatureStoreServicer_to_server(Servicer(), server)
        server.add_insecure_port(f"[::]:{TEST_PORT}")
        server.start()
        return server

    def wrapper(*args, **kwargs):
        server = _start_a_test_server()
        with grpc.insecure_channel(f"localhost:{TEST_PORT}") as channel:
            stub = FennelFeatureStoreStub(channel)
            client = MockClient(stub)
            f = test_func(*args, **kwargs, client=client)
            server.stop(0)
            return f

    return wrapper
