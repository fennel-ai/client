# TODO(aditya): read hamilton post: https://multithreaded.stitchfix.com/blog/2021/10/14/functions-dags-hamilton/
from typing import *

import grpc
import pandas as pd

import fennel.gen.services_pb2_grpc as services_pb2
from fennel.stream import Stream
from fennel.aggregate import Aggregate
from fennel.utils import check_response


# TODO(aditya): how will auth work?
class Workspace:
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.channel = grpc.insecure_channel(url)
        self.stub = services_pb2.FennelFeatureStoreStub(self.channel)

    def register_streams(self, *streams: List[Stream]):
        exceptions = []
        for stream in streams:
            exceptions.extend(stream.validate())

        if len(exceptions) > 0:
            raise Exception(exceptions)

        for stream in streams:
            resp = stream.register(self.stub)
            check_response(resp)
        print("Registered streams:", [stream.name for stream in streams])

    def register_aggregates(self, *aggregates: List[Aggregate]):
        exceptions = []
        for agg in aggregates:
            exceptions.extend(agg.validate())

        if len(exceptions) > 0:
            raise Exception(exceptions)

        for agg in aggregates:
            resp = agg.register(self.stub)
            check_response(resp)
        print("Registered aggregates:", [agg.name for agg in aggregates])

    def register_features(self, *features):
        exceptions = []
        for feature in features:
            exceptions.extend(feature.validate())

        if len(exceptions) > 0:
            raise Exception(exceptions)

        for feature in features:
            resp = feature.register(self.stub)
            check_response(resp)

    # TODO(aditya/nikhil): figure out the right signature for global extract function
    # extract a single feature using these kwargs
    def extract(config: Dict[str, Any], reqs: Dict[str, Dict[str, pd.Series]], batch=False, concat=False):
        pass

    def extract_magic(config: Dict[str, Any], names: List[str], df: pd.DataFrame):
        pass


class Test:
    def __init__(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


"""
    Unit
        # - stream's transform function for a populator works -- no mocks
        # - preaggrgate works (df -> df) -- this needs mock for aggregates
        # - df -> aggregated data (talk to server, mock)
        - everything works in env specified
        # - feature computation works given input streams (mock of aggregates/features)
    
    Integration
        - e2e == read from stream, e2e
        - e2e == artificial stream data, e2e aggregate
        - e2e == read from stream, populate aggregates, read features
"""
