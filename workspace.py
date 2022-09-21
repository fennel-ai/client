#TODO(aditya): read hamilton post: https://multithreaded.stitchfix.com/blog/2021/10/14/functions-dags-hamilton/
from typing import *

import aggregate
import stream


# TODO(aditya): how will auth work?
class Workspace:
    def __init__(self, name: str, prod: bool, ttl: int = 0, auth: bool = False, token: str = None, py_version=None, env=None):
        self.name = name
        self.prod = prod
        self.ttl = 0
        self.auth = auth
        self.token = token
        self.streams = []
        self.aggregates = []
        self.features = []

    def add_stream(self, s: str):
        self.add_stream_many([s])

    def add_stream_many(self, streams: List[str]):
        self.streams.extend(streams)

    def add_aggregate_many(self, aggregates: List[str]):
        self.aggregates.extend(aggregates)

    def add_aggregate(self, a: str):
        self.add_aggregate_many([a])

    def sync(self):
        pass

    #TODO(aditya/nikhil): figure out the right signature for global extract function
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