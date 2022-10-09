from typing import *

import grpc
import pandas as pd

import fennel.gen.feature_pb2 as feature_proto
import fennel.gen.services_pb2_grpc as services_pb2
from fennel.stream import Stream
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

    def register_aggregates(self, *aggregates: Any):
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

    def extract(
            self, config: Dict[str, Any], names: List[str], df: pd.DataFrame
    ):
        req = feature_proto.ExtractFeaturesRequest()
        req.names.extend(names)
        req.data.CopyFrom(df.to_dict(orient="tight"))
        req.config.CopyFrom(config)
        resp = self.stub.ExtractFeatures(req)
        check_response(resp)
        return resp
