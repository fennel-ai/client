import dataclasses
from typing import *
import functools

import pandas as pd
from fennel.lib.schema import Schema
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.aggregate import Aggregate
from fennel.gen.feature_pb2 import CreateFeatureRequest


def feature(
        name: str = None,
        version: int = 1,
        depends_on_aggregates: List[Aggregate] = None,
        depends_on_features: List[str] = None,
        mode: str = 'pandas',
        schema: Schema = None,
):
    def decorator(func):
        def ret(*args, **kwargs):
            raise Exception(
                "can not call feature directly"
            )

        ret.name = name
        ret.version = version
        ret.depends_on_aggregates = depends_on_aggregates
        ret.depends_on_features = depends_on_features
        ret.mode = mode
        ret.schema = schema

        def validate() -> List[Exception]:
            exceptions = schema.validate()
            return exceptions

        setattr(ret, "validate", validate)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            req = CreateFeatureRequest(
                name=name,
                version=version,
                depends_on_aggregates=[agg.name for agg in depends_on_aggregates],
                depends_on_features=depends_on_features,
                mode=mode,
                schema=schema.to_proto(),
            )
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator
