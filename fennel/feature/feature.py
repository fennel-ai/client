import ast
import functools
import inspect
from typing import *

import pandas as pd

import fennel.gen.feature_pb2 as feature_proto
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.lib.schema import Schema
from fennel.utils import fennel_pickle


def aggregate_lookup(agg_name: str, **kwargs):
    raise Exception("Aggregate lookup incorrectly patched")


def feature_extract(feature_name, **kwargs):
    raise Exception("Feature extract incorrectly patched")


def _is_sign_args_and_kwargs(sign):
    return (
        len(sign.parameters) == 2
        and "args" in sign.parameters
        and "kwargs" in sign.parameters
    )


class FeatureExtractTransformer(ast.NodeTransformer):
    def __init__(self, agg2name: Dict[str, str], feature2name: Dict[str, str]):
        self.agg2name = agg2name
        self.feature2name = feature2name

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and node.func.attr == "lookup":
            if node.func.value.id not in self.agg2name:
                raise Exception(
                    f"aggregate {node.func.value.id} not included in feature definition"
                )

            agg_name = self.agg2name[node.func.value.id]
            return ast.Call(
                func=ast.Name("aggregate_lookup", ctx=node.func.ctx),
                args=[ast.Constant(agg_name)],
                keywords=node.keywords,
            )

        if isinstance(node.func, ast.Attribute) and node.func.attr == "extract":
            if node.func.value.id not in self.feature2name:
                raise Exception(
                    f"feature {node.func.value.id} not included in feature definition"
                )

            feature_name = self.feature2name[node.func.value.id]
            return ast.Call(
                func=ast.Name("feature_extract", ctx=node.func.ctx),
                args=[ast.Constant(feature_name)],
                keywords=node.keywords,
            )
        return node


def feature(
    name: str = None,
    version: int = 1,
    mode: str = "pandas",
    schema: Schema = None,
):
    def decorator(func):
        def ret(*args, **kwargs):
            return func(*args, **kwargs)

        ret.name = name
        if hasattr(func, "wrapped_function"):
            ret.func_def_name = func.wrapped_function.__name__
        else:
            ret.func_def_name = func.__name__
        ret.version = version
        if hasattr(func, "depends_on_aggregates"):
            ret.depends_on_aggregates = func.depends_on_aggregates
        else:
            ret.depends_on_aggregates = []
        if hasattr(func, "depends_on_features"):
            ret.depends_on_features = func.depends_on_features
        else:
            ret.depends_on_features = []
        ret.mode = mode
        ret.schema = schema

        def validate() -> List[Exception]:
            exceptions = schema.validate()
            sign = inspect.signature(func)
            if _is_sign_args_and_kwargs(sign):
                sign = func.signature
            for param in sign.parameters.values():
                if param.annotation != pd.Series and mode == "pandas":
                    exceptions.append(
                        f"parameter {param.name} is not a pandas.Series"
                    )
            if sign.return_annotation != pd.Series and mode == "pandas":
                exceptions.append(
                    "feature function must return a pandas.Series"
                )
            return exceptions

        setattr(ret, "validate", validate)

        # Directly called only for testing. Else the modded function feature_extract is called in the backend.
        def extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=fennel_pickle(func),
                function_source_code=inspect.getsource(func),
                type=feature_proto.FeatureDefType.FEATURE,
            )
            req.depends_on_features.extend(
                [f.name for f in ret.depends_on_features]
            )
            req.depends_on_aggregates.extend(
                [
                    str(agg.name)
                    for agg in ret.depends_on_aggregates
                    if agg is not None
                ]
            )
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator


def feature_pack(
    name: str = None,
    version: int = 1,
    mode: str = "pandas",
    schema: Schema = None,
):
    def decorator(func):
        def ret(*args, **kwargs):
            raise Exception("Feature pack should not be called directly")

        ret.name = name
        ret.version = version
        if hasattr(func, "depends_on_aggregates"):
            ret.depends_on_aggregates = func.depends_on_aggregates
        else:
            ret.depends_on_aggregates = []
        if hasattr(func, "depends_on_features"):
            ret.depends_on_features = func.depends_on_features
        else:
            ret.depends_on_features = []
        ret.mode = mode
        ret.schema = schema

        def validate() -> List[Exception]:
            exceptions = schema.validate()
            sign = inspect.signature(func)
            if _is_sign_args_and_kwargs(sign):
                sign = func.signature
            for param in sign.parameters.values():
                if param.annotation != pd.Series and mode == "pandas":
                    exceptions.append(
                        f"parameter {param.name} is not a pandas.Series"
                    )
            if sign.return_annotation != pd.DataFrame and mode == "pandas":
                exceptions.append(
                    "feature function must return a pandas.Series"
                )
            return exceptions

        setattr(ret, "validate", validate)

        def extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            print(
                "Going to dump func"
                "--------------------------------------------------------"
            )
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=fennel_pickle(func),
                function_source_code=inspect.getsource(func),
                type=feature_proto.FeatureDefType.FEATURE_PACK,
            )
            req.depends_on_features.extend(
                [f.name for f in ret.depends_on_features]
            )
            req.depends_on_aggregates.extend(
                [
                    str(agg.name)
                    for agg in ret.depends_on_aggregates
                    if agg is not None
                ]
            )
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator
