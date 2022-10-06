import ast
import functools
from typing import *
import inspect
import cloudpickle
import pandas as pd
import pickle
import fennel.gen.feature_pb2 as feature_proto
from fennel.aggregate import Aggregate
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.lib.schema import Schema
import fennel.aggregate
from fennel.utils import modify_aggregate_lookup


def aggregate_lookup(agg_name: str, **kwargs):
    print("Aggregate", agg_name, " lookup patched")
    return pd.Series([8, 12, 13]), pd.Series([21, 22, 23])


class AggLookupTransformer(ast.NodeTransformer):
    def __init__(self, agg2name: Dict[str, str]):
        self.agg2name = agg2name

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'lookup':
            if node.func.value.id not in self.agg2name:
                raise Exception(f"aggregate {node.func.value.id} not included in feature definition")

            agg_name = self.agg2name[node.func.value.id]
            return ast.Call(func=ast.Name('aggregate_lookup', ctx=node.func.ctx), args=[ast.Constant(agg_name)],
                            keywords=node.keywords)
        return node


def is_sign_args_and_kwargs(sign):
    return len(sign.parameters) == 2 and 'args' in sign.parameters and 'kwargs' in sign.parameters


def feature(
        name: str = None,
        version: int = 1,
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
        if hasattr(func, 'depends_on_aggregates'):
            ret.depends_on_aggregates = func.depends_on_aggregates
        else:
            ret.depends_on_aggregates = []
        if hasattr(func, 'depends_on_features'):
            ret.depends_on_features = func.depends_on_features
        else:
            ret.depends_on_features = []
        ret.mode = mode
        ret.schema = schema
        # The original code written by the user
        ret.org_func = func

        def validate() -> List[Exception]:
            exceptions = schema.validate()
            sign = inspect.signature(func)
            if is_sign_args_and_kwargs(sign):
                sign = func.signature
            for param in sign.parameters.values():
                if param.annotation != pd.Series and mode == 'pandas':
                    exceptions.append(f'parameter {param.name} is not a pandas.Series')
            if sign.return_annotation != pd.Series and mode == 'pandas':
                exceptions.append(f"feature function must return a pandas.Series")
            return exceptions

        setattr(ret, "validate", validate)

        def extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            agg2name = {agg.instance().__class__.__name__: agg.instance().name for agg in ret.depends_on_aggregates}
            new_function, function_source_code = modify_aggregate_lookup(func, agg2name)
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(new_function),
                function_source_code=function_source_code,
                type=feature_proto.FeatureDefType.FEATURE,
            )
            req.depends_on_features.extend([f.name for f in ret.depends_on_features])
            req.depends_on_aggregates.extend([str(agg.name) for agg in ret.depends_on_aggregates if agg is not None])
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator


def feature_pack(
        name: str = None,
        version: int = 1,
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
        if hasattr(func, 'depends_on_aggregates'):
            ret.depends_on_aggregates = func.depends_on_aggregates
        else:
            ret.depends_on_aggregates = []
        if hasattr(func, 'depends_on_features'):
            ret.depends_on_features = func.depends_on_features
        else:
            ret.depends_on_features = []
        ret.mode = mode
        ret.schema = schema

        def validate() -> List[Exception]:
            exceptions = schema.validate()
            sign = inspect.signature(func)
            if is_sign_args_and_kwargs(sign):
                sign = func.signature
            for param in sign.parameters.values():
                if param.annotation != pd.Series and mode == 'pandas':
                    exceptions.append(f'parameter {param.name} is not a pandas.Series')
            if sign.return_annotation != pd.DataFrame and mode == 'pandas':
                exceptions.append(f"feature function must return a pandas.Series")
            return exceptions

        setattr(ret, "validate", validate)

        def compute() -> pd.DataFrame:
            pass

        setattr(ret, "compute", compute)

        def extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            agg2name = {agg.instance().__class__.__name__: agg.instance().name for agg in ret.depends_on_aggregates}
            new_function, function_source_code = modify_aggregate_lookup(func, agg2name)
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(new_function),
                function_source_code=function_source_code,
                type=feature_proto.FeatureDefType.FEATURE_PACK,
            )
            req.depends_on_features.extend([f.name for f in ret.depends_on_features])
            req.depends_on_aggregates.extend([str(agg.name) for agg in ret.depends_on_aggregates if agg is not None])
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator
