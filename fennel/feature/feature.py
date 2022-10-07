import functools
import inspect
from typing import *

import cloudpickle
import pandas as pd

import fennel.gen.feature_pb2 as feature_proto
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.lib.schema import Schema
from fennel.utils import modify_feature_extract


def aggregate_lookup(agg_name: str, **kwargs):
    raise Exception("Aggregate lookup incorrectly patched")


def feature_extract(feature_name, **kwargs):
    raise Exception("Feature extract incorrectly patched")


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
            return func(*args, **kwargs)

        ret.name = name
        if hasattr(func, 'wrapped_function'):
            ret.func_def_name = func.wrapped_function.__name__
        else:
            ret.func_def_name = func.__name__
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
            if sign.return_annotation != pd.Series and mode == 'pandas':
                exceptions.append(f"feature function must return a pandas.Series")
            return exceptions

        setattr(ret, "validate", validate)

        def mod_extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "mod_extract", mod_extract)

        def extract(*args, **kwargs) -> pd.DataFrame:
            agg2name = {agg.instance().__class__.__name__: agg.instance().name for agg in ret.depends_on_aggregates}
            feature2name = {f.func_def_name: f.name for f in ret.depends_on_features}
            mod_feature_func, _ = modify_feature_extract(func, agg2name, feature2name)
            return mod_feature_func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            agg2name = {agg.instance().__class__.__name__: agg.instance().name for agg in ret.depends_on_aggregates}
            feature2name = {f.func_def_name: f.name for f in ret.depends_on_features}
            mod_feature_func, function_source_code = modify_feature_extract(func, agg2name, feature2name)

            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(mod_feature_func),
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
            raise Exception("Feature pack should not be called directly")

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

        def extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            agg2name = {agg.instance().__class__.__name__: agg.instance().name for agg in ret.depends_on_aggregates}
            new_function, function_source_code = modify_feature_extract(func, agg2name, ret.depends_on_features)
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
