import ast
import functools
import inspect
import textwrap
from typing import *

import cloudpickle
import pandas as pd

import fennel.gen.feature_pb2 as feature_proto
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.lib.schema import Schema


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


# Takes the list of aggregates that this feature depends upon as dictionary of aggregate to aggregate name
# and the list of feature names that this feature depends upon.
def _modify_feature_extract(
        func, agg2name: Dict[str, str], feature2name: List[Any]
):
    if hasattr(func, "wrapped_function"):
        feature_func = func.wrapped_function
    else:
        feature_func = func
    function_source_code = textwrap.dedent(inspect.getsource(feature_func))
    tree = ast.parse(function_source_code)
    new_tree = FeatureExtractTransformer(agg2name, feature2name).visit(tree)
    decorators = []
    for decorator in new_tree.body[0].decorator_list:
        if isinstance(decorator, ast.Call) and decorator.func.id in (
                "feature",
                "feature_pack",
        ):
            decorators.append(
                ast.Call(decorator.func, decorator.args, decorator.keywords)
            )
    new_tree.body[0].decorator_list = decorators
    ast.fix_missing_locations(new_tree)
    ast.copy_location(new_tree.body[0], tree)
    code = compile(tree, inspect.getfile(feature_func), "exec")
    tmp_namespace = {}
    if hasattr(func, "namespace"):
        namespace = func.namespace
    else:
        namespace = func.__globals__
    if hasattr(func, "func_name"):
        func_name = func.func_name
    else:
        func_name = func.__name__
    for k, v in namespace.items():
        if k == func_name:
            continue
        tmp_namespace[k] = v
    exec(code, tmp_namespace)
    return tmp_namespace[func_name], function_source_code


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

        def mod_extract(*args, **kwargs) -> pd.DataFrame:
            return func(*args, **kwargs)

        setattr(ret, "mod_extract", mod_extract)

        # Directly called only for testing. Else the modded function feature_extract is called in the backend.
        def extract(*args, **kwargs) -> pd.DataFrame:
            agg2name = {
                agg.__name__: agg.name
                for agg in ret.depends_on_aggregates
            }
            feature2name = {
                f.func_def_name: f.name for f in ret.depends_on_features
            }
            mod_feature_func, _ = _modify_feature_extract(
                func, agg2name, feature2name
            )
            return mod_feature_func(*args, **kwargs)

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            agg2name = {
                agg.__name__: agg.name
                for agg in ret.depends_on_aggregates
            }
            feature2name = {
                f.func_def_name: f.name for f in ret.depends_on_features
            }
            mod_feature_func, function_source_code = _modify_feature_extract(
                func, agg2name, feature2name
            )

            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(mod_feature_func),
                function_source_code=function_source_code,
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
            agg2name = {
                agg.__name__: agg.name
                for agg in ret.depends_on_aggregates
            }
            new_function, function_source_code = _modify_feature_extract(
                func, agg2name, ret.depends_on_features
            )
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(new_function),
                function_source_code=function_source_code,
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
