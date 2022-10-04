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


class AggLookupTransformer(ast.NodeTransformer):
    def __init__(self, agg2name: Dict[str, str]):
        self.agg2name = agg2name

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'lookup':
            if node.func.value.id not in self.agg2name:
                return node

            agg_name = self.agg2name[node.func.value.id]
            return ast.Call(func=ast.Name('aggregate_lookup', ctx=node.func.ctx), args=[ast.Constant(agg_name)],
                            keywords=node.keywords)
        return node


def feature(
        name: str = None,
        version: int = 1,
        depends_on_aggregates: List[Aggregate] = [],
        depends_on_features: List[Any] = [],
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
        # The original code written by the user
        ret.org_func = func

        def validate() -> List[Exception]:
            exceptions = schema.validate()
            sign = inspect.signature(func)
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
            function_source_code = inspect.getsource(func)
            tree = ast.parse(function_source_code)
            agg2name = {agg.instance().__class__.__name__: agg.instance().name for agg in depends_on_aggregates}
            # print(ast.dump(tree, indent=4))
            new_tree = AggLookupTransformer(agg2name).visit(tree)
            ast.fix_missing_locations(new_tree)
            ast.copy_location(new_tree.body[0], tree)
            # print("=====================================")
            # print(ast.dump(new_tree, indent=4))
            code = compile(tree, inspect.getfile(func), 'exec')
            tmp_namespace = {}
            namespace = func.__globals__
            for k, v in namespace.items():
                if k == func.__name__:
                    continue
                tmp_namespace[k] = v
            exec(code, tmp_namespace)
            new_function = tmp_namespace[func.__name__]
            functools.update_wrapper(new_function, func)
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(new_function),
                function_source_code=function_source_code,
                type=feature_proto.FeatureDefType.FEATURE,
            )
            req.depends_on_features.extend([f.name for f in depends_on_features])
            req.depends_on_aggregates.extend([str(agg.name) for agg in depends_on_aggregates if agg is not None])
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator


def feature_pack(
        name: str = None,
        version: int = 1,
        depends_on_aggregates: List[Aggregate] = [],
        depends_on_features: List[Any] = [],
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
            sign = inspect.signature(func)
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

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            p = ast.parse(func)
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(func),
                type=feature_proto.FeatureDefType.FEATURE_PACK,
            )
            req.depends_on_features.extend([f.name for f in depends_on_features])
            req.depends_on_aggregates.extend([agg.name for agg in depends_on_aggregates])
            return stub.RegisterFeature(req)

        setattr(ret, "register", register)

        return ret

    return decorator
