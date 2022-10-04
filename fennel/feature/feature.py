import ast
import functools
from typing import *
import inspect
import cloudpickle
import pandas as pd
import fennel.gen.feature_pb2 as feature_proto
from fennel.aggregate import Aggregate, aggregate_lookup
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.lib.schema import Schema


class AggNameRetrievalVisitor(ast.NodeVisitor):
    def __init__(self):
        self.agg_name = None
        self.agg2name = {}

    def visit_Attribute(self, node):
        self.agg_name = node.attr
        self.agg2name[node.value.id] = node.attr
        self.generic_visit(node)


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

        def extract() -> pd.DataFrame:
            pass

        setattr(ret, "extract", extract)

        @functools.wraps(func)
        def register(stub: FennelFeatureStoreStub):
            function_source_code = inspect.getsource(func)
            tree = ast.parse(function_source_code)
            # print(ast.dump(tree, indent=4))
            print(depends_on_aggregates)
            agg_names = {agg.__class__.__name__: str(agg.name) for agg in depends_on_aggregates if agg is not None}
            print("$", agg_names)
            # vis = AggNameRetrievalVisitor()
            # new_tree = vis.visit(tree)
            # ast.fix_missing_locations(new_tree)
            # print(new_tree)
            # print(vis.agg2name)
            req = feature_proto.CreateFeatureRequest(
                name=name,
                version=version,
                mode=mode,
                schema=schema.to_proto(),
                function=cloudpickle.dumps(func),
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
            print(p)
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
