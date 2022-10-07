import ast
import inspect
import textwrap
from typing import Any, Dict, List

from fennel.gen.status_pb2 import Status

_ENABLE_LOGGING_TRACE_IDS = "ENABLE_LOGGING_TRACE_ID"
_REXER_TRACER_KEY = "rexer-traceid"

API_VERSION = "/internal/v1"


class Singleton(object):
    """Use to create a singleton"""

    def __new__(cls, *args, **kwds):
        it_id = "__it__"
        # getattr will dip into base classes, so __dict__ must be used
        it = cls.__dict__.get(it_id, None)
        if it is not None:
            return it
        it = object.__new__(cls)
        setattr(cls, it_id, it)
        it.init(*args, **kwds)
        return it

    @classmethod
    def instance(cls):
        if not hasattr(cls, "__it__"):
            raise Exception(f'Singleton instance {cls.__name__} not initialized')
        return getattr(cls, "__it__")

    def init(self, *args, **kwds):
        pass


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


# Takes the list of aggregates that this aggregate depends upon as dictionary of aggregate to aggregate name.
def modify_aggregate_lookup(func, agg2name: Dict[str, str]):
    if hasattr(func, 'wrapped_function'):
        feature_func = func.wrapped_function
    else:
        feature_func = func
    function_source_code = textwrap.dedent(inspect.getsource(feature_func))
    tree = ast.parse(function_source_code)
    new_tree = AggLookupTransformer(agg2name).visit(tree)
    new_tree.body[0].decorator_list = []
    # Remove the class argument
    new_tree.body[0].args = ast.arguments(args=new_tree.body[0].args.args[1:],
                                          posonlyargs=new_tree.body[0].args.posonlyargs,
                                          kwonlyargs=new_tree.body[0].args.kwonlyargs,
                                          kw_defaults=new_tree.body[0].args.kw_defaults,
                                          defaults=new_tree.body[0].args.defaults)
    ast.fix_missing_locations(new_tree)
    ast.copy_location(new_tree.body[0], tree)
    code = compile(tree, inspect.getfile(feature_func), 'exec')
    tmp_namespace = {}
    if hasattr(func, 'namespace'):
        namespace = func.namespace
    else:
        namespace = func.__globals__
    if hasattr(func, 'func_name'):
        func_name = func.func_name
    else:
        func_name = func.__name__
    for k, v in namespace.items():
        if k == func_name:
            continue
        tmp_namespace[k] = v
    exec(code, tmp_namespace)
    return tmp_namespace[func_name], function_source_code


class FeatureExtractTransformer(ast.NodeTransformer):
    def __init__(self, agg2name: Dict[str, str], feature2name: Dict[str, str]):
        self.agg2name = agg2name
        self.feature2name = feature2name

    def visit_Call(self, node):
        if isinstance(node.func, ast.Attribute) and node.func.attr == 'lookup':
            if node.func.value.id not in self.agg2name:
                raise Exception(f"aggregate {node.func.value.id} not included in feature definition")

            agg_name = self.agg2name[node.func.value.id]
            return ast.Call(func=ast.Name('aggregate_lookup', ctx=node.func.ctx), args=[ast.Constant(agg_name)],
                            keywords=node.keywords)

        if isinstance(node.func, ast.Attribute) and node.func.attr == 'extract':
            if node.func.value.id not in self.feature2name:
                raise Exception(f"feature {node.func.value.id} not included in feature definition")

            feature_name = self.feature2name[node.func.value.id]
            return ast.Call(func=ast.Name('feature_extract', ctx=node.func.ctx), args=[ast.Constant(feature_name)],
                            keywords=node.keywords)
        return node


# Takes the list of aggregates that this feature depends upon as dictionary of aggregate to aggregate name
# and the list of feature names that this feature depends upon.
def modify_feature_extract(func, agg2name: Dict[str, str], feature2name: List[Any]):
    if hasattr(func, 'wrapped_function'):
        feature_func = func.wrapped_function
    else:
        feature_func = func
    function_source_code = textwrap.dedent(inspect.getsource(feature_func))
    tree = ast.parse(function_source_code)
    new_tree = FeatureExtractTransformer(agg2name, feature2name).visit(tree)
    decorators = []
    for decorator in new_tree.body[0].decorator_list:
        if isinstance(decorator, ast.Call) and decorator.func.id in ('feature', 'feature_pack'):
            decorators.append(ast.Call(decorator.func, decorator.args, decorator.keywords))
    new_tree.body[0].decorator_list = decorators
    ast.fix_missing_locations(new_tree)
    ast.copy_location(new_tree.body[0], tree)
    code = compile(tree, inspect.getfile(feature_func), 'exec')
    tmp_namespace = {}
    if hasattr(func, 'namespace'):
        namespace = func.namespace
    else:
        namespace = func.__globals__
    if hasattr(func, 'func_name'):
        func_name = func.func_name
    else:
        func_name = func.__name__
    for k, v in namespace.items():
        if k == func_name:
            continue
        tmp_namespace[k] = v
    exec(code, tmp_namespace)
    return tmp_namespace[func_name], function_source_code


def check_response(response: Status):
    """Check the response from the server and raise an exception if the response is not OK"""
    if response.code != 200:
        raise Exception(response.message)
