from fennel.gen.status_pb2 import Status
import ast
from typing import Any, Dict, List
import inspect
import functools
import textwrap

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
            raise Exception("Singleton instance not initialized")
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


def modify_aggregate_lookup(func, agg2name: Dict[str, str], caller_type: str = 'feature'):
    if hasattr(func, 'wrapped_function'):
        feature_func = func.wrapped_function
    else:
        feature_func = func
    function_source_code = textwrap.dedent(inspect.getsource(feature_func))
    tree = ast.parse(function_source_code)
    print(ast.dump(tree, indent=4))
    print("-----------------")
    new_tree = AggLookupTransformer(agg2name).visit(tree)
    decorators = []
    for decorator in new_tree.body[0].decorator_list:
        if isinstance(decorator, ast.Call) and decorator.func.id in ('feature', 'feature_pack'):
            decorators.append(ast.Call(decorator.func, decorator.args, decorator.keywords))
    new_tree.body[0].decorator_list = decorators
    ast.fix_missing_locations(new_tree)
    print(ast.dump(new_tree, indent=4))
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
