import ast
import datetime
import inspect
import textwrap
from typing import List

import cloudpickle

from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.stream_pb2 import CreateConnectorRequest, CreateStreamRequest
from fennel.lib import Schema


def _modify_stream(func):
    if hasattr(func, "populator_func"):
        populator_func = func.populator_func
    else:
        populator_func = func
    function_source_code = textwrap.dedent(inspect.getsource(populator_func))
    tree = ast.parse(function_source_code)
    # Remove the class argument
    tree.body[0].args = ast.arguments(
        args=tree.body[0].args.args[1:],
        posonlyargs=tree.body[0].args.posonlyargs,
        kwonlyargs=tree.body[0].args.kwonlyargs,
        kw_defaults=tree.body[0].args.kw_defaults,
        defaults=tree.body[0].args.defaults,
    )
    tree.body[0].decorator_list = []
    ast.fix_missing_locations(tree)
    code = compile(tree, inspect.getfile(populator_func), "exec")
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


class Stream:
    name = None
    version = 1
    retention = datetime.timedelta(days=30)
    # by default, get all the data from 1st of Jan 2020
    start = datetime.datetime(2020, 1, 1)
    schema: Schema = None

    @classmethod
    def validate(cls) -> List[Exception]:
        # Validate the schema
        exceptions = cls.schema.validate()
        # Validate the populators(source + connector) functions
        for name, func in inspect.getmembers(
                cls.__class__, predicate=inspect.isfunction
        ):
            if hasattr(func, "validate"):
                exceptions.extend(func.validate())
        return exceptions

    @classmethod
    def register(cls, stub: FennelFeatureStoreStub):
        # Register the connectors/populators one by one. Go through all
        # functions of the class and find the ones that can be registered (
        # sources)
        source_requests = []
        connector_requests = []
        for name, func in cls.__dict__.items():
            # Has a source attached to it
            if hasattr(func, "create_source_request"):
                source_requests.append(func.create_source_request())
                mod_func, function_source_code = _modify_stream(func)
                if hasattr(func, "func_name"):
                    func_name = func.func_name
                else:
                    func_name = func.__name__
                connector_requests.append(
                    CreateConnectorRequest(
                        name=func_name,
                        source_name=func.source.name,
                        source_type=func.source.type(),
                        connector_function=cloudpickle.dumps(mod_func),
                        populator_src_code=function_source_code,
                        table_name=func.table,
                    )
                )
        req = CreateStreamRequest(
            name=cls.name,
            version=cls.version,
            retention=int(cls.retention.total_seconds()),
            # Fix start time
            # start=self.start,
            schema=cls.schema.to_proto(),
            sources=source_requests,
            connectors=connector_requests,
        )
        resp = stub.RegisterStream(req)
        return resp
