import ast
import inspect
import textwrap
from typing import Any, Dict, List, Optional

import cloudpickle
import pandas as pd

import fennel.gen.aggregate_pb2 as proto
from fennel.errors import NameException
from fennel.gen.aggregate_pb2 import CreateAggregateRequest
from fennel.gen.services_pb2_grpc import FennelFeatureStoreStub
from fennel.gen.status_pb2 import Status
from fennel.lib.schema import Field, FieldType, Schema


class AggLookupTransformer(ast.NodeTransformer):
    def __init__(cls, agg2name: Dict[str, str]):
        cls.agg2name = agg2name

    def visit_Call(cls, node):
        if isinstance(node.func, ast.Attribute) and node.func.attr == "lookup":
            if node.func.value.id not in cls.agg2name:
                raise Exception(
                    f"aggregate {node.func.value.id} not included in feature definition"
                )

            agg_name = cls.agg2name[node.func.value.id]
            return ast.Call(
                func=ast.Name("aggregate_lookup", ctx=node.func.ctx),
                args=[ast.Constant(agg_name)],
                keywords=node.keywords,
            )
        return node


# Takes the list of aggregates that this aggregate depends upon as dictionary of aggregate to aggregate name.
def _modify_aggregate_lookup(func, agg2name: Dict[str, str]):
    if hasattr(func, "wrapped_function"):
        feature_func = func.wrapped_function
    else:
        feature_func = func
    function_source_code = textwrap.dedent(inspect.getsource(feature_func))
    tree = ast.parse(function_source_code)
    new_tree = AggLookupTransformer(agg2name).visit(tree)
    new_tree.body[0].decorator_list = []
    # Remove the class argument
    new_tree.body[0].args = ast.arguments(
        args=new_tree.body[0].args.args[1:],
        posonlyargs=new_tree.body[0].args.posonlyargs,
        kwonlyargs=new_tree.body[0].args.kwonlyargs,
        kw_defaults=new_tree.body[0].args.kw_defaults,
        defaults=new_tree.body[0].args.defaults,
    )
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


class AggregateSchema(Schema):
    def __init__(self, fields: List[Field]):
        super().__init__(fields)

    def validate(self) -> List[Exception]:
        exceptions = self.validate_fields()
        key_fields = []
        value_fields = []
        timestamp_fields = []
        for field in self.fields:
            if field.field_type == FieldType.None_:
                exceptions.append(
                    f"field {field.name} does not specify valid type - key, value, or timestamp"
                )
            elif field.field_type == FieldType.Key:
                key_fields.append(field.name)
            elif field.field_type == FieldType.Value:
                value_fields.append(field.name)
            elif field.field_type == FieldType.Timestamp:
                timestamp_fields.append(field.name)
            else:
                exceptions.append(f"invalid field type: {field.type}")

        if len(key_fields) <= 0:
            exceptions.append("no field with type 'key' provided")
        if len(value_fields) <= 0:
            exceptions.append("no field with type 'value' provided")
        if len(timestamp_fields) != 1:
            exceptions.append(
                f"expected exactly one timestamp field but got: {len(timestamp_fields)} instead"
            )

        return exceptions


def notify(fn, schema):
    def fncomposite(df):
        # First parameter is the class instance which is not used.
        if hasattr(fn.__func__, "depends_on_aggregates"):
            depends_on_aggregates = fn.__func__.depends_on_aggregates
        else:
            depends_on_aggregates = []
        agg2names = {
            agg.__name__: agg.name
            for agg in depends_on_aggregates
        }
        mod_preaggregate, _ = _modify_aggregate_lookup(fn.__func__, agg2names)
        result = mod_preaggregate(df)
        field2types = schema.get_fields_and_types()
        # Shallow Type Check
        for col, dtype in zip(result.columns, result.dtypes):
            if col not in field2types:
                raise Exception("Column {} not in schema".format(col))
            if not field2types[col].type_check(dtype):
                raise Exception(
                    f"Column {col} type mismatch, got {dtype} expected {field2types[col]}"
                )
        # Deeper Type Check
        for (colname, colvals) in result.items():
            for val in colvals:
                type_errors = field2types[colname].validate(val)
                if type_errors:
                    raise Exception(
                        f"Column {colname} value {val} failed validation: {type_errors}"
                    )
        return result

    # Return the composite function
    return fncomposite


# Metaclass that creates aggregate class
class AggregateMetaclass(type):
    def __new__(cls, name, bases, attrs):
        preaggregate_fn = None
        schema = None
        for attr_name, func in attrs.items():
            if attr_name == "preaggregate":
                preaggregate_fn = func
            elif attr_name == "schema":
                schema = func
        if preaggregate_fn is not None:
            attrs["preaggregate"] = notify(preaggregate_fn, schema)
            attrs["og_preaggregate"] = preaggregate_fn
        return super(AggregateMetaclass, cls).__new__(cls, name, bases, attrs)


class Aggregate(metaclass=AggregateMetaclass):
    name: str = None
    version: int = 1
    stream: str = None
    mode: str = "pandas"
    schema: AggregateSchema = None

    @classmethod
    def preaggregate(cls, df: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

    @classmethod
    def register(cls, stub: FennelFeatureStoreStub) -> Status:
        if cls.windows is None:
            raise Exception("windows not provided")
        if hasattr(cls.og_preaggregate, "depends_on_aggregates"):
            depends_on_aggregates = cls.og_preaggregate.depends_on_aggregates
        else:
            depends_on_aggregates = []
        agg2names = {
            agg.__name__: agg.name
            for agg in depends_on_aggregates
        }
        mod_preaggregate, function_src_code = _modify_aggregate_lookup(
            cls.og_preaggregate, agg2names)

        req = CreateAggregateRequest(
            name=cls.name,
            version=cls.version,
            stream=cls.stream,
            mode=cls.mode,
            # aggregate_type=cls.get_agg_type(),
            preaggregate_function=cloudpickle.dumps(mod_preaggregate),
            function_source_code=function_src_code,
            windows=[int(w.total_seconds()) for w in cls.windows],
            schema=cls.schema.to_proto(),
        )
        resp = stub.RegisterAggregate(req)
        return resp

    @classmethod
    def get_agg_type(cls):
        raise NotImplementedError()

    @classmethod
    def _validate_preaggregate(cls) -> List[Exception]:
        exceptions = []
        found_preaggregate = False
        class_methods = {name: func.__func__ for name, func in
                         cls.__dict__.items() if hasattr(func, "__func__")}
        print(class_methods.keys())
        for name, func in class_methods.items():
            if name[0] != "_" and name != "og_preaggregate":
                exceptions.append(
                    TypeError(f"invalid method {name} found in aggregate "
                              f"class, only preaggregate is allowed")
                )
            if hasattr(func, "wrapped_function"):
                func = func.wrapped_function

            if func.__code__.co_argcount != 2:
                exceptions.append(
                    TypeError(
                        f"preaggregate function should take 2 arguments ( cls & df ) but got {func.__code__.co_argcount}"
                    )
                )
            found_preaggregate = True
        if not found_preaggregate:
            exceptions.append(
                Exception("preaggregate function not found in aggregate class")
            )
        return exceptions

    @classmethod
    def validate(cls) -> List[Exception]:
        # Validate the schema
        exceptions = cls.schema.validate_fields()
        if cls.name is None:
            exceptions.append(
                NameException(f"name not provided  {cls.__class__.__name__}")
            )
        if cls.stream is None:
            exceptions.append(
                Exception(
                    f"stream not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )

        # Validate the preaggregate function
        exceptions.extend(cls._validate_preaggregate())
        return exceptions


class Count(Aggregate):
    windows: List[int] = None

    @classmethod
    def get_agg_type(cls):
        return proto.AggregateType.COUNT

    @classmethod
    def validate(cls) -> List[Exception]:
        exceptions = super().validate()
        if cls.windows is None or len(cls.windows) == 0:
            exceptions.append(
                Exception(
                    f"windows not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )
        return exceptions


class Min(Aggregate):
    windows: List[int] = None

    @classmethod
    def _get_agg_type(cls):
        return proto.AggregateType.MIN

    @classmethod
    def validate(cls) -> List[Exception]:
        exceptions = super().validate()
        if cls.windows is None or len(cls.windows) == 0:
            exceptions.append(
                Exception(
                    f"windows not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )
        return exceptions


class Max(Aggregate):
    windows: List[int] = None

    @classmethod
    def _get_agg_type(cls):
        return proto.AggregateType.MAX

    @classmethod
    def validate(cls) -> List[Exception]:
        exceptions = super().validate()
        if cls.windows is None or len(cls.windows) == 0:
            exceptions.append(
                Exception(
                    f"windows not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )
        return exceptions


class KeyValue(Aggregate):
    static = False

    @classmethod
    def _get_agg_type(cls):
        return proto.AggregateType.KEY_VALUE

    @classmethod
    def validate(cls) -> List[Exception]:
        return super().validate()


class Rate(Aggregate):
    windows: List[int] = None

    @classmethod
    def _get_agg_type(cls):
        return proto.AggregateType.RATE

    @classmethod
    def validate(cls) -> List[Exception]:
        exceptions = super().validate()
        if cls.windows is None or len(cls.windows) == 0:
            exceptions.append(
                Exception(
                    f"windows not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )
        return exceptions


class Average(Aggregate):
    windows: List[int] = None

    @classmethod
    def _get_agg_type(cls):
        return proto.AggregateType.AVG

    @classmethod
    def validate(cls) -> List[Exception]:
        exceptions = super().validate()
        if cls.windows is None or len(cls.windows) == 0:
            exceptions.append(
                Exception(
                    f"windows not provided for aggregate  "
                    f"{cls.__class__.__name__}"
                )
            )
        return exceptions


def depends_on(
        aggregates: Optional[List[Any]] = None,
        features: List[Any] = None,
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        if aggregates is not None:
            wrapper.depends_on_aggregates = aggregates
        else:
            wrapper.depends_on_aggregates = []

        if features is not None:
            wrapper.depends_on_features = features
        else:
            wrapper.depends_on_features = []
        wrapper.signature = inspect.signature(func)
        wrapper.wrapped_function = func
        wrapper.namespace = func.__globals__
        wrapper.func_name = func.__name__
        return wrapper

    return decorator
