from __future__ import annotations

import datetime
import inspect
from typing import (Callable, Dict, List, Optional, Tuple, Type,
                    TypeVar, Union)

import cloudpickle
import pyarrow

import fennel.gen.dataset_pb2 as proto
from fennel.lib.aggregate import AggregateType
from fennel.lib.duration.duration import Duration, duration_to_timedelta, \
    timedelta_to_micros
from fennel.lib.field import Field
from fennel.lib.schema import get_pyarrow_field
from fennel.utils import fhash, parse_annotation_comments

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar('T')

DEFAULT_RETENTION = Duration('2y')
DEFAULT_MAX_STALENESS = Duration('30d')


# ---------------------------------------------------------------------
# Node
# ---------------------------------------------------------------------

class _Node:

    def __init__(self):
        self.out_edges = []

    def transform(self, func: Callable,
                  timestamp: Optional[str] = None) -> _Node:
        return _Transform(self, func, timestamp)

    def groupby(self, *args) -> _GroupBy:
        return _GroupBy(self, *args)

    def join(self, other: Dataset, on: Optional[List[str]] = None, left_on:
    Optional[List[str]] = None, right_on: Optional[List[str]] = None) -> _Join:
        if not isinstance(other, Dataset) and isinstance(other, _Node):
            raise ValueError(f'Cannot join with an intermediate dataset')
        if not isinstance(other, _Node):
            raise TypeError('Cannot join with a non-dataset object')
        return _Join(self, other, on, left_on, right_on)

    def __add__(self, other):
        return _Union(self, other)

    def signature(self):
        raise NotImplementedError

    def num_out_edges(self) -> int:
        return len(self.out_edges)


class _Transform(_Node):
    def __init__(self, node: _Node, func: Callable, timestamp: Optional[str]):
        super().__init__()
        self.func = func
        self.node = node
        self.timestamp_field = timestamp
        self.node.out_edges.append(self)

    def signature(self):
        return fhash(self.node.signature(), self.func, self.timestamp_field)


class _Aggregate(_Node):
    def __init__(self, node: _Node, keys: List[str],
                 aggregates: List[AggregateType]):
        super().__init__()
        if len(keys) == 0:
            raise ValueError('Must specify at least one key')
        self.keys = keys
        self.aggregates = aggregates
        self.node = node
        self.node.out_edges.append(self)

    def signature(self):
        agg_signature = fhash([agg.signature() for agg in self.aggregates])
        return fhash(self.node.signature(), self.keys, agg_signature)


class _GroupBy:
    def __init__(self, node: _Node, *args):
        super().__init__()
        self.keys = args
        self.node = node
        self.node.out_edges.append(self)

    def aggregate(self, aggregates: List[AggregateType]) -> _Node:
        return _Aggregate(self.node, self.keys, aggregates)


class _Join(_Node):
    def __init__(self, node: _Node, dataset: Dataset, on: Optional[List[
        str]] = None, left_on: Optional[List[str]] = None, right_on:
    Optional[List[str]] = None):
        super().__init__()
        self.node = node
        self.dataset = dataset
        self.on = on
        self.left_on = left_on
        self.right_on = right_on
        self.node.out_edges.append(self)
        if on is not None:
            if left_on is not None or right_on is not None:
                raise ValueError('Cannot specify on and left_on/right_on')
            if not isinstance(on, list):
                raise ValueError('on must be a list of keys')

        if on is None:
            if not isinstance(left_on, list) or not isinstance(right_on, list):
                raise ValueError(
                    'Must specify left_on and right_on as a list of keys')
            if left_on is None or right_on is None:
                raise ValueError('Must specify on or left_on/right_on')

    def signature(self):
        return fhash(self.node.signature(), self.dataset.name,
            self.on, self.left_on, self.right_on)


class _Union(_Node):
    def __init__(self, node: _Node, other: _Node):
        super().__init__()
        self.nodes = [node, other]
        node.out_edges.append(self)
        other.out_edges.append(self)

    def signature(self):
        return fhash([n.signature() for n in self.nodes])


# ---------------------------------------------------------------------
# dataset & pipeline decorators
# ---------------------------------------------------------------------


class dataset:
    """
    dataset is a decorator that creates a Dataset class.
    A dataset class contains the schema of the dataset, an optional pull
    function, and a set of pipelines that declare how to generate data for the
    dataset from other datasets.
    Parameters
    ----------
    retention : Duration ( Optional )
        The amount of time to keep data in the dataset.
    max_staleness : Duration ( Optional )
        The maximum amount of time that data in the dataset can be stale.
    """

    def __new__(cls, dataset_cls: Optional[Type[T]] = None, *args, **kwargs):
        """This is called when dataset is called without parens."""
        if dataset_cls is None:
            # We're called as @dataset(*args, *kwargs).
            return super().__new__(cls)
        # We're called as @dataset without parens.
        return dataset._create_dataset(dataset_cls, *args, **kwargs)

    def __init__(self, retention: Duration = DEFAULT_RETENTION,
                 max_staleness: Duration = DEFAULT_MAX_STALENESS):
        """
        This is called to instantiate a dataset that is called with
        parens.
        """
        self._retention = retention
        self._max_staleness = max_staleness

    def __call__(self, dataset_cls: Optional[Type[T]]):
        """This is called when dataset is called with parens."""
        return dataset._create_dataset(dataset_cls, self._retention,
            self._max_staleness)

    @staticmethod
    def _create_dataset(cls: Type[T], retention: Duration = DEFAULT_RETENTION,
                        max_staleness: Duration = DEFAULT_MAX_STALENESS) -> T:
        cls_annotations = cls.__dict__.get('__annotations__', {})

        fields = [
            dataset._get_field(
                cls=cls,
                annotation_name=name,
                dtype=cls_annotations[name],
                field2comment_map=parse_annotation_comments(cls),
            )
            for name in cls_annotations
        ]

        pull_fn = getattr(cls, 'pull', None)
        return Dataset(
            cls,
            fields,
            retention=duration_to_timedelta(retention),
            max_staleness=duration_to_timedelta(max_staleness),
            pull_fn=pull_fn,
        )

    @staticmethod
    def _get_field(
            cls: Type,
            annotation_name: str,
            dtype: Type,
            field2comment_map: Dict[str, str],
    ):
        field = getattr(cls, annotation_name, None)
        if isinstance(field, Field):
            field.name = annotation_name
            field.dtype = dtype
        else:
            field = Field(name=annotation_name, key=False, timestamp=False,
                owner=None, description='', pa_field=None, dtype=dtype)

        field.description = field2comment_map.get(annotation_name, '')
        field.pa_field = get_pyarrow_field(annotation_name, dtype)
        return field


def pipeline(pipeline_func: Callable):
    if not callable(pipeline_func):
        raise Exception('pipeline_func must be callable')
    sig = inspect.signature(pipeline_func)
    params = []
    for name, param in sig.parameters.items():
        if param.name == 'self':
            raise TypeError('pipeline_func cannot have self as a parameter '
                            'and should be a static method')
        if not isinstance(param.annotation, Dataset):
            raise TypeError(f'Parameter {name} is not a Dataset')
        params.append(param.annotation)
    pipeline = Pipeline(node=pipeline_func(*params), inputs=params)
    pipeline_func.__fennel_pipeline__ = pipeline
    return pipeline_func


# ---------------------------------------------------------------------
# Dataset & Pipeline
# ---------------------------------------------------------------------

class Pipeline:
    node: _Node
    inputs: List['Dataset']
    _sign: str
    _proto: proto.Pipeline

    def __init__(self, node: _Node, inputs: List[Dataset]):
        self.node = node
        self.inputs = inputs
        serializer = Serializer()
        self._proto = serializer.serialize(self)
        self._sign = self._proto.signature

    def signature(self):
        return self._sign

    def to_proto(self) -> proto.Pipeline:
        return self._proto


class Dataset(_Node):
    """Dataset is a collection of data."""
    name: str
    pull_fn: Optional[Callable]
    signature: str
    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    _retention: datetime.timedelta
    _fields: List[Field]
    _pipelines: List[Pipeline]
    _key_field: str
    _timestamp_field: str
    _max_staleness: Optional[datetime.timedelta]
    _owner: Optional[str]
    _description: Optional[str]
    __fennel_original_cls__: Type[T]

    def __init__(
            self,
            cls: Type,
            fields: List[Field],
            retention: Optional[datetime.timedelta] = None,
            max_staleness: Optional[datetime.timedelta] = None,
            pull_fn: Optional[Callable] = None,
    ):
        super().__init__()
        self.name = cls.__name__
        self.pull_fn = pull_fn
        self.__name__ = cls.__name__
        self._fields = fields
        self._set_timestamp_field()
        self._retention = retention
        self._max_staleness = max_staleness
        self.__fennel_original_cls__ = cls
        self._pipelines = self._get_pipelines()
        self._sign = self._create_signature()

    def __getattr__(self, key):
        if key in self.__fennel_original_cls__.__dict__['__annotations__']:
            return str(key)
        return super().__getattribute__(key)

    def lookup(self, key):
        raise NotImplementedError

    def signature(self):
        return self._sign

    def _create_signature(self):
        return fhash(
            self.name, self._retention, self._max_staleness, self.pull_fn,
            [f.signature() for f in self._fields], [p.signature for p in
                                                    self._pipelines])

    def _set_timestamp_field(self):
        timestamp_field_set = False
        for field in self._fields:
            if field.timestamp:
                if timestamp_field_set:
                    raise ValueError(
                        'Multiple timestamp fields are not supported.')
                timestamp_field_set = True

        if timestamp_field_set:
            return

        # Find a field that has datetime type and set it as timestamp.

        for field in self._fields:
            if field.dtype != datetime.datetime:
                continue

            if not timestamp_field_set:
                field.timestamp = True
                timestamp_field_set = True
            else:
                raise ValueError(
                    'Multiple timestamp fields are not supported.')
        if not timestamp_field_set:
            raise ValueError('No timestamp field found.')

    def _get_schema(self) -> bytes:
        schema = pyarrow.schema(
            [field.pa_field for field in self._fields if field.pa_field])
        return schema.serialize().to_pybytes()

    def __repr__(self):
        return f'Dataset({self.__name__}, {self._fields})'

    def _pull_to_proto(self) -> proto.PullLookup:
        return proto.PullLookup(
            function_source_code=inspect.getsource(self.pull_fn),
            function=cloudpickle.dumps(self.pull_fn),
        )

    def _get_pipelines(self) -> List[Pipeline]:
        pipelines = []
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if name == 'pull':
                continue
            if not hasattr(method, '__fennel_pipeline__'):
                continue
            pipeline = method.__fennel_pipeline__
            pipelines.append(pipeline)
        return pipelines

    def create_dataset_request_proto(self) -> proto.CreateDatasetRequest:
        return proto.CreateDatasetRequest(
            name=self.__name__,
            schema=self._get_schema(),
            retention=timedelta_to_micros(self._retention),
            max_staleness=timedelta_to_micros(self._max_staleness),
            pipelines=[p.to_proto() for p in self._pipelines],
            mode='pandas',
            # TODO: Parse description from docstring.
            description='',
            # Currently we don't support versioning of datasets.
            # Kept for future use.
            version=0,
            fields=[field.to_proto() for field in self._fields],
            pull_lookup=self._pull_to_proto() if self.pull_fn else None,
        )


# ---------------------------------------------------------------------
# Visitor
# ---------------------------------------------------------------------


class Visitor:
    def visit(self, obj):
        if isinstance(obj, Dataset):
            return self.visitDataset(obj)
        elif isinstance(obj, _Transform):
            return self.visitTransform(obj)
        elif isinstance(obj, _GroupBy):
            return self.visitGroupBy(obj)
        elif isinstance(obj, _Aggregate):
            return self.visitAggregate(obj)
        elif isinstance(obj, _Join):
            return self.visitJoin(obj)
        elif isinstance(obj, _Union):
            return self.visitUnion(obj)
        else:
            raise Exception('invalid node type: %s' % obj)

    def visitDataset(self, obj):
        raise NotImplementedError()

    def visitTransform(self, obj):
        raise NotImplementedError()

    def visitGroupBy(self, obj):
        raise Exception(f'group by object {obj} must be aggregated')

    def visitAggregate(self, obj):
        return NotImplementedError()

    def visitJoin(self, obj):
        raise NotImplementedError()

    def visitUnion(self, obj):
        raise NotImplementedError()


class Printer(Visitor):
    def __init__(self):
        super(Printer, self).__init__()
        self.lines = []
        self.offset = 0

    def indent(self):
        return ' ' * self.offset

    def visit(self, obj):
        ret = super(Printer, self).visit(obj)
        return ret

    def print(self, obj):
        last = self.visit(obj)
        self.lines.append(last)
        print('\n'.join(self.lines))

    def visitDataset(self, obj):
        return f'{self.indent()}{obj.name}\n'

    def visitTransform(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        self.offset -= 1
        return f'{self.indent()}Transform(\n{pipe_str}{self.indent()})\n'

    def visitAggregate(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        self.offset -= 1
        return f'{self.indent()}Aggregate(\n{pipe_str}{self.indent()})\n'

    def visitJoin(self, obj):
        self.offset += 1
        pipe_str = self.visit(obj.pipe)
        dataset_str = self.visit(obj.dataset)
        self.offset -= 1
        return f'{self.indent()}Join(\n{pipe_str}{self.indent()},' \
               f'{dataset_str}), on={obj.on} left={obj.left_on} right={obj.right_on}{self.indent()})\n'


class Serializer(Visitor):
    def __init__(self):
        super(Serializer, self).__init__()
        self.proto_by_node_id = {}
        self.nodes = []

    def _get_rootnode_id(self, node):
        if node.HasField('node_id'):
            return node.node_id
        elif node.HasField('dataset'):
            return node.dataset.name
        elif node.HasField('operator'):
            return node.operator.id
        raise Exception('invalid node type')

    def serialize(self, pipe: Pipeline):
        last = self.visit(pipe.node)

        return proto.Pipeline(
            root=last,
            nodes=self.nodes,
            inputs=[proto.Dataset(name=i.name) for i in pipe.inputs],
            signature=self._get_rootnode_id(last),
        )

    def visit(self, obj):
        node_id = obj.signature()
        if node_id not in self.proto_by_node_id and not isinstance(obj,
                Dataset):
            ret = super(Serializer, self).visit(obj)
            self.nodes.append(ret)
            self.proto_by_node_id[node_id] = ret
        if isinstance(obj, Dataset):
            return proto.Node(dataset=proto.Dataset(name=obj.name))
        return proto.Node(node_id=node_id)

    def visitDataset(self, obj):
        return proto.Node(dataset=proto.Dataset(name=obj.name), )

    def visitTransform(self, obj):
        return proto.Node(operator=proto.Operator(
            transform=proto.Transform(
                node=self.visit(obj.node),
                function=cloudpickle.dumps(obj.func),
                function_source_code=inspect.getsource(obj.func),
                timestamp_field=obj.timestamp_field, ),
            id=obj.signature()), )

    def visitAggregate(self, obj):
        return proto.Node(operator=proto.Operator(
            aggregate=proto.Aggregate(
                node=self.visit(obj.node),
                keys=obj.keys,
                aggregates=[agg.to_proto() for agg in
                            obj.aggregates]), id=obj.signature()), )

    def visitJoin(self, obj):
        if obj.on is not None:
            on = {k: k for k in obj.on}
        else:
            on = {l: r for l, r in zip(obj.left_on, obj.right_on)}

        return proto.Node(operator=proto.Operator(join=proto.Join(
            node=self.visit(obj.node),
            dataset=proto.Dataset(name=obj.dataset.name), on=on),
            id=obj.signature()), )

    def visitUnion(self, obj):
        return proto.Node(operator=proto.Operator(
            union=proto.Union(nodes=[self.visit(node) for node in
                                     obj.nodes]), id=obj.signature()), )
