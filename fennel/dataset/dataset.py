import ast
import cloudpickle
import datetime
import functools
import inspect
import pyarrow
import textwrap
from dataclasses import dataclass
from typing import (Callable, cast, Dict, List, Mapping, Optional, Tuple, Type,
                    TypeVar, Union)

import fennel.gen.dataset_pb2 as proto
from fennel.concept import FennelConcept
from fennel.dataset.aggregate import AggregateType
from fennel.lib.schema import get_pyarrow_field
from fennel.utils.duration import Duration, duration_to_timedelta, \
    timedelta_to_micros

Tags = Union[List[str], Tuple[str, ...], str]

T = TypeVar("T")

DEFAULT_RETENTION = Duration("2y")
DEFAULT_MAX_STALENESS = Duration("30d")


# ---------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------

@dataclass
class Field:
    name: str
    key: bool
    timestamp: bool
    owner: str
    description: str
    pa_field: pyarrow.lib.Field
    dtype: Type
    aggregation: Optional[AggregateType] = None

    def __post_init__(self):
        if self.aggregation:
            self.aggregation.validate()
            
    def to_proto(self) -> proto.DatasetField:
        # Type is passed as part of the dataset schema
        return proto.DatasetField(
            name=self.name,
            is_key=self.key,
            is_timestamp=self.timestamp,
            owner=self.owner,
            description=self.description,
            is_nullable=self.pa_field.nullable,
            aggregation=self.aggregation.to_proto() if self.aggregation else None,
        )


def field(
        key: bool = False,
        timestamp: bool = False,
        description: Optional[str] = None,
        owner: Optional[str] = None,
) -> T:
    return cast(
        T,
        Field(
            key=key,
            timestamp=timestamp,
            owner=owner,
            description=description,
            # These fields will be filled in later.
            name="",
            pa_field=None,
            dtype=None,
        ),
    )


# ---------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------

def _parse_annotation_comments(cls: Type[T]) -> Mapping[str, str]:
    try:
        source = textwrap.dedent(inspect.getsource(cls))

        source_lines = source.splitlines()
        tree = ast.parse(source)
        if len(tree.body) != 1:
            return {}

        comments_for_annotations: Dict[str, str] = {}
        class_def = tree.body[0]
        if isinstance(class_def, ast.ClassDef):
            for stmt in class_def.body:
                if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target,
                        ast.Name):
                    line = stmt.lineno - 2
                    comments: List[str] = []
                    while line >= 0 and source_lines[line].strip().startswith(
                            "#"):
                        comment = source_lines[line].strip().strip("#").strip()
                        comments.insert(0, comment)
                        line -= 1

                    if len(comments) > 0:
                        comments_for_annotations[
                            stmt.target.id] = textwrap.dedent(
                            "\n".join(comments))

        return comments_for_annotations
    except Exception:
        return {}


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
    elif isinstance(field, AggregateType):
        field = Field(name=annotation_name, key=False, timestamp=False,
            owner=None, description="", pa_field=None, dtype=dtype,
            aggregation=field)
    else:
        field = Field(name=annotation_name, key=False, timestamp=False,
            owner=None, description="", pa_field=None, dtype=dtype)

    field.description = field2comment_map.get(annotation_name, "")
    field.pa_field = get_pyarrow_field(annotation_name, dtype)
    return field


def _create_dataset(cls: Type[T], retention: Duration = DEFAULT_RETENTION,
                    max_staleness: Duration = DEFAULT_MAX_STALENESS) -> T:
    cls_annotations = cls.__dict__.get("__annotations__", {})
    fields = [
        _get_field(
            cls=cls,
            annotation_name=name,
            dtype=cls_annotations[name],
            field2comment_map=_parse_annotation_comments(cls),
        )
        for name in cls_annotations
    ]

    pull_fn = getattr(cls, "pull", None)
    return Dataset(
        cls,
        fields,
        retention=duration_to_timedelta(retention),
        max_staleness=duration_to_timedelta(max_staleness),
        pull_fn=pull_fn,
    )


def _create_aggregated_dataset(cls: Type[T], base_dataset_cls: Type[T],
                               retention: Duration = DEFAULT_RETENTION,
                               max_staleness: Duration = DEFAULT_MAX_STALENESS) -> T:
    cls_annotations = cls.__dict__.get("__annotations__", {})
    fields = [
        _get_field(
            cls=cls,
            annotation_name=name,
            dtype=cls_annotations[name],
            field2comment_map=_parse_annotation_comments(cls),
        )
        for name in cls_annotations
    ]

    if getattr(cls, "pull", None) != None:
        raise ValueError("Aggregated datasets cannot have a pull function")

    return AggregateDataset(
        cls,
        fields,
        retention=duration_to_timedelta(retention),
        max_staleness=duration_to_timedelta(max_staleness),
        base_dataset_cls_name=base_dataset_cls.name,
    )


class dataset:
    """dataset is a decorator that creates a Dataset class.
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
        return _create_dataset(dataset_cls, *args, **kwargs)

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
        return _create_dataset(dataset_cls, self._retention,
            self._max_staleness)

    def aggregate(arg1, arg2: Optional[Type[T]] = None):
        """aggregate is a decorator that creates a Dataset class that
        aggregates another dataset.
        Parameters
        ----------
        arg1 : self or Dataset
            If arg1 is self, arg2 is the base dataset class.
            else arg1 is the base_dataset_cls.
        arg2 : Dataset ( Optional )
            The dataset to aggregate, used when dataset() creates an object
            and hence arg1 is self.
        """

        base_dataset_cls = arg2
        if base_dataset_cls is None:
            # We are calling it as @dataset.aggregate
            base_dataset_cls = arg1

        def wrapper(aggregated_cls):
            if arg2 is not None:
                return _create_aggregated_dataset(aggregated_cls,
                    base_dataset_cls,
                    arg1._retention,
                    arg1._max_staleness)
            return _create_aggregated_dataset(aggregated_cls, base_dataset_cls)

        return wrapper


class Dataset(FennelConcept):
    """Dataset is a collection of data."""
    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    _retention: datetime.timedelta
    _fields: List[Field]
    _key_field: str
    _timestamp_field: str
    _max_staleness: Optional[datetime.timedelta]
    _owner: Optional[str]
    __fennel_original_cls__: Optional[Type]
    pull_fn: Optional[Callable]

    def __init__(
            self,
            cls: Type,
            fields: List[Field],
            retention: Optional[datetime.timedelta] = None,
            max_staleness: Optional[datetime.timedelta] = None,
            pull_fn: Optional[Callable] = None,
    ):
        super().__init__(cls.__name__)
        self.__name__ = cls.__name__
        self._fields = fields
        self._set_timestamp_field()
        self._retention = retention
        self._max_staleness = max_staleness
        self.__fennel_original_cls__ = cls
        self.pull_fn = pull_fn

    def __getattr__(self, key):
        if key in self.__fennel_original_cls__.__dict__["__annotations__"]:
            return str(key)
        return super().__getattribute__(key)

    def _set_timestamp_field(self):
        timestamp_field_set = False
        for field in self._fields:
            if field.timestamp:
                if timestamp_field_set:
                    raise ValueError(
                        "Multiple timestamp fields are not supported.")
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
                    "Multiple timestamp fields are not supported.")
        if not timestamp_field_set:
            raise ValueError("No timestamp field found.")

    def _get_schema(self) -> bytes:
        schema = pyarrow.schema(
            [field.pa_field for field in self._fields if field.pa_field])
        return schema.serialize().to_pybytes()

    def __repr__(self):
        return f"FennelDataset({self.__name__}, {self._fields})"

    def _pull_to_proto(self) -> proto.PullLookup:
        depends_on_datasets = getattr(self.pull_fn, "_depends_on_datasets", [])
        return proto.PullLookup(
            function_source_code=inspect.getsource(self.pull_fn),
            function=cloudpickle.dumps(self.pull_fn),
            depends_on_datasets=[d.name for d in depends_on_datasets],
        )

    def to_proto(self) -> proto.CreateDatasetRequest:
        return proto.CreateDatasetRequest(
            name=self.__name__,
            schema=self._get_schema(),
            retention=timedelta_to_micros(self._retention),
            max_staleness=timedelta_to_micros(self._max_staleness),
            mode="pandas",
            # TODO: Parse description from docstring.
            description="",
            # Currently we don't support versioning of datasets.
            # Kept for future use.
            version=0,
            fields=[field.to_proto() for field in self._fields],
            pull_lookup=self._pull_to_proto() if self.pull_fn else None,
        )


class AggregateDataset(Dataset):
    _base_dataset_cls_name: Optional[str]

    def __init__(
            self,
            cls_name: str,
            fields: List[Field],
            retention: Optional[datetime.timedelta] = None,
            max_staleness: Optional[datetime.timedelta] = None,
            base_dataset_cls_name: Optional[str] = None,
    ):
        super().__init__(cls_name, fields, retention, max_staleness, None)
        self._base_dataset_cls_name = base_dataset_cls_name

    def __repr__(self):
        return f"AggregateDataset({self.__name__}, {self._fields}, " \
               f"{self.base_dataset})"

    def to_proto(self) -> proto.CreateDatasetRequest:
        return proto.CreateDatasetRequest(
            name=self.__name__,
            schema=self._get_schema(),
            retention=timedelta_to_micros(self._retention),
            max_staleness=timedelta_to_micros(self._max_staleness),
            mode="pandas",
            # TODO: Parse description from docstring.
            description="",
            # Currently we don't support versioning of datasets.
            # Kept for future use.
            version=0,
            fields=[field.to_proto() for field in self._fields],
            is_aggregated=True,
            base_dataset=self._base_dataset_cls_name,
        )


def depends_on(*datasets: Type[Dataset]):
    def decorator(func):
        setattr(func, "_depends_on_datasets", list(datasets))

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator
