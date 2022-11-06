from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from typing import (Callable, Dict, Optional, Type, TypeVar, List, Union)

import cloudpickle
import pyarrow

import fennel.gen.featureset_pb2 as proto
from fennel.dataset import Dataset
from fennel.lib.field import Field
from fennel.lib.schema import get_pyarrow_schema
from fennel.utils import parse_annotation_comments

T = TypeVar('T')


# ---------------------------------------------------------------------
# featureset & extractor decorators
# ---------------------------------------------------------------------

def get_feature(
        cls: Type,
        annotation_name: str,
        dtype: Type,
        field2comment_map: Dict[str, str],
) -> Field:
    field = getattr(cls, annotation_name, None)
    if isinstance(field, Field):
        feature = Feature(
            name=annotation_name,
            featureset_name=cls.__name__,
            dtype=dtype,
            owner=field.owner,
            description=field.description,
        )
    else:
        feature = Feature(
            name=annotation_name,
            featureset_name=cls.__name__,
            dtype=dtype,
            owner=None,
            description='',
        )

    if feature.description is None:
        feature.description = field2comment_map.get(annotation_name, '')
    feature.dtype = get_pyarrow_schema(dtype)
    return feature


def featureset(featureset_cls: Type[T]):
    """featureset is a decorator for creating a Featureset class."""
    cls_annotations = featureset_cls.__dict__.get('__annotations__', {})
    fields = [
        get_feature(
            cls=featureset_cls,
            annotation_name=name,
            dtype=cls_annotations[name],
            field2comment_map=parse_annotation_comments(featureset_cls),
        )
        for name in cls_annotations
    ]

    return Featureset(
        featureset_cls,
        fields,
    )


def extractor(extractor_func: Callable):
    """
    extractor is a decorator for a function that extracts a feature from a
    featureset.
    """
    if not callable(extractor_func):
        raise TypeError("extractor can only be applied to functions")
    sig = inspect.signature(extractor_func)
    params = []
    for name, param in sig.parameters.items():
        if param.name == 'self':
            raise TypeError('extractor functions cannot have self as a '
                            'parameter and are like static methods')
        if param.name == 'ts':
            continue
        if not isinstance(param.annotation, Featureset) and not isinstance(
                param.annotation, Feature):
            raise TypeError(f'Parameter {name} is not a Featureset or a '
                            f'feature but a {type(param.annotation)}')
        params.append(param.annotation)
    setattr(extractor, "__fennel_extractor__",
        Extractor(params, extractor_func))
    return extractor


def depends_on(*datasets: Type[Dataset]):
    def decorator(func):
        setattr(func, "_depends_on_datasets", list(datasets))

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


# ---------------------------------------------------------------------
# Featureset & Extractor
# ---------------------------------------------------------------------

@dataclass
class Feature:
    name: str
    featureset_name: str
    owner: str
    description: str
    dtype: pyarrow.lib.Schema

    def to_proto(self) -> proto.Feature:
        return proto.Feature(
            feature_set=proto.FeatureSet(
                name=self.featureset_name,
            ),
            name=self.name,
            owner=self.owner,
            description=self.description,
            dtype=str(self.dtype),
        )

    def to_proto_as_input(self) -> proto.Feature:
        return proto.Feature(
            feature_set=proto.FeatureSet(
                name=self.featureset_name,
            ),
            name=self.name,
        )


class Featureset:
    """Featureset is a class that defines a group of features that belong to
    an entity. It contains several extractors that provide the
    logic of resolving the features it contains from other features/featuresets
    and can depend on on or more Datasets."""
    name: str
    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    __fennel_original_cls__: Type[T]
    _features: List[Field]
    _feature_map: Dict[str, Feature] = {}
    _extractors: List[Extractor]
    _owner: Optional[str]
    _description: Optional[str]

    def __init__(
            self,
            featureset_cls: Type[T],
            fields: List[Feature],
    ):
        self.__fennel_original_cls__ = featureset_cls
        self.name = featureset_cls.__name__
        self.__name__ = featureset_cls.__name__
        self._features = fields
        self._feature_map = {field.name: field for field in fields}
        self._extractors = self._get_extractors()
        self._owner = None
        self._description = None

    def __getattr__(self, key):
        if key in self.__fennel_original_cls__.__dict__['__annotations__']:
            return self._feature_map[key]
        return super().__getattribute__(key)

    # ---------------------------------------------------------------------
    # Public Methods
    # ---------------------------------------------------------------------

    def signature(self) -> str:
        pass

    def create_featureset_request_proto(self):
        return proto.CreateFeaturesetRequest(
            name=self.name,
            owner=self._owner if self._owner else '',
            description=self._description if self._description else '',
            features=[feature.to_proto() for feature in self._features],
            extractors=[extractor.to_proto() for extractor in self._extractors],
        )

    def to_proto(self):
        return proto.FeatureSet(
            name=self.name,
        )

    # ---------------------------------------------------------------------
    # Private Methods
    # ---------------------------------------------------------------------

    def _get_extractors(self) -> List[Extractor]:
        extractors = []
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, '__fennel_extractor__'):
                continue
            extractors.append(method.__fennel_extractor__)
        return extractors


class Extractor:
    inputs: List[Featureset]
    func: Callable

    def __init__(self, inputs: List[Union[Feature, Featureset]],
                 func: Callable):
        self.inputs = inputs
        self.func = func

    def signature(self) -> str:
        pass

    def to_proto(self):
        inputs = []
        for input in self.inputs:
            if isinstance(input, Feature):
                inputs.append(proto.Input(feature=input.to_proto_as_input()))
            elif isinstance(input, Featureset):
                inputs.append(proto.Input(feature_set=input.to_proto()))
            else:
                raise TypeError(f'Extractor input {input} is not a Feature or '
                                f'Featureset but a {type(input)}')
        return proto.Extractor(
            name=self.func.__name__,
            func=cloudpickle.dumps(self.func),
            func_source_code=inspect.getsource(self.func),
            datasets=[dataset.name for dataset in
                      self.func._depends_on_datasets],
            inputs=inputs,
        )
