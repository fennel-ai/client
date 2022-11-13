from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from typing import (
    Any,
    cast,
    Callable,
    Dict,
    Optional,
    Type,
    TypeVar,
    List,
    Union,
    ForwardRef,
    Set,
)

import cloudpickle
import pyarrow

import fennel.gen.featureset_pb2 as proto
from fennel.datasets import Dataset
from fennel.lib.metadata import (
    meta,
    get_meta_attr,
    set_meta_attr,
    get_metadata_proto,
)
from fennel.lib.schema import get_pyarrow_schema
from fennel.utils import parse_annotation_comments, propogate_fennel_attributes

T = TypeVar("T")


# ---------------------------------------------------------------------
# feature annotation
# ---------------------------------------------------------------------


def feature(
    id: int,
) -> T:
    return cast(
        T,
        Feature(
            id=id,
            # These fields will be filled in later.
            name="",
            dtype=None,
            featureset_name="",
        ),
    )


# ---------------------------------------------------------------------
# featureset & extractor decorators
# ---------------------------------------------------------------------


def get_feature(
    cls: Type,
    annotation_name: str,
    dtype: Type,
    field2comment_map: Dict[str, str],
) -> Feature:
    feature = getattr(cls, annotation_name, None)
    if not isinstance(feature, Feature):
        raise TypeError(f"Field {annotation_name} is not a Feature")

    feature.featureset_name = cls.__name__
    feature.name = annotation_name
    if "." in annotation_name:
        raise ValueError(
            f"Feature name {annotation_name} cannot contain a " f"period"
        )
    feature.dtype = dtype
    description = get_meta_attr(feature, "description")
    if description is None or description == "":
        description = field2comment_map.get(annotation_name, "")
        set_meta_attr(feature, "description", description)

    feature.dtype = get_pyarrow_schema(dtype)
    return feature


def featureset(featureset_cls: Type[T]):
    """featureset is a decorator for creating a Featureset class."""
    cls_annotations = featureset_cls.__dict__.get("__annotations__", {})

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
    extractor_name = extractor_func.__name__
    params = []
    for name, param in sig.parameters.items():
        if param.name == "self":
            raise TypeError(
                "extractor functions cannot have self as a "
                "parameter and are like static methods"
            )
        if param.name == "ts":
            continue
        if not isinstance(param.annotation, Featureset) and not isinstance(
            param.annotation, Feature
        ):
            raise TypeError(
                f"Parameter {name} is not a Featureset or a "
                f"feature but a {type(param.annotation)}"
            )
        params.append(param.annotation)
    # x = type("userid", (int,), {})
    return_annotation = extractor_func.__annotations__.get("return", None)
    outputs = []
    if return_annotation is not None:
        # The _name is right, don't change it.
        if (
            "_name" not in return_annotation.__dict__
            or return_annotation.__dict__["_name"] != "Tuple"
        ):
            raise TypeError("extractor functions must return a tuple")
        for type_ in return_annotation.__dict__["__args__"]:
            if not isinstance(type_, ForwardRef):
                raise TypeError(
                    f"extractor {extractor_name}() must "
                    f"extract features for the Featureset "
                    f"it is defined in, found {type_}"
                )
            outputs.append(type_.__forward_arg__)
    setattr(
        extractor_func,
        "__fennel_extractor__",
        Extractor(extractor_name, params, extractor_func, outputs),
    )
    return extractor_func


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
    id: int
    featureset_name: str
    dtype: pyarrow.lib.Schema
    wip: bool = False
    deprecated: bool = False

    def to_proto(self) -> proto.Feature:
        return proto.Feature(
            id=self.id,
            name=self.name,
            dtype=str(self.dtype),
            metadata=get_metadata_proto(self),
        )

    def to_proto_as_input(self) -> proto.Input.Feature:
        return proto.Input.Feature(
            feature_set=proto.Input.FeatureSet(
                name=self.featureset_name,
            ),
            name=self.name,
        )

    def meta(self, **kwargs: Any) -> Feature:
        return meta(**kwargs)(self)


class Featureset:
    """Featureset is a class that defines a group of features that belong to
    an entity. It contains several extractors that provide the
    logic of resolving the features it contains from other features/featuresets
    and can depend on on or more Datasets."""

    name: str
    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    __fennel_original_cls__: Type
    _features: List[Feature]
    _feature_map: Dict[str, Feature] = {}
    _extractors: List[Extractor]

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
        self._validate()
        propogate_fennel_attributes(featureset_cls, self)

    def __getattr__(self, key):
        if key in self.__fennel_original_cls__.__dict__["__annotations__"]:
            return self._feature_map[key]
        return super().__getattribute__(key)

    # ------------------- Public Methods --------------------------

    def signature(self) -> str:
        pass

    def create_featureset_request_proto(self):
        self._check_owner_exists()
        return proto.CreateFeaturesetRequest(
            name=self.name,
            features=[feature.to_proto() for feature in self._features],
            extractors=[extractor.to_proto() for extractor in self._extractors],
            # Currently we don't support versioning of featuresets.
            # Kept for future use.
            version=0,
            metadata=get_metadata_proto(self),
        )

    def to_proto(self):
        return proto.Input.FeatureSet(
            name=self.name,
        )

    # ------------------- Private Methods ----------------------------------

    def _check_owner_exists(self):
        owner = get_meta_attr(self, "owner")
        if owner is None or owner == "":
            raise Exception(f"Featureset {self.name} must have an owner.")

    def _get_extractors(self) -> List[Extractor]:
        extractors = []
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, "__fennel_extractor__"):
                continue
            extractors.append(method.__fennel_extractor__)
        return extractors

    def _validate(self):
        # Check that all features have unique ids.
        feature_id_set = set()
        for feature in self._features:
            if feature.id in feature_id_set:
                raise ValueError(
                    f"Feature {feature.name} has a duplicate id {feature.id}"
                )
            feature_id_set.add(feature.id)

        # Check that each feature is extracted by at max one extractor.
        extracted_features: Set[str] = set()
        for extractor in self._extractors:
            if extractor.outputs is None:
                for feature in self._features:
                    if feature.name not in extracted_features:
                        extracted_features.add(feature.name)
                    else:
                        raise TypeError(
                            f"Feature {feature.name} is "
                            f"extracted multiple times"
                        )

            for feature in extractor.outputs:
                if feature in extracted_features:
                    raise TypeError(
                        f"Feature {feature} is extracted by "
                        f"multiple extractors"
                    )
                extracted_features.add(feature)


class Extractor:
    name: str
    inputs: List[Union[Feature, Featureset]]
    func: Callable
    # If outputs is None, entire featureset is being extracted
    outputs: Optional[List[str]]

    def __init__(
        self,
        name: str,
        inputs: List[Union[Feature, Featureset]],
        func: Callable,
        outputs: Optional[List[str]],
    ):
        self.name = name
        self.inputs = inputs
        self.func = func  # type: ignore
        self.outputs = outputs if outputs else []

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
                raise TypeError(
                    f"Extractor input {input} is not a Feature or "
                    f"Featureset but a {type(input)}"
                )
        return proto.Extractor(
            name=self.name,
            func=cloudpickle.dumps(self.func),
            func_source_code=inspect.getsource(self.func),
            datasets=[
                dataset.name for dataset in self.func._depends_on_datasets
            ]
            if hasattr(self.func, "_depends_on_datasets")
            else [],
            inputs=inputs,
            features=self.outputs,
            metadata=get_metadata_proto(self.func),
        )
