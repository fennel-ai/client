from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    cast,
    Callable,
    Dict,
    Type,
    TypeVar,
    List,
    Union,
    Set,
)

import cloudpickle
import pandas as pd

import fennel.gen.featureset_pb2 as proto
from fennel.lib.metadata import (
    meta,
    get_meta_attr,
    set_meta_attr,
    get_metadata_proto,
)
from fennel.lib.schema import get_datatype
from fennel.utils import (
    parse_annotation_comments,
    propogate_fennel_attributes,
)

T = TypeVar("T")
EXTRACTOR_ATTR = "__fennel_extractor__"
DEPENDS_ON_DATASETS_ATTR = "__fennel_depends_on_datasets"


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
            fqn="",
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
    if "." in annotation_name:
        raise ValueError(
            f"Feature name {annotation_name} cannot contain a " f"period"
        )
    feature.name = annotation_name
    feature.fqn = f"{feature.featureset_name}.{annotation_name}"
    description = get_meta_attr(feature, "description")
    if description is None or description == "":
        description = field2comment_map.get(annotation_name, "")
        set_meta_attr(feature, "description", description)

    feature.dtype = dtype
    return feature


def featureset(featureset_cls: Type[T]):
    """featureset is a decorator for creating a Featureset class."""
    cls_annotations = featureset_cls.__dict__.get("__annotations__", {})
    features = [
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
        features,
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
    check_timestamp = False
    for name, param in sig.parameters.items():
        if param.name == "self":
            raise TypeError(
                "extractor functions cannot have self as a "
                "parameter and are like static methods"
            )
        if not check_timestamp:
            if param.annotation == datetime:
                check_timestamp = True
                continue
            raise TypeError(
                "extractor functions must have timestamp ( of type Series["
                "datetime] ) as the first parameter, found {} instead".format(
                    param.annotation
                )
            )
        if not isinstance(param.annotation, Featureset) and not isinstance(
            param.annotation, Feature
        ):
            raise TypeError(
                f"Parameter {name} is not a Featureset or a "
                f"feature but a {type(param.annotation)}"
            )
        params.append(param.annotation)
    return_annotation = extractor_func.__annotations__.get("return", None)
    outputs = []
    if return_annotation is not None:
        if isinstance(return_annotation, Feature):
            # If feature name is set, it means that the feature is from another
            # featureset.
            if (
                "." in str(return_annotation)
                and len(str(return_annotation)) > 0
            ):
                raise TypeError(
                    "Extractors can only extract a feature defined "
                    f"in the same featureset, found {str(return_annotation)}"
                )
            outputs.append(return_annotation.id)
        elif isinstance(return_annotation, str):
            raise TypeError(
                "str datatype not supported, please ensure "
                "from __future__ import annotations is disabled"
            )
        elif isinstance(return_annotation, tuple):
            for f in return_annotation:
                if not isinstance(f, Feature) and not isinstance(f, Featureset):
                    raise TypeError(
                        "Extractors can only return a Series[feature] or a "
                        "DataFrame[featureset]."
                    )
                if "." in str(f) and len(str(f)) > 0:
                    raise TypeError(
                        "Extractors can only extract a feature defined "
                        f"in the same featureset, found "
                        f"{str(return_annotation)}."
                    )
                outputs.append(f.id)
        elif isinstance(return_annotation, Featureset):
            raise TypeError(
                "Extractors can only return a Series[feature] or a "
                "DataFrame[<list of features defined in this Featureset>]."
            )
        else:
            raise TypeError(
                f"Return annotation {return_annotation} is not a "
                f"Series or DataFrame, found {type(return_annotation)}"
            )
    setattr(
        extractor_func,
        EXTRACTOR_ATTR,
        Extractor(extractor_name, params, extractor_func, outputs),
    )
    return extractor_func


def depends_on(*datasets: Any):
    if len(datasets) == 0:
        raise TypeError("depends_on must have at least one dataset")

    def decorator(func):
        setattr(func, DEPENDS_ON_DATASETS_ATTR, list(datasets))
        return func

    return decorator


# ---------------------------------------------------------------------
# Featureset & Extractor
# ---------------------------------------------------------------------


@dataclass
class Feature:
    name: str
    fqn: str
    id: int
    featureset_name: str
    dtype: Type
    wip: bool = False
    deprecated: bool = False

    def to_proto(self) -> proto.Feature:
        return proto.Feature(
            id=self.id,
            name=self.name,
            metadata=get_metadata_proto(self),
            dtype=get_datatype(self.dtype),
        )

    def to_proto_as_input(self) -> proto.Input.Feature:
        return proto.Input.Feature(
            feature_set=proto.Input.FeatureSet(
                name=self.featureset_name,
            ),
            name=self.name,
        )

    def meta(self, **kwargs: Any) -> T:
        return cast(T, meta(**kwargs)(self))

    def __repr__(self) -> str:
        return f"{self.fqn}"

    def __hash__(self) -> int:
        return hash(self.fqn)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Feature):
            return self.fqn == other.fqn
        if isinstance(other, str):
            return self.fqn == other
        return False


def _add_column_names(func, columns, fs_name):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        ret = func(*args, **kwargs)
        if isinstance(ret, pd.Series):
            ret.name = columns[0]
        elif isinstance(ret, pd.DataFrame):
            expected_columns = [x.split(".")[1] for x in columns]
            if len(ret.columns) != len(expected_columns) or set(
                ret.columns
            ) != set(expected_columns):
                raise ValueError(
                    f"Expected {len(expected_columns)} columns ({expected_columns}) but got"
                    f" {len(ret.columns)} columns {ret.columns} in"
                    f" {func.__name__}"
                )
            ret.columns = [f"{fs_name}.{x}" for x in ret.columns]
        return ret

    return inner


class Featureset:
    """Featureset is a class that defines a group of features that belong to
    an entity. It contains several extractors that provide the
    logic of resolving the features it contains from other features/featuresets
    and can depend on on or more Datasets."""

    _name: str
    # All attributes should start with _ to avoid conflicts with
    # the original attributes defined by the user.
    __fennel_original_cls__: Type
    _features: List[Feature]
    _feature_map: Dict[str, Feature] = {}
    _extractors: List[Extractor]
    _id_to_feature_fqn: Dict[int, str] = {}

    def __init__(
        self,
        featureset_cls: Type[T],
        features: List[Feature],
    ):
        self.__fennel_original_cls__ = featureset_cls
        self._name = featureset_cls.__name__
        self.__name__ = featureset_cls.__name__
        self._features = features
        self._feature_map = {feature.name: feature for feature in features}
        self._id_to_feature_fqn = {
            feature.id: feature.fqn for feature in features
        }
        self._extractors = self._get_extractors()
        propogate_fennel_attributes(featureset_cls, self)
        self._validate()
        self._set_extractors_as_attributes()

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
            name=self._name,
            features=[feature.to_proto() for feature in self._features],
            extractors=[
                extractor.to_proto(self._id_to_feature_fqn)
                for extractor in self._extractors
            ],
            # Currently we don't support versioning of featuresets.
            # Kept for future use.
            version=0,
            metadata=get_metadata_proto(self),
        )

    def to_proto(self):
        return proto.Input.FeatureSet(
            name=self._name,
        )

    # ------------------- Private Methods ----------------------------------

    def _check_owner_exists(self):
        owner = get_meta_attr(self, "owner")
        if owner is None or owner == "":
            raise Exception(f"Featureset {self._name} must have an owner.")

    def _get_extractors(self) -> List[Extractor]:
        extractors = []
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, EXTRACTOR_ATTR):
                continue
            extractor = getattr(method, EXTRACTOR_ATTR)
            if (
                extractor.output_feature_ids is None
                or len(extractor.output_feature_ids) == 0
            ):
                extractor.output_feature_ids = [
                    feature.id for feature in self._features
                ]
            extractor.output_features = [
                f"{self._id_to_feature_fqn[fid]}"
                for fid in extractor.output_feature_ids
            ]
            extractor.name = f"{self._name}.{extractor.name}"
            extractors.append(extractor)
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
        extracted_features: Set[int] = set()
        for extractor in self._extractors:
            for feature_id in extractor.output_feature_ids:
                if feature_id in extracted_features:
                    raise TypeError(
                        f"Feature {self._id_to_feature_fqn[feature_id]} is "
                        f"extracted by multiple extractors"
                    )
                extracted_features.add(feature_id)

    def _set_extractors_as_attributes(self):
        for extractor in self._extractors:
            columns = [
                self._id_to_feature_fqn[output]
                for output in extractor.output_feature_ids
            ]
            if len(columns) == 0:
                columns = [str(f) for f in self._features]
            extractor.func = _add_column_names(
                extractor.func, columns, self._name
            )
            setattr(self, extractor.func.__name__, extractor.func)

    @property
    def extractors(self):
        return self._extractors

    @property
    def features(self):
        return self._features


class Extractor:
    # FQN of the function that implements the extractor.
    name: str
    inputs: List[Union[Feature, Featureset]]
    func: Callable
    # If outputs is empty, entire featureset is being extracted
    # by this extractor, else stores the ids of the features being extracted.
    output_feature_ids: List[int]
    # List of FQN of features that this extractor produces
    output_features: List[str]

    def __init__(
        self,
        name: str,
        inputs: List[Union[Feature, Featureset]],
        func: Callable,
        outputs: List[int],
    ):
        self.name = name
        self.inputs = inputs
        self.func = func  # type: ignore
        self.output_feature_ids = outputs

    def signature(self) -> str:
        pass

    def to_proto(self, id_to_feature_name: Dict[int, str]) -> proto.Extractor:
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
        depended_datasets = []
        if hasattr(self.func, DEPENDS_ON_DATASETS_ATTR):
            depended_datasets = getattr(self.func, DEPENDS_ON_DATASETS_ATTR)
        return proto.Extractor(
            name=self.name,
            func=cloudpickle.dumps(self.func),
            func_source_code=inspect.getsource(self.func),
            datasets=[dataset._name for dataset in depended_datasets],
            inputs=inputs,
            # Output features are stored as names and NOT FQN.
            features=[
                id_to_feature_name[id].split(".")[1]
                for id in self.output_feature_ids
            ],
            metadata=get_metadata_proto(self.func),
        )
