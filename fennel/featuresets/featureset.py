from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from enum import Enum

import pandas as pd
from typing import (
    Any,
    cast,
    Callable,
    Dict,
    Type,
    TypeVar,
    Optional,
    List,
    overload,
    Set,
    Union,
)


from fennel.datasets import Dataset, Field
from fennel.gen.featureset_pb2 import ExtractorType
from fennel.lib.expectations import Expectations, GE_ATTR_FUNC
from fennel.lib.includes import FENNEL_INCLUDED_MOD
from fennel.lib.metadata import (
    meta,
    OWNER,
    get_meta_attr,
    set_meta_attr,
)
from fennel.lib.includes import TierSelector
from fennel.internal_lib.schema import (
    validate_val_with_dtype,
    fennel_get_optional_inner,
)
from fennel.lib.params import (
    FENNEL_INPUTS,
    FENNEL_OUTPUTS,
)

from fennel.lib import FENNEL_GEN_CODE_MARKER
from fennel.utils import (
    parse_annotation_comments,
    propogate_fennel_attributes,
    FENNEL_VIRTUAL_FILE,
)

T = TypeVar("T")
EXTRACTOR_ATTR = "__fennel_extractor__"
DEPENDS_ON_DATASETS_ATTR = "__fennel_depends_on_datasets"

RESERVED_FEATURE_NAMES = [
    "fqn_",
    "dtype",
    "featureset_name",
    "featureset",
    "extractor",
    "extractors",
    "features",
]


# ---------------------------------------------------------------------
# feature annotation
# ---------------------------------------------------------------------


def feature(
    id: int,
) -> T:  # type: ignore
    return cast(
        T,
        Feature(
            id=id,
            # Rest of fields filled in later
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
    feature.fqn_ = f"{feature.featureset_name}.{annotation_name}"
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

    try:
        if len(inspect.stack()) > 2:
            file_name = inspect.stack()[1].filename
            setattr(featureset_cls, FENNEL_VIRTUAL_FILE, file_name)
    except Exception:
        pass
    cls_module = inspect.getmodule(featureset_cls)
    owner = None
    if cls_module is not None and hasattr(cls_module, OWNER):
        owner = getattr(cls_module, OWNER)
    return Featureset(featureset_cls, features, owner)


def extractor(
    func: Optional[Callable] = None,
    depends_on: List = [],
    version: int = 0,
    tier: Optional[Union[str, List[str]]] = None,
):
    """
    extractor is a decorator for a function that extracts a feature from a
    featureset.
    """

    def _create_extractor(extractor_func: Callable, version: int):
        if not callable(extractor_func):
            raise TypeError("extractor can only be applied to functions")
        sig = inspect.signature(extractor_func)
        extractor_name = extractor_func.__name__
        params = []
        class_method = False
        if type(depends_on) is not list:
            raise TypeError(
                f"depends_on must be a list of Datasets, not a"
                f" {type(depends_on)}"
            )
        setattr(extractor_func, DEPENDS_ON_DATASETS_ATTR, list(depends_on))
        if not hasattr(extractor_func, FENNEL_INPUTS):
            inputs = []
        else:
            inputs = getattr(extractor_func, FENNEL_INPUTS)
        for name, param in sig.parameters.items():
            if not class_method and param.name != "cls":
                raise TypeError(
                    f"extractor `{extractor_name}` should have cls as the "
                    f"first parameter since they are class methods"
                )
            elif not class_method:
                class_method = True
                continue
            if param.name != "ts" and param.name != "_ts":
                raise TypeError(
                    f"extractor `{extractor_name}` should have ts as the "
                    f"second parameter"
                )
            break
        for inp in inputs:
            if not isinstance(inp, Feature):
                if hasattr(inp, "_name"):
                    name = inp._name
                elif hasattr(inp, "__name__"):
                    name = inp.__name__
                else:
                    if hasattr(inp, "fqn"):
                        name = inp.fqn()
                    else:
                        name = str(inp)
                raise TypeError(
                    f"Parameter `{name}` is not a feature, but a "
                    f"`{type(inp)}`, and hence not supported as an input for the extractor "
                    f"`{extractor_name}`"
                )
            params.append(inp)
        if hasattr(extractor_func, FENNEL_OUTPUTS):
            return_annotation = getattr(extractor_func, FENNEL_OUTPUTS)
        else:
            return_annotation = None
        outputs = []
        if return_annotation is not None:
            if isinstance(return_annotation, Feature):
                # If feature name is set, it means that the feature is from another
                # featureset.
                if (
                    "." in str(return_annotation.fqn())
                    and len(return_annotation.fqn()) > 0
                ):
                    raise TypeError(
                        "Extractors can only extract a feature defined "
                        f"in the same featureset, found "
                        f"{return_annotation.fqn()}"
                    )
                return_annotation = cast(Feature, return_annotation)
                outputs.append(return_annotation.id)
            elif isinstance(return_annotation, tuple):
                for f in return_annotation:
                    if not isinstance(f, Feature):
                        raise TypeError(
                            f"Extractor `{extractor_name}` can only return a "
                            f"set of features, but found type {type(f)} in "
                            f"output annotation."
                        )
                    f = cast(Feature, f)
                    if "." in f.fqn() and len(f.fqn()) > 0:
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

        extractor_func = _add_featureset_name(extractor_func, params)
        setattr(
            extractor_func,
            EXTRACTOR_ATTR,
            Extractor(
                extractor_name,
                ExtractorType.PY_FUNC,
                params,
                outputs,
                version,
                func=extractor_func,
                tier=tier,
            ),
        )
        return classmethod(extractor_func)

    def wrap(c: Callable):
        return _create_extractor(c, version)

    if func is None:
        # We're being called as @extractor(version=int).
        if version is None:
            raise TypeError("version must be specified as an integer.")
        if not isinstance(version, int):
            raise TypeError("version for extractor must be an int.")
        return wrap
    func = cast(Callable, func)
    # @extractor decorator was used without arguments
    return wrap(func)


# ---------------------------------------------------------------------
# Featureset & Extractor
# ---------------------------------------------------------------------


@dataclass
class Feature:
    id: int
    _name: str = ""
    _featureset_name: str = ""
    fqn_: str = ""
    dtype: Optional[Type] = None
    extractor: Optional[Extractor] = None
    deprecated: bool = False

    def meta(self, **kwargs: Any) -> T:  # type: ignore
        return cast(T, meta(**kwargs)(self))

    def __repr__(self) -> str:
        return self.fqn_

    def __hash__(self) -> int:
        return hash(self.fqn_)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Feature):
            return self.fqn_ == other.fqn_
        if isinstance(other, str):
            return self.fqn_ == other
        return False

    def __str__(self) -> str:
        return self.name

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name
        if self.extractor:
            self.extractor.output_features = [name]

    @property
    def featureset_name(self) -> str:
        return self._featureset_name

    @featureset_name.setter
    def featureset_name(self, name: str) -> None:
        self._featureset_name = name
        if self.extractor:
            self.extractor.featureset = name

    def fqn(self) -> str:
        return self.fqn_

    def extract(
        self,
        *,
        field: Field = None,
        provider: Featureset = None,
        default=None,
        feature: Feature = None,
        version: int = 0,
        tier: Optional[Union[str, List[str]]] = None,
    ) -> Feature:
        """
        Derives an extractor for the feature using the given params.
        The derived extractor either performs a lookup on the given field,
        or is an alias to the given feature.

        Parameters:
        field: the field in a dataset to lookup and return
        provider: the input featureset that should contain features matching
                  the named of all key fields. If not provided, then the current featureset
                  is assumed to contain the respective features
        default: An optional default value to fill null values from the lookup
        feature: If provided, this function creates a one way alias from the
                    calling feature to this feature.
        version: the version of this extractor
        tiers: The tiers which are enabled for this feature. If None, then all tiers are enabled.

        Returns:
        Feature: This feature
        """
        if self.extractor:
            raise TypeError(
                f"extract() can only be called once for feature id={self.id}"
            )
        if (not field and not feature) or (field and feature):
            raise TypeError(
                f"Exactly one of field or feature must be specified to extract feature id={self.id}"
            )

        # aliasing
        if feature:
            self.extractor = Extractor(
                name=f"_fennel_alias_{feature}",
                extractor_type=ExtractorType.ALIAS,
                inputs=[feature],
                outputs=[self.id],
                version=version,
                tier=tier,
            )
            return self
        provider_features = []
        # If provider is none, then the provider is this featureset. The input features
        # are captured once this featureset is initialized
        name = f"_fennel_lookup_{field}"
        field = cast(Field, field)
        ds = None
        if provider:
            if hasattr(field, "dataset"):
                ds = field.dataset
            else:
                raise ValueError(
                    f"Dataset `{field.dataset_name}` not found for field `{field}`"
                )

            for k in ds.dsschema().keys:  # type: ignore
                feature = provider.feature(k)
                if not feature:
                    raise ValueError(
                        f"Dataset key `{k}` not found in provider `{provider._name}` for extractor `{name}`"
                    )
                provider_features.append(feature)

        self.extractor = Extractor(
            name=f"_fennel_lookup_{field}",
            extractor_type=ExtractorType.LOOKUP,
            inputs=provider_features,
            outputs=[self.id],
            version=version,
            derived_extractor_info=Extractor.DatasetLookupInfo(field, default),
            depends_on=[ds] if ds else [],
            tier=tier,
        )
        return self


def is_valid_feature(feature_name: str):
    if "." not in feature_name or len(feature_name.split(".")) != 2:
        raise Exception(
            f"Invalid input feature name {feature_name}. "
            "Please provide the feature name in the format <featureset>.<feature>."
        )
    return True


def is_user_defined(obj):
    return inspect.isclass(type(obj)) and not inspect.isbuiltin(obj)


def _add_featureset_name(func, params):
    """Rewrites the output column names of the extractor to be fully qualified names.
    Also add feature names to the input parameters of the extractor.
    """

    @functools.wraps(func)
    def inner(*args, **kwargs):
        # Add param names from params to args
        assert len(args) == len(params) + 2
        args = list(args)
        # The cls and ts parameter are unchanged
        renamed_args = args[:2] + [
            arg.rename(name.fqn()) for name, arg in zip(params, args[2:])
        ]
        ret = func(*renamed_args, **kwargs)
        fs_name = func.__qualname__.split(".")[0]
        if isinstance(ret, pd.Series):
            if ret.name is None:
                raise ValueError(
                    f"Expected a named Series but got {ret} in "
                    f"{func.__qualname__}. Please use pd.Series(data=..., "
                    f"name=...)."
                )
            ret.name = f"{fs_name}.{ret.name}"
        elif isinstance(ret, pd.DataFrame):
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
    _id_to_feature: Dict[int, Feature] = {}
    _expectation: Expectations
    owner: Optional[str] = None

    def __init__(
        self,
        featureset_cls: Type[T],
        features: List[Feature],
        owner: Optional[str] = None,
    ):
        self.__fennel_original_cls__ = featureset_cls
        self._name = featureset_cls.__name__
        self._features = features
        self._feature_map = {feature.name: feature for feature in features}
        self._id_to_feature = {feature.id: feature for feature in features}
        self._validate()
        self._extractors = self._get_extractors()
        self._add_feature_names_as_attributes()
        self._set_extractors_as_attributes()
        self._expectation = self._get_expectations()
        setattr(self, OWNER, owner)
        propogate_fennel_attributes(featureset_cls, self)

    def get_dataset_dependencies(self) -> List[Dataset]:
        """
        This function gets the list of datasets the Featureset depends upon.
        This dependency is introduced by features that directly lookup a dataset
        via the DS-FS route.

        The motivation for this function is to help generated the required code, even
        if an extractor does not depend on a dataset, but is part of a featureset which
        has these kinds of dependencies.
        """
        depended_datasets = []
        for f in self._features:
            if (
                f.extractor is not None
                and f.extractor.derived_extractor_info is not None
            ):
                assert (
                    f.extractor.derived_extractor_info.field.dataset is not None
                )
                depended_datasets.append(
                    f.extractor.derived_extractor_info.field.dataset
                )

        return depended_datasets

    def get_featureset_dependencies(self) -> List[str]:
        """
        This function gets the list of featuresets the Featureset depends upon.
        This dependency is introduced by features that directly lookup a featureset
        via the FS-DS route, while specifying a provider.
        """
        depended_featuresets = set()
        for f in self._features:
            if f.extractor is None:
                continue
            if f.extractor.extractor_type == ExtractorType.ALIAS:
                # Alias extractors have exactly one input feature
                depended_featuresets.add(f.extractor.inputs[0].featureset_name)
            elif f.extractor.extractor_type == ExtractorType.LOOKUP:
                for inp_feature in f.extractor.inputs:
                    depended_featuresets.add(inp_feature.featureset_name)
        return list(depended_featuresets)

    # ------------------- Private Methods ----------------------------------

    def _add_feature_names_as_attributes(self):
        for feature in self._features:
            setattr(self, feature.name, feature)

    def all(self) -> List[Feature]:
        return self._features

    def _get_extractors(self) -> List[Extractor]:
        extractors = []
        # auto generated extractors for features
        for feature in self._features:
            if feature.extractor:
                extractor = feature.extractor
                if extractor.extractor_type == ExtractorType.LOOKUP and (
                    extractor.inputs is None or len(extractor.inputs) == 0
                ):
                    feature.extractor.set_inputs_from_featureset(self, feature)
                extractors.append(extractor)

        # user defined extractors
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
            # Set name of the features which the extractor sets values for.
            extractor.output_features = [
                f"{self._id_to_feature[fid].name}"
                for fid in extractor.output_feature_ids
            ]
            extractor.featureset = self._name
            extractors.append(extractor)
        return extractors

    def _validate(self):
        cls_module = inspect.getmodule(self.__fennel_original_cls__)
        if cls_module is not None and hasattr(
            cls_module, FENNEL_GEN_CODE_MARKER
        ):
            if getattr(cls_module, FENNEL_GEN_CODE_MARKER):
                return

        # Validate all auto generated extractors.
        for feature in self._features:
            if feature.extractor:
                extractor = feature.extractor
                if extractor.extractor_type == ExtractorType.ALIAS:
                    # Check that the types match
                    if feature.dtype != extractor.inputs[0].dtype:
                        raise TypeError(
                            f"Feature `{feature.fqn()}` has type `{feature.dtype}` "
                            f"but the extractor aliasing `{extractor.inputs[0].fqn()}` has input type "
                            f"`{extractor.inputs[0].dtype}`."
                        )
                if extractor.extractor_type == ExtractorType.LOOKUP:
                    # Check that the types match
                    field_dtype = extractor.derived_extractor_info.field.dtype
                    if extractor.derived_extractor_info.default is not None:
                        if extractor.derived_extractor_info.field.is_optional():
                            field_dtype = fennel_get_optional_inner(field_dtype)

                        if feature.dtype != field_dtype:
                            raise TypeError(
                                f"Feature `{feature.fqn()}` has type `{feature.dtype}` "
                                f"but expected type `{extractor.derived_extractor_info.field.dtype}`."
                            )
                    else:
                        if extractor.derived_extractor_info.field.is_optional():
                            expected_field_type = field_dtype
                        else:
                            expected_field_type = Optional[field_dtype]

                        if feature.dtype != expected_field_type:
                            raise TypeError(
                                f"Feature `{feature.fqn()}` has type `{feature.dtype}` "
                                f"but expectected type `{expected_field_type}`"
                            )
                    # Check that the default value has the right type
                    if extractor.derived_extractor_info.default is not None:
                        try:
                            validate_val_with_dtype(
                                extractor.derived_extractor_info.field.dtype,
                                extractor.derived_extractor_info.default,
                            )
                        except ValueError as e:
                            raise ValueError(
                                f"Default value `{extractor.derived_extractor_info.default}` for feature `{feature.fqn()}` has incorrect default value: {e}"
                            )

        # Check that all features have unique ids.
        feature_id_set = set()
        for feature in self._features:
            # Check features dont have protected names.
            if feature.name in RESERVED_FEATURE_NAMES:
                raise ValueError(
                    f"Feature `{feature.name}` in `{self._name}` has a "
                    f"reserved name `{feature.name}`."
                )
            if feature.id in feature_id_set:
                raise ValueError(
                    f"Feature `{feature.name}` has a duplicate id `"
                    f"{feature.id}` in featureset `{self._name}`."
                )
            feature_id_set.add(feature.id)

    def _set_extractors_as_attributes(self):
        for extractor in self._extractors:
            if extractor.extractor_type == ExtractorType.PY_FUNC:
                setattr(self, extractor.func.__name__, extractor.func)

    def _get_expectations(self):
        expectation = None

        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, GE_ATTR_FUNC):
                continue
            if expectation is not None:
                raise ValueError(
                    f"Multiple expectations are not supported for featureset"
                    f" {self._name}."
                )
            expectation = getattr(method, GE_ATTR_FUNC)

        if expectation is None:
            return None

        raise NotImplementedError(
            "Expectations are not yet supported for featuresets."
        )

    @property
    def extractors(self):
        return self._extractors

    @property
    def features(self):
        return self._features

    def feature(self, name):
        if name not in self._feature_map:
            return None
        return self._feature_map[name]

    @property
    def original_cls(self):
        return self.__fennel_original_cls__


class Extractor:
    # Name of the function that implements the extractor.
    name: str
    extractor_type: ExtractorType
    inputs: List[Feature]
    func: Optional[Callable]
    derived_extractor_info: Optional[DatasetLookupInfo]
    featureset: str
    # If outputs is empty, entire featureset is being extracted
    # by this extractor, else stores the ids of the features being extracted.
    output_feature_ids: List[int]
    # List of names of features that this extractor produces
    output_features: List[str]
    # depended on datasets: used for autogenerated extractors
    depends_on: List[Dataset]

    tiers: TierSelector

    def __init__(
        self,
        name: str,
        extractor_type: ExtractorType,
        inputs: List,
        outputs: List[int],
        version: int,
        func: Optional[Callable] = None,
        derived_extractor_info: Optional[DatasetLookupInfo] = None,
        depends_on: List[Dataset] = [],
        tier: Optional[Union[str, List[str]]] = None,
    ):
        self.name = name
        self.extractor_type = extractor_type
        self.inputs = inputs
        self.func = func  # type: ignore
        self.derived_extractor_info = derived_extractor_info
        self.output_feature_ids = outputs
        self.version = version
        self.depends_on = depends_on
        self.tiers = TierSelector(tier)

    def fqn(self) -> str:
        """Fully qualified name of the extractor."""
        return f"{self.featureset}.{self.name}"

    def fqn_output_features(self) -> List[str]:
        """Fully qualified name of the output features of this extractor."""
        return [
            f"{self.featureset}.{feature}" for feature in self.output_features
        ]

    def get_dataset_dependencies(self) -> List[Dataset]:
        if self.depends_on:
            return self.depends_on
        depended_datasets = []
        if hasattr(self.func, DEPENDS_ON_DATASETS_ATTR):
            depended_datasets = getattr(self.func, DEPENDS_ON_DATASETS_ATTR)
        return depended_datasets

    def get_included_modules(self) -> List[Callable]:
        if hasattr(self.func, FENNEL_INCLUDED_MOD):
            return getattr(self.func, FENNEL_INCLUDED_MOD)
        return []

    def set_inputs_from_featureset(
        self, featureset: Featureset, feature: Feature
    ):
        if self.inputs and len(self.inputs) > 0:
            return
        if self.extractor_type != ExtractorType.LOOKUP:
            return
        if not self.derived_extractor_info or not hasattr(
            self.derived_extractor_info, "field"
        ):
            raise ValueError("A lookup extractor must have a field to lookup")
        self.inputs = []

        field = self.derived_extractor_info.field
        ds = field.dataset
        ds = None
        if hasattr(field, "dataset"):
            ds = field.dataset
        if not ds:
            raise ValueError(
                f"Dataset `{field.dataset_name}` not found for field `{field}`"
            )
        self.depends_on = [ds]
        for k in ds.dsschema().keys:
            f = featureset.feature(k)
            if not f:
                raise ValueError(
                    f"Key field `{k}` for dataset `{ds._name}` not found in provider `{featureset._name}` for feature: `{feature.name}` auto generated extractor"
                )
            self.inputs.append(f)

    class DatasetLookupInfo:
        field: Field
        default: Optional[Any] = None

        def __init__(self, field: Field, default_val: Optional[Any] = None):
            self.field = field
            self.default = default_val


def sync_validation_for_extractors(extractors: List[Extractor]):
    """
    This validation function contains the checks that are run just before the sync call.
    It should only contain checks that are not possible to run during the registration phase/compilation phase.
    """
    extracted_features: Set[str] = set()
    for extractor in extractors:
        for feature in extractor.output_features:
            if feature in extracted_features:
                raise TypeError(
                    f"Feature `{feature}` is "
                    f"extracted by multiple extractors including `{extractor.name}` in featureset `{extractor.featureset}`."
                )
            extracted_features.add(feature)
