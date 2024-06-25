from __future__ import annotations

import functools
import inspect
from dataclasses import dataclass
from typing import (
    Any,
    cast,
    Callable,
    Dict,
    Type,
    TypeVar,
    Optional,
    List,
    Union,
)

import pandas as pd

from fennel.datasets import Dataset, Field
from fennel.datasets.datasets import get_index, IndexDuration
from fennel.gen.featureset_pb2 import ExtractorType
from fennel.internal_lib.schema import (
    validate_val_with_dtype,
    fennel_get_optional_inner,
)
from fennel.lib import FENNEL_GEN_CODE_MARKER
from fennel.lib.expectations import Expectations, GE_ATTR_FUNC
from fennel.lib.includes import EnvSelector
from fennel.lib.includes import FENNEL_INCLUDED_MOD
from fennel.lib.metadata import (
    meta,
    OWNER,
    get_meta_attr,
    set_meta_attr,
)
from fennel.lib.params import (
    FENNEL_INPUTS,
    FENNEL_OUTPUTS,
)
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
    *args: Optional[Any],
    default: Optional[Any] = None,
    version: Optional[int] = None,
    env: Optional[Union[str, List[str]]] = None,
    **kwargs,
) -> T:  # type: ignore
    if len(args) > 1:
        raise ValueError(
            f"We can only reference to only one feature or one field at a time found : {args}"
        )
    if len(args) == 0 and default is not None:
        raise ValueError(
            'Please specify a reference to a field of a dataset to use "default" param'
        )
    if len(args) == 0 and version is None:
        version = 0

    feature_obj = Feature(
        # Rest of fields filled in later
    )
    if len(args) != 0:
        feature_obj.ref = args[0]
    else:
        feature_obj.ref = None
    feature_obj.ref_default = default
    feature_obj.ref_version = version
    feature_obj.ref_env = env

    return cast(
        T,
        feature_obj,
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
    if feature is None:
        feature = Feature()
    else:
        if not isinstance(feature, Feature):
            raise TypeError(
                f"Field {annotation_name} is not a Feature, found : {type(feature)}"
            )

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
    deps: List[Dataset] = None,
    version: int = 0,
    env: Optional[Union[str, List[str]]] = None,
    **kwargs,
):
    """
    extractor is a decorator for a function that extracts a feature from a
    featureset.
    """
    if deps is None and kwargs.get("depends_on") is None:
        deps = []
    if deps is not None and kwargs.get("depends_on") is not None:
        raise ValueError("Use only one of 'depends_on' or 'deps' parameter")
    if kwargs.get("depends_on") is not None:
        deps = kwargs.get("depends_on")
        if not isinstance(deps, list):
            raise TypeError(
                f"depends_on must be a list of Datasets, not a {type(deps)}"
            )

    def _create_extractor(extractor_func: Callable, version: int):
        if not callable(extractor_func):
            raise TypeError("extractor can only be applied to functions")
        sig = inspect.signature(extractor_func)
        extractor_name = extractor_func.__name__
        params: List[Union[Feature, str]] = []
        class_method = False
        if not isinstance(deps, list):
            raise TypeError(
                f"deps must be a list of Datasets, not a {type(deps)}"
            )

        # check whether datasets in depends have either offline or online index
        for dataset in deps:
            index = get_index(dataset)
            if index is None:
                raise ValueError(
                    f"Please define either an offline or online index on dataset : {dataset._name} for extractor to work."
                )

            if not index.online and index.offline == IndexDuration.none:
                raise ValueError(
                    f"Please define either an offline or online index on dataset : {dataset._name} for extractor to work."
                )

        setattr(extractor_func, DEPENDS_ON_DATASETS_ATTR, list(deps))
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
            if not isinstance(inp, Feature) and not isinstance(inp, str):
                if hasattr(inp, "_name"):
                    name = inp._name
                elif hasattr(inp, "__name__"):
                    name = inp.__name__  # type: ignore
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
        outputs: List[Union[str, Feature]] = []
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
                outputs.append(return_annotation)
            elif isinstance(return_annotation, tuple):
                for f in return_annotation:
                    if isinstance(f, str):
                        outputs.append(f)
                    elif isinstance(f, Feature):
                        f = cast(Feature, f)
                        if "." in f.fqn() and len(f.fqn()) > 0:
                            raise TypeError(
                                "Extractors can only extract a feature defined "
                                f"in the same featureset, found "
                                f"{str(return_annotation)}."
                            )
                        outputs.append(f)
                    else:
                        raise TypeError(
                            f"Extractor `{extractor_name}` can only return a "
                            f"set of features or strings, but found type {type(f)} in "
                            f"output annotation."
                        )
            # Means output is a single Feature belonging to same featureset
            elif isinstance(return_annotation, str):
                outputs.append(return_annotation)
            elif isinstance(return_annotation, Featureset):
                raise TypeError(
                    "Extractors can only return a Series[feature, str] or a "
                    "dataframe[<list of features defined in this featureset>]."
                )
            else:
                raise TypeError(
                    f"Return annotation {return_annotation} is not a "
                    f"Series[feature, str] or dataframe[<list of features defined in this featureset>], "
                    f"found {type(return_annotation)}"
                )

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
                env=env,
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
    _ref: Optional[Union[Feature, Field]] = None
    _ref_default: Optional[Any] = None
    _ref_version: Optional[int] = None
    _ref_env: Optional[Union[str, List[str]]] = None
    _name: str = ""
    _featureset_name: str = ""
    fqn_: str = ""
    dtype: Optional[Type] = None
    deprecated: bool = False

    def meta(self, **kwargs: Any) -> T:  # type: ignore
        return cast(T, meta(**kwargs)(self))

    def __repr__(self) -> str:
        return self.fqn_

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

    @property
    def featureset_name(self) -> str:
        return self._featureset_name

    @featureset_name.setter
    def featureset_name(self, name: str) -> None:
        self._featureset_name = name

    @property
    def ref(self) -> Optional[Union[Feature, Field]]:
        return self._ref

    @ref.setter
    def ref(self, reference: Optional[Union[Feature, Field]]):
        self._ref = reference

    @property
    def ref_version(self) -> Optional[int]:
        return self._ref_version

    @ref_version.setter
    def ref_version(self, version: Optional[int]):
        self._ref_version = version

    @property
    def ref_default(self) -> Optional[Any]:
        return self._ref_default

    @ref_default.setter
    def ref_default(self, value: Optional[Any]):
        self._ref_default = value

    @property
    def ref_env(self) -> Optional[Union[str, List[str]]]:
        return self._ref_env

    @ref_env.setter
    def ref_env(self, env: Optional[Union[str, List[str]]]):
        self._ref_env = env

    def desc(self) -> str:
        return get_meta_attr(self, "description") or ""

    def fqn(self) -> str:
        return self.fqn_


def is_valid_feature(feature_name: str):
    if "." not in feature_name or len(feature_name.split(".")) != 2:
        return False
    return True


def is_valid_featureset(featureset_name: str):
    if "." in featureset_name:
        return False
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
    # Map of feature name to corresponding Generated Extractor
    _generated_extractor_map: Dict[str, Extractor] = {}
    _expectation: Optional[Expectations]
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
        self._extractors = self._get_extractors()
        self._validate()
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
        for extractor in self._extractors:
            if extractor.extractor_type == ExtractorType.LOOKUP:
                if extractor.derived_extractor_info is not None:
                    assert (
                        extractor.derived_extractor_info.field.dataset
                        is not None
                    )
                    depended_datasets.append(
                        extractor.derived_extractor_info.field.dataset
                    )
        return depended_datasets

    def get_featureset_dependencies(self) -> List[str]:
        """
        This function gets the list of featuresets the Featureset depends upon.
        This dependency is introduced by features that directly lookup a featureset
        via the FS-DS route, while specifying a provider.
        """
        depended_featuresets = set()
        for extractor in self._extractors:
            if extractor.extractor_type == ExtractorType.ALIAS:
                # Alias extractors have exactly one input feature
                depended_featuresets.add(extractor.inputs[0].featureset_name)
        return list(depended_featuresets)

    # ------------------- Private Methods ----------------------------------

    def _add_feature_names_as_attributes(self):
        for feature in self._features:
            setattr(self, feature.name, feature)

    def all(self) -> List[Feature]:
        return self._features

    def _get_generated_extractors(
        self,
    ) -> List[Extractor]:
        """
        Derives list of auto generated extractor for applicable features.
        The derived extractor either performs a lookup on the given field,
        or is an alias to the given feature.
        """
        output: List[Extractor] = []
        for feature in self._features:
            ref = feature.ref
            if ref is None:
                continue
            feature.ref_version = cast(int, feature.ref_version)
            if not isinstance(ref, Feature) and not isinstance(ref, Field):
                raise TypeError(
                    f"ref can either be of type Feature or Field but found : {type(ref)} for feature : {feature.name} "
                    f"in featureset : {self._name}"
                )

            # aliasing
            if isinstance(ref, Feature):
                extractor = Extractor(
                    f"_fennel_alias_{ref.fqn()}",
                    ExtractorType.ALIAS,
                    [ref],
                    [feature],
                    feature.ref_version,
                    None,
                    None,
                    None,
                    feature.ref_env,
                )
                extractor.outputs = [feature]
                extractor.inputs = [ref]
                extractor.featureset = self._name
                output.append(extractor)
                continue

            ref = cast(Field, ref)
            dataset = ref.dataset
            if dataset is None:
                raise ValueError(
                    f"Dataset `{ref.dataset_name}` not found for field `{ref}`"
                )
            index = get_index(dataset)
            if index is None:
                raise ValueError(
                    f"Please define either an offline or online index on dataset : {dataset._name} for extractor to work."
                )

            if not index.online and index.offline == IndexDuration.none:
                raise ValueError(
                    f"Please define either an offline or online index on dataset : {dataset._name} for extractor to work."
                )

            extractor = Extractor(
                f"_fennel_lookup_{ref.fqn()}",
                ExtractorType.LOOKUP,
                [],
                [feature],
                feature.ref_version,
                None,
                Extractor.DatasetLookupInfo(ref, feature.ref_default),
                [dataset] if dataset else [],
                feature.ref_env,
            )
            extractor.set_inputs_from_featureset(self, feature)
            extractor.featureset = self._name
            extractor.outputs = [feature]
            output.append(extractor)
        return output

    def _get_extractors(self) -> List[Extractor]:
        extractors = []

        # getting auto generated extractors for features
        extractors.extend(list(self._get_generated_extractors()))

        # user defined extractors
        for name, method in inspect.getmembers(self.__fennel_original_cls__):
            if not callable(method):
                continue
            if not hasattr(method, EXTRACTOR_ATTR):
                continue
            extractor = getattr(method, EXTRACTOR_ATTR)
            extractor = cast(Extractor, extractor)
            if len(extractor.user_defined_outputs) == 0:
                extractor.outputs = self._features
            else:
                # Converting the string extractor outputs decorator to Feature object
                outputs = []
                for i, output in enumerate(extractor.user_defined_outputs):
                    if isinstance(output, str):
                        try:
                            outputs.append(self._feature_map[output])
                        except KeyError:
                            raise ValueError(
                                f"When using strings in 'outputs' for an extractor, one can only choose from the features defined in the current featureset. "
                                f"Please choose an output from : {list(self._feature_map.keys())} found : `{output}` in extractor : `{extractor.name}`."
                            )
                    elif isinstance(output, Feature):
                        outputs.append(output)
                    else:
                        raise ValueError(
                            f"Extractor `{extractor.name}` can only return a "
                            f"set of features or strings, but found type {type(output)}."
                        )
                extractor.outputs = outputs

            # Converting the string in extractor inputs decorator to Feature object
            inputs = []
            for input in extractor.user_defined_inputs:
                if isinstance(input, str):
                    try:
                        inputs.append(self._feature_map[input])
                    except KeyError:
                        raise ValueError(
                            f"When using strings in 'inputs' for an extractor, one can only choose from the features defined in the current featureset. "
                            f"Please choose an input from : {list(self._feature_map.keys())} found : `{input}` in extractor : `{extractor.name}`."
                        )
                elif isinstance(input, Feature):
                    inputs.append(input)
                else:
                    raise ValueError(
                        f"Parameter `{input}` is not a feature, but a "
                        f"`{type(input)}`, and hence not supported as an input for the extractor "
                        f"`{extractor.name}`"
                    )
            extractor.featureset = self._name
            extractor.inputs = inputs
            func = _add_featureset_name(extractor.func, extractor.inputs)
            # Setting func at both extractor.func and class attribute
            extractor.func = func
            setattr(self.__fennel_original_cls__, name, classmethod(func))
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
        for extractor in self._extractors:
            if extractor.extractor_type == ExtractorType.ALIAS:
                feature = extractor.outputs[0]
                # Check that the types match
                if feature.dtype != extractor.inputs[0].dtype:
                    raise TypeError(
                        f"Feature `{feature.fqn()}` has type `{feature.dtype}` "
                        f"but the extractor aliasing `{extractor.inputs[0].fqn()}` has input type "
                        f"`{extractor.inputs[0].dtype}`."
                    )
            if extractor.extractor_type == ExtractorType.LOOKUP:
                feature = extractor.outputs[0]
                # Check that the types match
                field = extractor.derived_extractor_info.field
                default = extractor.derived_extractor_info.default
                if default is not None:
                    if field.is_optional():
                        field_dtype = fennel_get_optional_inner(field.dtype)
                    else:
                        field_dtype = field.dtype

                    if feature.dtype != field_dtype:
                        raise TypeError(
                            f"Feature `{feature.fqn()}` has type `{feature.dtype}` "
                            f"but expected type `{extractor.derived_extractor_info.field.dtype}`."
                        )
                else:
                    if field.is_optional():
                        # keeping with optional because default is not defined
                        expected_field_type = field.dtype
                    else:
                        # Adding optional because default is not defined
                        expected_field_type = Optional[field.dtype]

                    if feature.dtype != expected_field_type:
                        raise TypeError(
                            f"Feature `{feature.fqn()}` has type `{feature.dtype}` "
                            f"but expectected type `{expected_field_type}`"
                        )

                # Check that the default value has the right type
                if default is not None:
                    try:
                        validate_val_with_dtype(
                            field.dtype,
                            default,
                        )
                    except ValueError as e:
                        raise ValueError(
                            f"Default value `{default}` for feature `{feature.fqn()}` has incorrect default value: {e}"
                        )

        # Check that all features have unique names.
        feature_name_set = set()
        for feature in self._features:
            # Check features don't have protected names.
            if feature.name in RESERVED_FEATURE_NAMES:
                raise ValueError(
                    f"Feature `{feature.name}` in `{self._name}` has a "
                    f"reserved name `{feature.name}`."
                )
            if feature.name in feature_name_set:
                raise ValueError(
                    f"Feature `{feature.name}` has a duplicate in featureset `{self._name}`."
                )
            feature_name_set.add(feature.name)

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
    outputs: List[Feature]
    user_defined_outputs: List[Union[Feature, str]]
    func: Optional[Callable]
    derived_extractor_info: Optional[DatasetLookupInfo]
    featureset: str
    # depended on datasets: used for autogenerated extractors
    depends_on: List[Dataset]

    envs: EnvSelector

    def __init__(
        self,
        name: str,
        extractor_type: ExtractorType,
        user_defined_inputs: List[Union[Feature, str]],
        user_defined_outputs: List[Union[Feature, str]],
        version: int,
        func: Optional[Callable] = None,
        derived_extractor_info: Optional[DatasetLookupInfo] = None,
        depends_on: List[Dataset] = None,
        env: Optional[Union[str, List[str]]] = None,
    ):
        if depends_on is None:
            depends_on = []
        self.name = name
        self.extractor_type = extractor_type
        self.func = func  # type: ignore
        self.derived_extractor_info = derived_extractor_info
        self.user_defined_inputs = user_defined_inputs
        self.user_defined_outputs = user_defined_outputs
        self.inputs = []
        self.outputs = []
        self.version = version
        self.depends_on = depends_on
        self.envs = EnvSelector(env)

    def fqn(self) -> str:
        """Fully qualified name of the extractor."""
        return f"{self.featureset}.{self.name}"

    def fqn_output_features(self) -> List[str]:
        """Fully qualified name of the output features of this extractor."""
        return [f"{self.featureset}.{feature.name}" for feature in self.outputs]

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
