import logging
import sys
import types
from dataclasses import dataclass
from typing import Optional, List, Dict, Callable

import pandas as pd

from fennel.datasets.datasets import Dataset
from fennel.featuresets.featureset import (
    Featureset,
    Extractor,
)
from fennel.gen.featureset_pb2 import (
    CoreFeatureset,
    Feature as ProtoFeature,
    Extractor as ProtoExtractor,
    ExtractorType as ProtoExtractorType,
)
from fennel.lib.graph_algorithms import (
    is_extractor_graph_cyclic,
)
from fennel.lib.to_proto import (
    features_from_fs,
    featureset_to_proto,
    extractors_from_fs,
)
from fennel.test_lib.data_engine import DataEngine
from fennel.test_lib.test_utils import FakeResponse


logger = logging.getLogger(__name__)


def get_extractor_func(extractor_proto: ProtoExtractor) -> Callable:
    fqn = f"{extractor_proto.feature_set_name}.{extractor_proto.name}"
    mod = types.ModuleType(fqn)
    code = (
        extractor_proto.pycode.imports + extractor_proto.pycode.generated_code
    )
    try:
        sys.modules[fqn] = mod
        exec(code, mod.__dict__)
    except Exception as e:
        raise Exception(
            f"Error while executing code for {fqn}:\n {code} \n: {str(e)}"
        )
    return mod.__dict__[extractor_proto.pycode.entry_point]


@dataclass
class Entities:
    featureset_requests: Dict[str, CoreFeatureset]
    features_for_fs: Dict[str, List[ProtoFeature]]
    extractor_funcs: Dict[str, Callable]
    extractors: List[Extractor]


class Branch:
    def __init__(self, name: str):
        self.name = name
        self.data_engine = DataEngine()
        self.entities: Entities = Entities(
            featureset_requests={},
            features_for_fs={},
            extractor_funcs={},
            extractors=[],
        )

    def get_entities(self) -> Entities:
        return self.entities

    def list_datasets(self):
        return self.data_engine.list_datasets()

    def list_featuresets(self):
        return self.entities.featureset_requests

    def get_data_engine(self) -> DataEngine:
        return self.data_engine

    def get_dataset_df(self, dataset_name: str) -> pd.DataFrame:
        return self.data_engine.get_dataset_df(dataset_name)

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        _batch_size: int = 1000,
    ) -> FakeResponse:
        return self.data_engine.log(webhook, endpoint, df, _batch_size)

    def sync(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        tier: Optional[str] = None,
    ):
        self._reset()
        if datasets is None:
            datasets = []
        if featuresets is None:
            featuresets = []

        self.data_engine.add_datasets(datasets, tier)

        for featureset in featuresets:
            if not isinstance(featureset, Featureset):
                raise TypeError(
                    f"Expected a list of featuresets, got `{featureset.__name__}`"
                    f" of type `{type(featureset)}` instead."
                )
            self.entities.features_for_fs[featureset._name] = features_from_fs(
                featureset
            )
            self.entities.featureset_requests[
                featureset._name
            ] = featureset_to_proto(featureset)
            # Check if the dataset used by the extractor is registered
            for extractor in featureset.extractors:
                if not extractor.tiers.is_entity_selected(tier):
                    continue
                datasets = [
                    x._name for x in extractor.get_dataset_dependencies()
                ]
                for dataset in datasets:
                    if dataset not in self.data_engine.get_dataset_names():
                        raise ValueError(
                            f"Dataset `{dataset}` not found in sync call"
                        )
            self.entities.extractors.extend(
                [
                    x
                    for x in featureset.extractors
                    if x.tiers.is_entity_selected(tier)
                ]
            )
        fs_obj_map = {
            featureset._name: featureset for featureset in featuresets
        }

        for featureset in featuresets:
            proto_extractors = extractors_from_fs(featureset, fs_obj_map, tier)
            for extractor in proto_extractors:
                if extractor.extractor_type != ProtoExtractorType.PY_FUNC:
                    continue
                extractor_fqn = f"{featureset._name}.{extractor.name}"
                self.entities.extractor_funcs[
                    extractor_fqn
                ] = get_extractor_func(extractor)

        if is_extractor_graph_cyclic(self.entities.extractors):
            raise Exception("Cyclic graph detected in extractors")
        return FakeResponse(200, "OK")

    def _reset(self):
        self.data_engine = DataEngine()
        self.entities: Entities = Entities(
            featureset_requests={},
            features_for_fs={},
            extractor_funcs={},
            extractors=[],
        )
