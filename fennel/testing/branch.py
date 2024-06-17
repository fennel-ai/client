import logging
import sys
import types
from dataclasses import dataclass, field
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
from fennel.internal_lib.graph_algorithms import (
    is_extractor_graph_cyclic,
)
from fennel.internal_lib.to_proto import (
    features_from_fs,
    featureset_to_proto,
    extractors_from_fs,
)
from fennel.testing.data_engine import DataEngine
from fennel.testing.test_utils import FakeResponse

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
    featureset_map: Dict[str, Featureset] = field(default_factory=dict)
    featureset_requests: Dict[str, CoreFeatureset] = field(default_factory=dict)
    features_for_fs: Dict[str, List[ProtoFeature]] = field(default_factory=dict)
    extractor_funcs: Dict[str, Callable] = field(default_factory=dict)
    extractors: List[Extractor] = field(default_factory=list)


class Branch:
    """
    Class for storing data and methods related to a branch:
    1. Dataset Definition
    2. Pandas dataframe wrt a dataset
    3. Entities like features, featuresets, extractors
    """

    def __init__(self, name: str):
        self.name = name
        self.data_engine = DataEngine()
        self.entities: Entities = Entities()

    def get_entities(self) -> Entities:
        """
        Get entities apart datasets.
        """
        return self.entities

    def get_data_engine(self) -> DataEngine:
        """
        Get definitions and data related to datasets.
        """
        return self.data_engine

    def get_dataset_df(self, dataset_name: str) -> pd.DataFrame:
        """
        Get pandas dataframe corresponding to a dataset
        Args:
            dataset_name: (str) - Name of the dataset
        Returns:
            Pandas dataframe
        """
        return self.data_engine.get_dataset_df(dataset_name)

    def get_datasets(self) -> List[Dataset]:
        return self.data_engine.get_datasets()

    def get_featuresets(self) -> List[Featureset]:
        return list(self.entities.featureset_map.values())

    def get_features_from_fs(self, fs_name: str) -> List[str]:
        return [f.fqn() for f in self.entities.featureset_map[fs_name].features]

    def log(
        self,
        webhook: str,
        endpoint: str,
        df: pd.DataFrame,
        _batch_size: int = 1000,
    ) -> FakeResponse:
        return self.data_engine.log(webhook, endpoint, df, _batch_size)

    def log_to_dataset(
        self,
        dataset: Dataset,
        df: pd.DataFrame,
    ) -> FakeResponse:
        return self.data_engine.log_to_dataset(dataset, df)

    def commit(
        self,
        datasets: Optional[List[Dataset]] = None,
        featuresets: Optional[List[Featureset]] = None,
        preview=False,
        incremental=False,
        env: Optional[str] = None,
    ):
        if not incremental:
            self._reset()
        if datasets is None:
            datasets = []
        if featuresets is None:
            featuresets = []

        self.data_engine.add_datasets(datasets, incremental, env)

        for featureset in featuresets:
            self.entities.features_for_fs[featureset._name] = features_from_fs(
                featureset
            )
            self.entities.featureset_requests[featureset._name] = (
                featureset_to_proto(featureset)
            )
            self.entities.featureset_map[featureset._name] = featureset
            # Check if the dataset used by the extractor is registered
            for extractor in featureset.extractors:
                if not extractor.envs.is_entity_selected(env):
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
                list(
                    filter(
                        lambda x: x.envs.is_entity_selected(env),
                        featureset.extractors,
                    )
                )
            )
        fs_obj_map = {
            featureset._name: featureset for featureset in featuresets
        }

        for featureset in featuresets:
            proto_extractors = extractors_from_fs(featureset, fs_obj_map, env)
            for extractor in proto_extractors:
                if extractor.extractor_type != ProtoExtractorType.PY_FUNC:
                    continue
                extractor_fqn = f"{featureset._name}.{extractor.name}"
                self.entities.extractor_funcs[extractor_fqn] = (
                    get_extractor_func(extractor)
                )

        if is_extractor_graph_cyclic(self.entities.extractors):
            raise Exception("Cyclic graph detected in extractors")
        return FakeResponse(200, "OK")

    def _reset(self):
        self.data_engine = DataEngine()
        self.entities: Entities = Entities()
