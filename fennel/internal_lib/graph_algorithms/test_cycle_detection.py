import pandas as pd
import pytest

from fennel.featuresets import featureset, extractor, feature as F
from fennel.internal_lib.graph_algorithms import is_extractor_graph_cyclic
from fennel.lib import inputs, outputs


@featureset
class A:
    a1: int
    a2: int
    a3: int
    a4: int

    @extractor
    @inputs("a1")
    @outputs("a2", "a4")
    def a1_a2(cls, ts: pd.Series, f: pd.Series):
        pass

    @extractor
    @inputs("a2", "a4")
    @outputs("a3")
    def a2_a3(cls, ts: pd.Series, f: pd.Series, f2: pd.Series):
        pass

    @extractor
    @inputs("a3")
    @outputs("a1")
    def a3_a1(cls, ts: pd.Series, f: pd.Series):
        pass


def test_simple_cycle_detection():
    with pytest.raises(ValueError) as e:
        is_extractor_graph_cyclic(A.extractors)
    assert str(e.value) == "Cyclic dependency found for A.a2_a3"
