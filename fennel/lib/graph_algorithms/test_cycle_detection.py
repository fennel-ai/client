from datetime import datetime

import pandas as pd
import pytest

from fennel.featuresets import featureset, extractor, feature
from fennel.lib.graph_algorithms import is_extractor_graph_cyclic
from fennel.lib.schema import inputs, outputs

Series = pd.Series


@featureset
class A:
    a1: int = feature(id=1)
    a2: int = feature(id=2)
    a3: int = feature(id=3)
    a4: int = feature(id=4)

    @extractor
    @inputs(datetime, a1)
    @outputs(a2, a4)
    def a1_a2(cls, ts: Series, f: Series):
        pass

    @extractor
    @inputs(datetime, a2, a4)
    @outputs(a3)
    def a2_a3(cls, ts: Series, f: Series, f2: Series):
        pass

    @extractor
    @inputs(datetime, a3)
    @outputs(a1)
    def a3_a1(cls, ts: Series, f: Series):
        pass


def test_simple_cycle_detection():
    with pytest.raises(ValueError) as e:
        is_extractor_graph_cyclic(A.extractors)
    assert str(e.value) == "Cyclic dependency found for A.a2_a3"
