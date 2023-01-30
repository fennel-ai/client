from datetime import datetime

import pytest

from fennel.featuresets import featureset, extractor, feature
from fennel.lib.graph_algorithms import is_extractor_graph_cyclic
from fennel.lib.schema import Series, DataFrame


@featureset
class A:
    a1: int = feature(id=1)
    a2: int = feature(id=2)
    a3: int = feature(id=3)
    a4: int = feature(id=4)

    @extractor
    def a1_a2(cls, ts: Series[datetime], f: Series[a1]) -> DataFrame[a2, a4]:
        pass

    @extractor
    def a2_a3(
        cls, ts: Series[datetime], f: Series[a2], f2: Series[a4]
    ) -> Series[a3]:
        pass

    @extractor
    def a3_a1(cls, ts: Series[datetime], f: Series[a3]) -> Series[a1]:
        pass


def test_simple_cycle_detection():
    with pytest.raises(ValueError) as e:
        is_extractor_graph_cyclic(A.extractors)
    assert str(e.value) == "Cyclic dependency found for A.a2_a3"
