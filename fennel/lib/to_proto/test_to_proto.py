from datetime import datetime

from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature, extractor, depends_on
from fennel.lib.include_mod import includes
from fennel.lib.to_proto.to_proto import to_extractor_pycode
from fennel.test_lib import *


def a1():
    return 1


def b1():
    return 2


@includes(a1, b1)
def A():
    return 11


def B():
    return 22


def c1():
    return 3


@includes(c1)
def C():
    return 33


@dataset
class TestDataset:
    a1: int = field(key=True)
    t: datetime = field(timestamp=True)


@featureset
class TestFeatureset:
    f1: int = feature(id=1)
    f2: int = feature(id=2)
    f3: int = feature(id=3)

    @extractor
    @depends_on(TestDataset)
    @includes(A, B, C)
    def test_extractor(cls):
        pass


def test_includes():
    f = {
        'sourceCode': '\n\n\ndef test_extractor(cls):\n    pass\n',
        'extractorName': 'test_extractor',
        'includes': [
            {
                'sourceCode': '\ndef A():\n    return 11\n',
                'name': 'A',
                'includes': [
                    {
                        'sourceCode': 'def a1():\n    return 1\n',
                        'name': 'a1'
                    },
                    {
                        'sourceCode': 'def b1():\n    return 2\n',
                        'name': 'b1'
                    }
                ]
            },
            {
                'sourceCode': 'def B():\n    return 22\n',
                'name': 'B'
            },
            {
                'sourceCode': '\ndef C():\n    return 33\n',
                'name': 'C',
                'includes': [
                    {
                        'sourceCode': 'def c1():\n    return 3\n',
                        'name': 'c1'
                    }
                ]
            }
        ],
        'datasetNames': [
            'TestDataset'
        ],
        'featuresetCode': '\nclass TestFeatureset:\n    f1: int = feature(id=1)\n    f2: int = feature(id=2)\n    f3: int = feature(id=3)\n\n\n\n    def test_extractor(cls):\n        pass\n\n    @classproperty\n    def f1(cls):\n        return "f1"\n\n\n\n    @classproperty\n    def f2(cls):\n        return "f2"\n\n\n\n    @classproperty\n    def f3(cls):\n        return "f3"\n\n\n',
        'imports': '\nfrom datetime import datetime\nimport pandas as pd\nimport numpy as np\nfrom typing import List, Dict, Tuple, Optional, Union, Any\nfrom fennel.featuresets import *\nfrom fennel.lib.metadata import meta\nfrom fennel.lib.include_mod import includes\nfrom fennel.datasets import *\nfrom fennel.lib.schema import *\nfrom fennel.datasets.datasets import dataset_lookup\nclass classproperty(object):\n    def __init__(self, f):\n        self.f = classmethod(f)\n    def __get__(self, *a):\n        return self.f.__get__(*a)()\n'
    }
    TestFeatureset.extractors[0]
    includes_proto = to_extractor_pycode(
        TestFeatureset.extractors[0], TestFeatureset
    )
    # Delete datasets from proto
    includes_proto.dataset_codes[:] = []
    expected_extractor = ParseDict(f, pycode_proto.ExtractorPyCode())
    assert includes_proto == expected_extractor, error_message(
        includes_proto, expected_extractor
    )
