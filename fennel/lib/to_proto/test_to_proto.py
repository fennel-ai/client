from datetime import datetime

from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, feature, extractor
from fennel.lib.includes import includes
from fennel.lib.schema import outputs
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

    @extractor(depends_on=[TestDataset])
    @includes(A, B, C)
    @outputs(f2, f3)
    def test_extractor(cls, ts):
        pass


def test_includes():
    f = {
        "entryPoint": "TestFeatureset.test_extractor",
        "sourceCode": "@extractor(depends_on=[TestDataset])\n@includes(A, B, C)\n@outputs(f2, f3)\ndef test_extractor(cls, ts):\n    pass\n",
        "coreCode": "@extractor(depends_on=[TestDataset])\n@includes(A, B, C)\n@outputs(f2, f3)\ndef test_extractor(cls, ts):\n    pass\n",
        "generatedCode": "\nfrom datetime import datetime\nimport pandas as pd\nimport numpy as np\nimport functools\nfrom typing import List, Dict, Tuple, Optional, Union, Any, no_type_check\nfrom fennel.lib.metadata import meta\nfrom fennel.lib.includes import includes\nfrom fennel.datasets import *\nfrom fennel.featuresets import *\nfrom fennel.lib.schema import *\nfrom fennel.datasets.datasets import dataset_lookup\n\ndef a1():\n    return 1\n\ndef b1():\n    return 2\n\n@includes(a1, b1)\ndef A():\n    return 11\n\ndef B():\n    return 22\n\ndef c1():\n    return 3\n\n@includes(c1)\ndef C():\n    return 33\n\n\n@dataset\nclass TestDataset:\n    a1: int = field(key=True)\n    t: datetime = field(timestamp=True)\n\n\n@featureset\nclass TestFeatureset:\n    f1: int = feature(id=1)\n    f2: int = feature(id=2)\n    f3: int = feature(id=3)\n\n    @extractor(depends_on=[TestDataset])\n    @includes(A, B, C)\n    @outputs(f2, f3)\n    def test_extractor(cls, ts):\n        pass\n",
        "includes": [
            {
                "entryPoint": "A",
                "sourceCode": "@includes(a1, b1)\ndef A():\n    return 11\n",
                "coreCode": "@includes(a1, b1)\ndef A():\n    return 11\n",
                "generatedCode": "@includes(a1, b1)\ndef A():\n    return 11\n",
                "includes": [
                    {
                        "entryPoint": "a1",
                        "sourceCode": "def a1():\n    return 1\n",
                        "coreCode": "def a1():\n    return 1\n",
                        "generatedCode": "def a1():\n    return 1\n",
                    },
                    {
                        "entryPoint": "b1",
                        "sourceCode": "def b1():\n    return 2\n",
                        "coreCode": "def b1():\n    return 2\n",
                        "generatedCode": "def b1():\n    return 2\n",
                    },
                ],
            },
            {
                "entryPoint": "B",
                "sourceCode": "def B():\n    return 22\n",
                "coreCode": "def B():\n    return 22\n",
                "generatedCode": "def B():\n    return 22\n",
            },
            {
                "entryPoint": "C",
                "sourceCode": "@includes(c1)\ndef C():\n    return 33\n",
                "coreCode": "@includes(c1)\ndef C():\n    return 33\n",
                "generatedCode": "@includes(c1)\ndef C():\n    return 33\n",
                "includes": [
                    {
                        "entryPoint": "c1",
                        "sourceCode": "def c1():\n    return 3\n",
                        "coreCode": "def c1():\n    return 3\n",
                        "generatedCode": "def c1():\n    return 3\n",
                    }
                ],
            },
        ],
        "refIncludes": {
            "TestDataset": "Dataset",
            "TestFeatureset": "Featureset",
        },
    }
    TestFeatureset.extractors[0]
    includes_proto = to_extractor_pycode(
        TestFeatureset.extractors[0], TestFeatureset, {}
    )
    # Delete datasets from proto
    expected_extractor = ParseDict(f, pycode_proto.PyCode())
    assert includes_proto == expected_extractor, error_message(
        includes_proto, expected_extractor
    )
