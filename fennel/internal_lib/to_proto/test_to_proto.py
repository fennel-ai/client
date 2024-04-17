from datetime import datetime

from google.protobuf.json_format import ParseDict  # type: ignore

import fennel.gen.pycode_pb2 as pycode_proto
from fennel.datasets import dataset, field
from fennel.featuresets import featureset, extractor
from fennel.internal_lib.to_proto.to_proto import to_extractor_pycode
from fennel.lib import includes, outputs
from fennel.testing import *


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


@dataset(index=True)
class TestDataset:
    a1: int = field(key=True)
    t: datetime = field(timestamp=True)


@featureset
class TestFeatureset:
    f1: int
    f2: int
    f3: int

    @extractor(deps=[TestDataset])  # type: ignore
    @includes(A, B, C)
    @outputs("f2", "f3")
    def test_extractor(cls, ts):
        pass


def rm_imports(pycode: pycode_proto.PyCode) -> pycode_proto.PyCode:
    pycode.imports = ""
    for child in pycode.includes:
        rm_imports(child)
    return pycode


def test_includes():
    f = {
        "entryPoint": "TestFeatureset_test_extractor",
        "source_code": '@extractor(deps=[TestDataset])  # type: ignore\n@includes(A, B, C)\n@outputs("f2", "f3")\ndef test_extractor(cls, ts):\n    pass\n',
        "core_code": '@extractor(deps=[TestDataset])  # type: ignore\n@includes(A, B, C)\n@outputs("f2", "f3")\ndef test_extractor(cls, ts):\n    pass\n',
        "generated_code": '\n\n\n\n\ndef b1():\n    return 2\n\n\n\ndef a1():\n    return 1\n\n\n\n@includes(a1, b1)\ndef A():\n    return 11\n\n\n\ndef B():\n    return 22\n\n\n\ndef c1():\n    return 3\n\n\n\n@includes(c1)\ndef C():\n    return 33\n\n\n@dataset(index=True)\nclass TestDataset:\n    a1: int = field(key=True)\n    t: datetime = field(timestamp=True)\n\n\n@featureset\nclass TestFeatureset:\n    f1: int\n    f2: int\n    f3: int\n\n    @extractor(deps=[TestDataset])  # type: ignore\n    @includes(A, B, C)\n    @outputs("f2", "f3")\n    def test_extractor(cls, ts):\n        pass\n\ndef TestFeatureset_test_extractor(*args, **kwargs):\n    x = TestFeatureset.__fennel_original_cls__\n    return getattr(x, "test_extractor")(*args, **kwargs)\n    ',
        "includes": [
            {
                "entryPoint": "A",
                "sourceCode": "@includes(a1, b1)\ndef A():\n    return 11\n",
                "coreCode": "@includes(a1, b1)\ndef A():\n    return 11\n",
                "generatedCode": "\ndef b1():\n    return 2\n\n\n\ndef a1():\n    return 1\n\n\n\n@includes(a1, b1)\ndef A():\n    return 11\n\n",
                "includes": [
                    {
                        "entryPoint": "a1",
                        "sourceCode": "def a1():\n    return 1\n",
                        "coreCode": "def a1():\n    return 1\n",
                        "generatedCode": "\ndef a1():\n    return 1\n\n",
                    },
                    {
                        "entryPoint": "b1",
                        "sourceCode": "def b1():\n    return 2\n",
                        "coreCode": "def b1():\n    return 2\n",
                        "generatedCode": "\ndef b1():\n    return 2\n\n",
                    },
                ],
            },
            {
                "entryPoint": "B",
                "sourceCode": "def B():\n    return 22\n",
                "coreCode": "def B():\n    return 22\n",
                "generatedCode": "\ndef B():\n    return 22\n\n",
            },
            {
                "entryPoint": "C",
                "sourceCode": "@includes(c1)\ndef C():\n    return 33\n",
                "coreCode": "@includes(c1)\ndef C():\n    return 33\n",
                "generatedCode": "\ndef c1():\n    return 3\n\n\n\n@includes(c1)\ndef C():\n    return 33\n\n",
                "includes": [
                    {
                        "entryPoint": "c1",
                        "sourceCode": "def c1():\n    return 3\n",
                        "coreCode": "def c1():\n    return 3\n",
                        "generatedCode": "\ndef c1():\n    return 3\n\n",
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
        TestFeatureset.extractors[0],
        TestFeatureset,
        {"TestFeatureset": TestFeatureset},
    )
    expected_extractor = rm_imports(ParseDict(f, pycode_proto.PyCode()))
    includes_proto = rm_imports(includes_proto)
    assert includes_proto == expected_extractor, error_message(
        includes_proto, expected_extractor
    )
