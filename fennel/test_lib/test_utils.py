from typing import Any

from fennel._vendor import jsondiff  # type: ignore
from google.protobuf.json_format import MessageToDict  # type: ignore

from fennel.gen.dataset_pb2 import Operator, Filter, Transform
from fennel.gen.featureset_pb2 import Extractor
from fennel.gen.pycode_pb2 import PyCode


def error_message(actual: Any, expected: Any) -> str:
    expected_dict = MessageToDict(expected)
    actual_dict = MessageToDict(actual)
    # Don't delete the following line. It is used to debug the test in
    # case of failure.
    print(actual_dict)
    return jsondiff.diff(expected_dict, actual_dict, syntax="symmetric")


def erase_extractor_pycode(extractor: Extractor) -> Extractor:
    new_extractor = Extractor(
        name=extractor.name,
        version=extractor.version,
        datasets=extractor.datasets,
        inputs=extractor.inputs,
        features=extractor.features,
        metadata=extractor.metadata,
        feature_set_name=extractor.feature_set_name,
        pycode=PyCode(),
    )
    return new_extractor


def erase_operator_pycode(operator: Operator) -> Operator:
    if operator.HasField("filter"):
        return Operator(
            id=operator.id,
            name=operator.name,
            pipeline_name=operator.pipeline_name,
            dataset_name=operator.dataset_name,
            is_root=operator.is_root,
            filter=Filter(
                operand_id=operator.filter.operand_id,
                pycode=PyCode(source_code=""),
            ),
        )
    if operator.HasField("transform"):
        return Operator(
            id=operator.id,
            name=operator.name,
            pipeline_name=operator.pipeline_name,
            dataset_name=operator.dataset_name,
            is_root=operator.is_root,
            transform=Transform(
                operand_id=operator.transform.operand_id,
                schema=operator.transform.schema,
                pycode=PyCode(source_code=""),
            ),
        )
    raise ValueError(f"Operator {operator} has no pycode field")
