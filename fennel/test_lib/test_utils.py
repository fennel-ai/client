from typing import Any

import jsondiff  # type: ignore
from google.protobuf.json_format import MessageToDict  # type: ignore

import fennel.gen.dataset_pb2 as ds_proto
import fennel.gen.featureset_pb2 as fs_proto


def error_message(actual: Any, expected: Any) -> str:
    expected_dict = MessageToDict(expected)
    actual_dict = MessageToDict(actual)
    # Don't delete the following line. It is used to debug the test in
    # case of failure.
    print(actual_dict)
    return jsondiff.diff(expected_dict, actual_dict, syntax="symmetric")


def clean_fs_func_src_code(featureset_req: fs_proto.CreateFeaturesetRequest):
    extractors = []
    for extractor in featureset_req.extractors:
        extractor.func_source_code = ""
        extractor.func = b""
        extractors.append(extractor)
    return fs_proto.CreateFeaturesetRequest(
        name=featureset_req.name,
        features=featureset_req.features,
        extractors=extractors,
        metadata=featureset_req.metadata,
    )


def clean_ds_func_src_code(
    dataset_req: ds_proto.CreateDatasetRequest,
) -> ds_proto.CreateDatasetRequest:
    def cleanup_node(node):
        if node.HasField("operator") and node.operator.HasField("transform"):
            return ds_proto.Node(
                operator=ds_proto.Operator(
                    transform=ds_proto.Transform(
                        operand_node_id=node.operator.transform.operand_node_id,
                    ),
                ),
                id=node.id,
            )
        if node.HasField("operator") and node.operator.HasField("filter"):
            return ds_proto.Node(
                operator=ds_proto.Operator(
                    filter=ds_proto.Filter(
                        operand_node_id=node.operator.transform.operand_node_id,
                    ),
                ),
                id=node.id,
            )
        return node

    dataset_req.on_demand.function_source_code = ""
    dataset_req.on_demand.function = b""
    pipelines = []
    for j in range(len(dataset_req.pipelines)):
        pipelines.append(
            ds_proto.Pipeline(
                root=dataset_req.pipelines[j].root,
                nodes=[cleanup_node(n) for n in dataset_req.pipelines[j].nodes],
                signature=dataset_req.pipelines[j].signature,
                inputs=dataset_req.pipelines[j].inputs,
            )
        )
    return ds_proto.CreateDatasetRequest(
        name=dataset_req.name,
        fields=dataset_req.fields,
        max_staleness=dataset_req.max_staleness,
        retention=dataset_req.retention,
        mode=dataset_req.mode,
        pipelines=pipelines,
        on_demand=dataset_req.on_demand,
        metadata=dataset_req.metadata,
    )
