from typing import Any

import jsondiff
from google.protobuf.json_format import MessageToDict

import fennel.gen.dataset_pb2 as ds_proto
import fennel.gen.featureset_pb2 as fs_proto


def error_message(actual: Any, expected: Any) -> str:
    expected_dict = MessageToDict(expected)
    actual_dict = MessageToDict(actual)
    print(actual_dict)
    return jsondiff.diff(expected_dict, actual_dict, syntax='symmetric')


def clean_fs_func_src_code(featureset_req: fs_proto.CreateFeaturesetRequest):
    extractors = []
    for extractor in featureset_req.extractors:
        extractor.func_source_code = ''
        extractor.func = b''
        extractors.append(extractor)
    return fs_proto.CreateFeaturesetRequest(
        name=featureset_req.name,
        owner=featureset_req.owner,
        description=featureset_req.description,
        features=featureset_req.features,
        extractors=extractors,
    )


def clean_ds_func_src_code(dataset_req: ds_proto.CreateDatasetRequest) -> \
        ds_proto.CreateDatasetRequest:
    def cleanup_node(node):
        if node.HasField('operator') and node.operator.HasField('transform'):
            return ds_proto.Node(operator=ds_proto.Operator(
                transform=ds_proto.Transform(
                    node=cleanup_node(node.operator.transform.node),
                    timestamp_field=node.operator.transform.timestamp_field,
                ), id=node.operator.id))
        elif node.HasField('operator') and node.operator.HasField('aggregate'):
            return ds_proto.Node(operator=ds_proto.Operator(
                aggregate=ds_proto.Aggregate(node=cleanup_node(
                    node.operator.aggregate.node),
                    keys=node.operator.aggregate.keys,
                    aggregates=node.operator.aggregate.aggregates),
                id=node.operator.id))
        elif node.HasField('operator') and node.operator.HasField('join'):
            return ds_proto.Node(operator=ds_proto.Operator(
                join=ds_proto.Join(node=cleanup_node(
                    node.operator.join.node),
                    dataset=node.operator.join.dataset,
                    on=node.operator.join.on),
                id=node.operator.id))
        elif node.HasField('operator') and node.operator.HasField('union'):
            return ds_proto.Node(operator=ds_proto.Operator(
                union=ds_proto.Union(nodes=[cleanup_node(n) for n in
                                            node.operator.union.nodes]),
                id=node.operator.id))

        return node

    dataset_req.pull_lookup.function_source_code = ''
    dataset_req.pull_lookup.function = b''
    pipelines = []
    for j in range(len(dataset_req.pipelines)):
        pipelines.append(ds_proto.Pipeline(
            root=cleanup_node(dataset_req.pipelines[j].root),
            nodes=[cleanup_node(n) for n in dataset_req.pipelines[j].nodes],
            signature=dataset_req.pipelines[
                j].signature,
            inputs=dataset_req.pipelines[j].inputs,
        ))
    return ds_proto.CreateDatasetRequest(
        name=dataset_req.name,
        fields=dataset_req.fields,
        max_staleness=dataset_req.max_staleness,
        retention=dataset_req.retention,
        mode=dataset_req.mode,
        pipelines=pipelines,
        schema=b'',
        pull_lookup=dataset_req.pull_lookup,
    )
