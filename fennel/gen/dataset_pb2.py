# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: dataset.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.status_pb2 as status__pb2
import fennel.gen.source_pb2 as source__pb2
import fennel.gen.metadata_pb2 as metadata__pb2
import fennel.gen.schema_pb2 as schema__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rdataset.proto\x12\x0c\x66\x65nnel.proto\x1a\x0cstatus.proto\x1a\x0csource.proto\x1a\x0emetadata.proto\x1a\x0cschema.proto\"\x8c\x01\n\x05\x46ield\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0e\n\x06is_key\x18\x02 \x01(\x08\x12\x14\n\x0cis_timestamp\x18\x03 \x01(\x08\x12%\n\x05\x64type\x18\x04 \x01(\x0b\x32\x16.fennel.proto.DataType\x12(\n\x08metadata\x18\x05 \x01(\x0b\x32\x16.fennel.proto.Metadata\"Q\n\x08OnDemand\x12\x1c\n\x14\x66unction_source_code\x18\x01 \x01(\t\x12\x10\n\x08\x66unction\x18\x02 \x01(\x0c\x12\x15\n\rexpires_after\x18\x03 \x01(\x03\"\xa4\x03\n\x14\x43reateDatasetRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12#\n\x06\x66ields\x18\x02 \x03(\x0b\x32\x13.fennel.proto.Field\x12)\n\tpipelines\x18\x03 \x03(\x0b\x32\x16.fennel.proto.Pipeline\x12\x35\n\x10input_connectors\x18\x04 \x03(\x0b\x32\x1b.fennel.proto.DataConnector\x12\x36\n\x11output_connectors\x18\x05 \x03(\x0b\x32\x1b.fennel.proto.DataConnector\x12\x11\n\tsignature\x18\x06 \x01(\t\x12(\n\x08metadata\x18\x07 \x01(\x0b\x32\x16.fennel.proto.Metadata\x12\x0c\n\x04mode\x18\x08 \x01(\t\x12\x0f\n\x07version\x18\t \x01(\r\x12\x0e\n\x06schema\x18\n \x01(\x0c\x12\x11\n\tretention\x18\x0b \x01(\x03\x12\x15\n\rmax_staleness\x18\x0c \x01(\x03\x12)\n\ton_demand\x18\r \x01(\x0b\x32\x16.fennel.proto.OnDemand\"K\n\x15\x43reateDatasetResponse\x12\x0c\n\x04name\x18\x01 \x01(\t\x12$\n\x06status\x18\x02 \x01(\x0b\x32\x14.fennel.proto.Status\"\x88\x01\n\x08Pipeline\x12!\n\x05nodes\x18\x01 \x03(\x0b\x32\x12.fennel.proto.Node\x12\x0c\n\x04root\x18\x02 \x01(\t\x12\x11\n\tsignature\x18\x03 \x01(\t\x12(\n\x08metadata\x18\x04 \x01(\x0b\x32\x16.fennel.proto.Metadata\x12\x0e\n\x06inputs\x18\x05 \x03(\t\"^\n\x04Node\x12\n\n\x02id\x18\x01 \x01(\t\x12*\n\x08operator\x18\x02 \x01(\x0b\x32\x16.fennel.proto.OperatorH\x00\x12\x16\n\x0c\x64\x61taset_name\x18\x03 \x01(\tH\x00\x42\x06\n\x04node\"\xde\x01\n\x08Operator\x12,\n\taggregate\x18\x01 \x01(\x0b\x32\x17.fennel.proto.AggregateH\x00\x12\"\n\x04join\x18\x02 \x01(\x0b\x32\x12.fennel.proto.JoinH\x00\x12,\n\ttransform\x18\x03 \x01(\x0b\x32\x17.fennel.proto.TransformH\x00\x12$\n\x05union\x18\x04 \x01(\x0b\x32\x13.fennel.proto.UnionH\x00\x12&\n\x06\x66ilter\x18\x05 \x01(\x0b\x32\x14.fennel.proto.FilterH\x00\x42\x04\n\x02op\"a\n\tAggregate\x12\x17\n\x0foperand_node_id\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12-\n\naggregates\x18\x03 \x03(\x0b\x32\x19.fennel.proto.Aggregation\"\x88\x01\n\x04Join\x12\x13\n\x0blhs_node_id\x18\x01 \x01(\t\x12\x18\n\x10rhs_dataset_name\x18\x02 \x01(\t\x12&\n\x02on\x18\x03 \x03(\x0b\x32\x1a.fennel.proto.Join.OnEntry\x1a)\n\x07OnEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"T\n\tTransform\x12\x17\n\x0foperand_node_id\x18\x01 \x01(\t\x12\x10\n\x08\x66unction\x18\x02 \x01(\x0c\x12\x1c\n\x14\x66unction_source_code\x18\x03 \x01(\t\"Q\n\x06\x46ilter\x12\x17\n\x0foperand_node_id\x18\x01 \x01(\t\x12\x10\n\x08\x66unction\x18\x02 \x01(\x0c\x12\x1c\n\x14\x66unction_source_code\x18\x03 \x01(\t\"!\n\x05Union\x12\x18\n\x10operand_node_ids\x18\x01 \x03(\t\"\xd8\x01\n\x0b\x41ggregation\x12)\n\x04type\x18\x01 \x01(\x0e\x32\x1b.fennel.proto.AggregateType\x12-\n\x0bwindow_spec\x18\x02 \x01(\x0b\x32\x18.fennel.proto.WindowSpec\x12\x15\n\x0bvalue_field\x18\x03 \x01(\tH\x00\x12(\n\x04topk\x18\x04 \x01(\x0b\x32\x18.fennel.proto.TopKConfigH\x00\x12$\n\x02\x63\x66\x18\x05 \x01(\x0b\x32\x16.fennel.proto.CFConfigH\x00\x42\x08\n\x06\x63onfig\"$\n\x06Window\x12\r\n\x05start\x18\x01 \x01(\x03\x12\x0b\n\x03\x65nd\x18\x02 \x01(\x03\"[\n\x0b\x44\x65ltaWindow\x12&\n\x08\x62\x61seline\x18\x01 \x01(\x0b\x32\x14.fennel.proto.Window\x12$\n\x06target\x18\x02 \x01(\x0b\x32\x14.fennel.proto.Window\"\x89\x01\n\nWindowSpec\x12\x18\n\x0e\x66orever_window\x18\x01 \x01(\x08H\x00\x12&\n\x06window\x18\x02 \x01(\x0b\x32\x14.fennel.proto.WindowH\x00\x12\x31\n\x0c\x64\x65lta_window\x18\x03 \x01(\x0b\x32\x19.fennel.proto.DeltaWindowH\x00\x42\x06\n\x04spec\"A\n\nTopKConfig\x12\t\n\x01k\x18\x01 \x01(\x05\x12\x13\n\x0bitem_fields\x18\x02 \x03(\t\x12\x13\n\x0bscore_field\x18\x03 \x01(\t\"G\n\x08\x43\x46\x43onfig\x12\r\n\x05limit\x18\x01 \x01(\x05\x12\x16\n\x0e\x63ontext_fields\x18\x02 \x03(\t\x12\x14\n\x0cweight_field\x18\x03 \x01(\t*P\n\rAggregateType\x12\x07\n\x03SUM\x10\x00\x12\x07\n\x03\x41VG\x10\x01\x12\t\n\x05\x43OUNT\x10\x02\x12\x07\n\x03MIN\x10\x03\x12\x07\n\x03MAX\x10\x04\x12\x08\n\x04TOPK\x10\x05\x12\x06\n\x02\x43\x46\x10\x06\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'dataset_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _JOIN_ONENTRY._options = None
  _JOIN_ONENTRY._serialized_options = b'8\001'
  _AGGREGATETYPE._serialized_start=2347
  _AGGREGATETYPE._serialized_end=2427
  _FIELD._serialized_start=90
  _FIELD._serialized_end=230
  _ONDEMAND._serialized_start=232
  _ONDEMAND._serialized_end=313
  _CREATEDATASETREQUEST._serialized_start=316
  _CREATEDATASETREQUEST._serialized_end=736
  _CREATEDATASETRESPONSE._serialized_start=738
  _CREATEDATASETRESPONSE._serialized_end=813
  _PIPELINE._serialized_start=816
  _PIPELINE._serialized_end=952
  _NODE._serialized_start=954
  _NODE._serialized_end=1048
  _OPERATOR._serialized_start=1051
  _OPERATOR._serialized_end=1273
  _AGGREGATE._serialized_start=1275
  _AGGREGATE._serialized_end=1372
  _JOIN._serialized_start=1375
  _JOIN._serialized_end=1511
  _JOIN_ONENTRY._serialized_start=1470
  _JOIN_ONENTRY._serialized_end=1511
  _TRANSFORM._serialized_start=1513
  _TRANSFORM._serialized_end=1597
  _FILTER._serialized_start=1599
  _FILTER._serialized_end=1680
  _UNION._serialized_start=1682
  _UNION._serialized_end=1715
  _AGGREGATION._serialized_start=1718
  _AGGREGATION._serialized_end=1934
  _WINDOW._serialized_start=1936
  _WINDOW._serialized_end=1972
  _DELTAWINDOW._serialized_start=1974
  _DELTAWINDOW._serialized_end=2065
  _WINDOWSPEC._serialized_start=2068
  _WINDOWSPEC._serialized_end=2205
  _TOPKCONFIG._serialized_start=2207
  _TOPKCONFIG._serialized_end=2272
  _CFCONFIG._serialized_start=2274
  _CFCONFIG._serialized_end=2345
# @@protoc_insertion_point(module_scope)
