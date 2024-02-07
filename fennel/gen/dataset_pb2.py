# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: dataset.proto
# Protobuf Python Version: 4.25.2
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.metadata_pb2 as metadata__pb2
import fennel.gen.pycode_pb2 as pycode__pb2
import fennel.gen.schema_pb2 as schema__pb2
import fennel.gen.spec_pb2 as spec__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rdataset.proto\x12\x14\x66\x65nnel.proto.dataset\x1a\x0emetadata.proto\x1a\x0cpycode.proto\x1a\x0cschema.proto\x1a\nspec.proto\x1a\x1egoogle/protobuf/duration.proto\"\xe5\x03\n\x0b\x43oreDataset\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x31\n\x08metadata\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata\x12/\n\x08\x64sschema\x18\x03 \x01(\x0b\x32\x1d.fennel.proto.schema.DSSchema\x12*\n\x07history\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12,\n\tretention\x18\x05 \x01(\x0b\x32\x19.google.protobuf.Duration\x12L\n\x0e\x66ield_metadata\x18\x06 \x03(\x0b\x32\x34.fennel.proto.dataset.CoreDataset.FieldMetadataEntry\x12+\n\x06pycode\x18\x07 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x19\n\x11is_source_dataset\x18\x08 \x01(\x08\x12\x0f\n\x07version\x18\t \x01(\r\x12\x0c\n\x04tags\x18\n \x03(\t\x1aU\n\x12\x46ieldMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12.\n\x05value\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata:\x02\x38\x01\"Q\n\x08OnDemand\x12\x1c\n\x14\x66unction_source_code\x18\x01 \x01(\t\x12\x10\n\x08\x66unction\x18\x02 \x01(\x0c\x12\x15\n\rexpires_after\x18\x03 \x01(\x03\"\xd2\x01\n\x08Pipeline\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_name\x18\x02 \x01(\t\x12\x11\n\tsignature\x18\x03 \x01(\t\x12\x31\n\x08metadata\x18\x04 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata\x12\x1b\n\x13input_dataset_names\x18\x05 \x03(\t\x12\x12\n\nds_version\x18\x06 \x01(\r\x12+\n\x06pycode\x18\x07 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\"\xfb\x05\n\x08Operator\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0f\n\x07is_root\x18\x02 \x01(\x08\x12\x15\n\rpipeline_name\x18\x03 \x01(\t\x12\x14\n\x0c\x64\x61taset_name\x18\x04 \x01(\t\x12\x12\n\nds_version\x18\x13 \x01(\r\x12\x34\n\taggregate\x18\x05 \x01(\x0b\x32\x1f.fennel.proto.dataset.AggregateH\x00\x12*\n\x04join\x18\x06 \x01(\x0b\x32\x1a.fennel.proto.dataset.JoinH\x00\x12\x34\n\ttransform\x18\x07 \x01(\x0b\x32\x1f.fennel.proto.dataset.TransformH\x00\x12,\n\x05union\x18\x08 \x01(\x0b\x32\x1b.fennel.proto.dataset.UnionH\x00\x12.\n\x06\x66ilter\x18\t \x01(\x0b\x32\x1c.fennel.proto.dataset.FilterH\x00\x12\x37\n\x0b\x64\x61taset_ref\x18\n \x01(\x0b\x32 .fennel.proto.dataset.DatasetRefH\x00\x12.\n\x06rename\x18\x0c \x01(\x0b\x32\x1c.fennel.proto.dataset.RenameH\x00\x12*\n\x04\x64rop\x18\r \x01(\x0b\x32\x1a.fennel.proto.dataset.DropH\x00\x12\x30\n\x07\x65xplode\x18\x0e \x01(\x0b\x32\x1d.fennel.proto.dataset.ExplodeH\x00\x12,\n\x05\x64\x65\x64up\x18\x0f \x01(\x0b\x32\x1b.fennel.proto.dataset.DedupH\x00\x12,\n\x05\x66irst\x18\x10 \x01(\x0b\x32\x1b.fennel.proto.dataset.FirstH\x00\x12.\n\x06\x61ssign\x18\x11 \x01(\x0b\x32\x1c.fennel.proto.dataset.AssignH\x00\x12\x32\n\x08\x64ropnull\x18\x12 \x01(\x0b\x32\x1e.fennel.proto.dataset.DropnullH\x00\x12\x0c\n\x04name\x18\x0b \x01(\tB\x06\n\x04kind\"n\n\tAggregate\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12)\n\x05specs\x18\x03 \x03(\x0b\x32\x1a.fennel.proto.spec.PreSpec\x12\x14\n\x0coperand_name\x18\x04 \x01(\t\"\xa2\x03\n\x04Join\x12\x16\n\x0elhs_operand_id\x18\x01 \x01(\t\x12\x1c\n\x14rhs_dsref_operand_id\x18\x02 \x01(\t\x12.\n\x02on\x18\x03 \x03(\x0b\x32\".fennel.proto.dataset.Join.OnEntry\x12\x32\n\nwithin_low\x18\x06 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x88\x01\x01\x12\x33\n\x0bwithin_high\x18\x07 \x01(\x0b\x32\x19.google.protobuf.DurationH\x01\x88\x01\x01\x12\x18\n\x10lhs_operand_name\x18\x04 \x01(\t\x12\x1e\n\x16rhs_dsref_operand_name\x18\x05 \x01(\t\x12+\n\x03how\x18\x08 \x01(\x0e\x32\x1e.fennel.proto.dataset.Join.How\x1a)\n\x07OnEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x1a\n\x03How\x12\x08\n\x04Left\x10\x00\x12\t\n\x05Inner\x10\x01\x42\r\n\x0b_within_lowB\x0e\n\x0c_within_high\"\xed\x01\n\tTransform\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12;\n\x06schema\x18\x02 \x03(\x0b\x32+.fennel.proto.dataset.Transform.SchemaEntry\x12+\n\x06pycode\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x14\n\x0coperand_name\x18\x04 \x01(\t\x1aL\n\x0bSchemaEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType:\x02\x38\x01\"_\n\x06\x46ilter\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12+\n\x06pycode\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"\xa8\x01\n\x06\x41ssign\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12+\n\x06pycode\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x13\n\x0b\x63olumn_name\x18\x03 \x01(\t\x12\x32\n\x0boutput_type\x18\x04 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\x12\x14\n\x0coperand_name\x18\x05 \x01(\t\"E\n\x08\x44ropnull\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"B\n\x04\x44rop\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x10\n\x08\x64ropcols\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"\xa5\x01\n\x06Rename\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12?\n\ncolumn_map\x18\x02 \x03(\x0b\x32+.fennel.proto.dataset.Rename.ColumnMapEntry\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\x1a\x30\n\x0e\x43olumnMapEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"3\n\x05Union\x12\x13\n\x0boperand_ids\x18\x01 \x03(\t\x12\x15\n\roperand_names\x18\x02 \x03(\t\"B\n\x05\x44\x65\x64up\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"D\n\x07\x45xplode\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"=\n\x05\x46irst\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\n\n\x02\x62y\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\",\n\nDatasetRef\x12\x1e\n\x16referring_dataset_name\x18\x01 \x01(\t\"\x80\x02\n\x08\x44\x61taflow\x12\x16\n\x0c\x64\x61taset_name\x18\x01 \x01(\tH\x00\x12L\n\x11pipeline_dataflow\x18\x02 \x01(\x0b\x32/.fennel.proto.dataset.Dataflow.PipelineDataflowH\x00\x12\x0c\n\x04tags\x18\x03 \x03(\t\x1ax\n\x10PipelineDataflow\x12\x14\n\x0c\x64\x61taset_name\x18\x01 \x01(\t\x12\x15\n\rpipeline_name\x18\x02 \x01(\t\x12\x37\n\x0finput_dataflows\x18\x03 \x03(\x0b\x32\x1e.fennel.proto.dataset.DataflowB\x06\n\x04kind\"\x9c\x01\n\x10PipelineLineages\x12\x14\n\x0c\x64\x61taset_name\x18\x01 \x01(\t\x12\x15\n\rpipeline_name\x18\x02 \x01(\t\x12=\n\x0einput_datasets\x18\x03 \x03(\x0b\x32%.fennel.proto.dataset.DatasetLineages\x12\x0e\n\x06\x61\x63tive\x18\x04 \x01(\x08\x12\x0c\n\x04tags\x18\x05 \x03(\t\"\\\n\x17\x44\x61tasetPipelineLineages\x12\x41\n\x11pipeline_lineages\x18\x02 \x03(\x0b\x32&.fennel.proto.dataset.PipelineLineages\"\x8b\x01\n\x0f\x44\x61tasetLineages\x12\x18\n\x0esource_dataset\x18\x01 \x01(\tH\x00\x12H\n\x0f\x64\x65rived_dataset\x18\x02 \x01(\x0b\x32-.fennel.proto.dataset.DatasetPipelineLineagesH\x00\x12\x0c\n\x04tags\x18\x03 \x03(\tB\x06\n\x04kindb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'dataset_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_COREDATASET_FIELDMETADATAENTRY']._options = None
  _globals['_COREDATASET_FIELDMETADATAENTRY']._serialized_options = b'8\001'
  _globals['_JOIN_ONENTRY']._options = None
  _globals['_JOIN_ONENTRY']._serialized_options = b'8\001'
  _globals['_TRANSFORM_SCHEMAENTRY']._options = None
  _globals['_TRANSFORM_SCHEMAENTRY']._serialized_options = b'8\001'
  _globals['_RENAME_COLUMNMAPENTRY']._options = None
  _globals['_RENAME_COLUMNMAPENTRY']._serialized_options = b'8\001'
  _globals['_COREDATASET']._serialized_start=128
  _globals['_COREDATASET']._serialized_end=613
  _globals['_COREDATASET_FIELDMETADATAENTRY']._serialized_start=528
  _globals['_COREDATASET_FIELDMETADATAENTRY']._serialized_end=613
  _globals['_ONDEMAND']._serialized_start=615
  _globals['_ONDEMAND']._serialized_end=696
  _globals['_PIPELINE']._serialized_start=699
  _globals['_PIPELINE']._serialized_end=909
  _globals['_OPERATOR']._serialized_start=912
  _globals['_OPERATOR']._serialized_end=1675
  _globals['_AGGREGATE']._serialized_start=1677
  _globals['_AGGREGATE']._serialized_end=1787
  _globals['_JOIN']._serialized_start=1790
  _globals['_JOIN']._serialized_end=2208
  _globals['_JOIN_ONENTRY']._serialized_start=2108
  _globals['_JOIN_ONENTRY']._serialized_end=2149
  _globals['_JOIN_HOW']._serialized_start=2151
  _globals['_JOIN_HOW']._serialized_end=2177
  _globals['_TRANSFORM']._serialized_start=2211
  _globals['_TRANSFORM']._serialized_end=2448
  _globals['_TRANSFORM_SCHEMAENTRY']._serialized_start=2372
  _globals['_TRANSFORM_SCHEMAENTRY']._serialized_end=2448
  _globals['_FILTER']._serialized_start=2450
  _globals['_FILTER']._serialized_end=2545
  _globals['_ASSIGN']._serialized_start=2548
  _globals['_ASSIGN']._serialized_end=2716
  _globals['_DROPNULL']._serialized_start=2718
  _globals['_DROPNULL']._serialized_end=2787
  _globals['_DROP']._serialized_start=2789
  _globals['_DROP']._serialized_end=2855
  _globals['_RENAME']._serialized_start=2858
  _globals['_RENAME']._serialized_end=3023
  _globals['_RENAME_COLUMNMAPENTRY']._serialized_start=2975
  _globals['_RENAME_COLUMNMAPENTRY']._serialized_end=3023
  _globals['_UNION']._serialized_start=3025
  _globals['_UNION']._serialized_end=3076
  _globals['_DEDUP']._serialized_start=3078
  _globals['_DEDUP']._serialized_end=3144
  _globals['_EXPLODE']._serialized_start=3146
  _globals['_EXPLODE']._serialized_end=3214
  _globals['_FIRST']._serialized_start=3216
  _globals['_FIRST']._serialized_end=3277
  _globals['_DATASETREF']._serialized_start=3279
  _globals['_DATASETREF']._serialized_end=3323
  _globals['_DATAFLOW']._serialized_start=3326
  _globals['_DATAFLOW']._serialized_end=3582
  _globals['_DATAFLOW_PIPELINEDATAFLOW']._serialized_start=3454
  _globals['_DATAFLOW_PIPELINEDATAFLOW']._serialized_end=3574
  _globals['_PIPELINELINEAGES']._serialized_start=3585
  _globals['_PIPELINELINEAGES']._serialized_end=3741
  _globals['_DATASETPIPELINELINEAGES']._serialized_start=3743
  _globals['_DATASETPIPELINELINEAGES']._serialized_end=3835
  _globals['_DATASETLINEAGES']._serialized_start=3838
  _globals['_DATASETLINEAGES']._serialized_end=3977
# @@protoc_insertion_point(module_scope)
