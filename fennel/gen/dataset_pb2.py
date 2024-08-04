# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: dataset.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
import fennel.gen.metadata_pb2 as metadata__pb2
import fennel.gen.pycode_pb2 as pycode__pb2
import fennel.gen.schema_pb2 as schema__pb2
import fennel.gen.spec_pb2 as spec__pb2
import fennel.gen.window_pb2 as window__pb2
import fennel.gen.expr_pb2 as expr__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\rdataset.proto\x12\x14\x66\x65nnel.proto.dataset\x1a\x1egoogle/protobuf/duration.proto\x1a\x0emetadata.proto\x1a\x0cpycode.proto\x1a\x0cschema.proto\x1a\nspec.proto\x1a\x0cwindow.proto\x1a\nexpr.proto\"\xe5\x03\n\x0b\x43oreDataset\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x31\n\x08metadata\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata\x12/\n\x08\x64sschema\x18\x03 \x01(\x0b\x32\x1d.fennel.proto.schema.DSSchema\x12*\n\x07history\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12,\n\tretention\x18\x05 \x01(\x0b\x32\x19.google.protobuf.Duration\x12L\n\x0e\x66ield_metadata\x18\x06 \x03(\x0b\x32\x34.fennel.proto.dataset.CoreDataset.FieldMetadataEntry\x12+\n\x06pycode\x18\x07 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x19\n\x11is_source_dataset\x18\x08 \x01(\x08\x12\x0f\n\x07version\x18\t \x01(\r\x12\x0c\n\x04tags\x18\n \x03(\t\x1aU\n\x12\x46ieldMetadataEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12.\n\x05value\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata:\x02\x38\x01\"Q\n\x08OnDemand\x12\x1c\n\x14\x66unction_source_code\x18\x01 \x01(\t\x12\x10\n\x08\x66unction\x18\x02 \x01(\x0c\x12\x15\n\rexpires_after\x18\x03 \x01(\x03\"\xd2\x01\n\x08Pipeline\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x14\n\x0c\x64\x61taset_name\x18\x02 \x01(\t\x12\x11\n\tsignature\x18\x03 \x01(\t\x12\x31\n\x08metadata\x18\x04 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata\x12\x1b\n\x13input_dataset_names\x18\x05 \x03(\t\x12\x12\n\nds_version\x18\x06 \x01(\r\x12+\n\x06pycode\x18\x07 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\"\x8f\x08\n\x08Operator\x12\n\n\x02id\x18\x01 \x01(\t\x12\x0f\n\x07is_root\x18\x02 \x01(\x08\x12\x15\n\rpipeline_name\x18\x03 \x01(\t\x12\x14\n\x0c\x64\x61taset_name\x18\x04 \x01(\t\x12\x12\n\nds_version\x18\x14 \x01(\r\x12\x34\n\taggregate\x18\x05 \x01(\x0b\x32\x1f.fennel.proto.dataset.AggregateH\x00\x12*\n\x04join\x18\x06 \x01(\x0b\x32\x1a.fennel.proto.dataset.JoinH\x00\x12\x34\n\ttransform\x18\x07 \x01(\x0b\x32\x1f.fennel.proto.dataset.TransformH\x00\x12,\n\x05union\x18\x08 \x01(\x0b\x32\x1b.fennel.proto.dataset.UnionH\x00\x12.\n\x06\x66ilter\x18\t \x01(\x0b\x32\x1c.fennel.proto.dataset.FilterH\x00\x12\x37\n\x0b\x64\x61taset_ref\x18\n \x01(\x0b\x32 .fennel.proto.dataset.DatasetRefH\x00\x12.\n\x06rename\x18\x0c \x01(\x0b\x32\x1c.fennel.proto.dataset.RenameH\x00\x12*\n\x04\x64rop\x18\r \x01(\x0b\x32\x1a.fennel.proto.dataset.DropH\x00\x12\x30\n\x07\x65xplode\x18\x0e \x01(\x0b\x32\x1d.fennel.proto.dataset.ExplodeH\x00\x12,\n\x05\x64\x65\x64up\x18\x0f \x01(\x0b\x32\x1b.fennel.proto.dataset.DedupH\x00\x12,\n\x05\x66irst\x18\x10 \x01(\x0b\x32\x1b.fennel.proto.dataset.FirstH\x00\x12.\n\x06\x61ssign\x18\x11 \x01(\x0b\x32\x1c.fennel.proto.dataset.AssignH\x00\x12\x32\n\x08\x64ropnull\x18\x12 \x01(\x0b\x32\x1e.fennel.proto.dataset.DropnullH\x00\x12:\n\x06window\x18\x13 \x01(\x0b\x32(.fennel.proto.dataset.WindowOperatorKindH\x00\x12.\n\x06latest\x18\x15 \x01(\x0b\x32\x1c.fennel.proto.dataset.LatestH\x00\x12\x34\n\tchangelog\x18\x16 \x01(\x0b\x32\x1f.fennel.proto.dataset.ChangelogH\x00\x12\x37\n\x0b\x61ssign_expr\x18\x17 \x01(\x0b\x32 .fennel.proto.dataset.AssignExprH\x00\x12\x37\n\x0b\x66ilter_expr\x18\x18 \x01(\x0b\x32 .fennel.proto.dataset.FilterExprH\x00\x12\x0c\n\x04name\x18\x0b \x01(\tB\x06\n\x04kind\"\xf7\x01\n\tAggregate\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0c\n\x04keys\x18\x02 \x03(\t\x12)\n\x05specs\x18\x03 \x03(\x0b\x32\x1a.fennel.proto.spec.PreSpec\x12\x12\n\x05\x61long\x18\x05 \x01(\tH\x00\x88\x01\x01\x12\x43\n\remit_strategy\x18\x06 \x01(\x0e\x32,.fennel.proto.dataset.Aggregate.EmitStrategy\x12\x14\n\x0coperand_name\x18\x04 \x01(\t\"$\n\x0c\x45mitStrategy\x12\t\n\x05\x45\x61ger\x10\x00\x12\t\n\x05\x46inal\x10\x01\x42\x08\n\x06_along\"\xa2\x03\n\x04Join\x12\x16\n\x0elhs_operand_id\x18\x01 \x01(\t\x12\x1c\n\x14rhs_dsref_operand_id\x18\x02 \x01(\t\x12.\n\x02on\x18\x03 \x03(\x0b\x32\".fennel.proto.dataset.Join.OnEntry\x12\x32\n\nwithin_low\x18\x06 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x88\x01\x01\x12\x33\n\x0bwithin_high\x18\x07 \x01(\x0b\x32\x19.google.protobuf.DurationH\x01\x88\x01\x01\x12\x18\n\x10lhs_operand_name\x18\x04 \x01(\t\x12\x1e\n\x16rhs_dsref_operand_name\x18\x05 \x01(\t\x12+\n\x03how\x18\x08 \x01(\x0e\x32\x1e.fennel.proto.dataset.Join.How\x1a)\n\x07OnEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"\x1a\n\x03How\x12\x08\n\x04Left\x10\x00\x12\t\n\x05Inner\x10\x01\x42\r\n\x0b_within_lowB\x0e\n\x0c_within_high\"\xed\x01\n\tTransform\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12;\n\x06schema\x18\x02 \x03(\x0b\x32+.fennel.proto.dataset.Transform.SchemaEntry\x12+\n\x06pycode\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x14\n\x0coperand_name\x18\x04 \x01(\t\x1aL\n\x0bSchemaEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType:\x02\x38\x01\"]\n\nFilterExpr\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12%\n\x04\x65xpr\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"_\n\x06\x46ilter\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12+\n\x06pycode\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"\xa8\x01\n\x06\x41ssign\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12+\n\x06pycode\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCode\x12\x13\n\x0b\x63olumn_name\x18\x03 \x01(\t\x12\x32\n\x0boutput_type\x18\x04 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\x12\x14\n\x0coperand_name\x18\x05 \x01(\t\"\xd5\x02\n\nAssignExpr\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12:\n\x05\x65xprs\x18\x02 \x03(\x0b\x32+.fennel.proto.dataset.AssignExpr.ExprsEntry\x12G\n\x0coutput_types\x18\x03 \x03(\x0b\x32\x31.fennel.proto.dataset.AssignExpr.OutputTypesEntry\x12\x14\n\x0coperand_name\x18\x05 \x01(\t\x1a\x45\n\nExprsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr:\x02\x38\x01\x1aQ\n\x10OutputTypesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType:\x02\x38\x01\"E\n\x08\x44ropnull\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"B\n\x04\x44rop\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x10\n\x08\x64ropcols\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"\xa5\x01\n\x06Rename\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12?\n\ncolumn_map\x18\x02 \x03(\x0b\x32+.fennel.proto.dataset.Rename.ColumnMapEntry\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\x1a\x30\n\x0e\x43olumnMapEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\"3\n\x05Union\x12\x13\n\x0boperand_ids\x18\x01 \x03(\t\x12\x15\n\roperand_names\x18\x02 \x03(\t\"B\n\x05\x44\x65\x64up\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"D\n\x07\x45xplode\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x0f\n\x07\x63olumns\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"=\n\x05\x46irst\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\n\n\x02\x62y\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\">\n\x06Latest\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\n\n\x02\x62y\x18\x02 \x03(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"L\n\tChangelog\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x15\n\rdelete_column\x18\x02 \x01(\t\x12\x14\n\x0coperand_name\x18\x03 \x01(\t\"\xcb\x01\n\x12WindowOperatorKind\x12\x12\n\noperand_id\x18\x01 \x01(\t\x12\x30\n\x0bwindow_type\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\n\n\x02\x62y\x18\x03 \x03(\t\x12\r\n\x05\x66ield\x18\x04 \x01(\t\x12\x32\n\x07summary\x18\x06 \x01(\x0b\x32\x1c.fennel.proto.window.SummaryH\x00\x88\x01\x01\x12\x14\n\x0coperand_name\x18\x05 \x01(\tB\n\n\x08_summary\",\n\nDatasetRef\x12\x1e\n\x16referring_dataset_name\x18\x01 \x01(\t\"\x80\x02\n\x08\x44\x61taflow\x12\x16\n\x0c\x64\x61taset_name\x18\x01 \x01(\tH\x00\x12L\n\x11pipeline_dataflow\x18\x02 \x01(\x0b\x32/.fennel.proto.dataset.Dataflow.PipelineDataflowH\x00\x12\x0c\n\x04tags\x18\x03 \x03(\t\x1ax\n\x10PipelineDataflow\x12\x14\n\x0c\x64\x61taset_name\x18\x01 \x01(\t\x12\x15\n\rpipeline_name\x18\x02 \x01(\t\x12\x37\n\x0finput_dataflows\x18\x03 \x03(\x0b\x32\x1e.fennel.proto.dataset.DataflowB\x06\n\x04kind\"\x9c\x01\n\x10PipelineLineages\x12\x14\n\x0c\x64\x61taset_name\x18\x01 \x01(\t\x12\x15\n\rpipeline_name\x18\x02 \x01(\t\x12=\n\x0einput_datasets\x18\x03 \x03(\x0b\x32%.fennel.proto.dataset.DatasetLineages\x12\x0e\n\x06\x61\x63tive\x18\x04 \x01(\x08\x12\x0c\n\x04tags\x18\x05 \x03(\t\"\\\n\x17\x44\x61tasetPipelineLineages\x12\x41\n\x11pipeline_lineages\x18\x02 \x03(\x0b\x32&.fennel.proto.dataset.PipelineLineages\"\x8b\x01\n\x0f\x44\x61tasetLineages\x12\x18\n\x0esource_dataset\x18\x01 \x01(\tH\x00\x12H\n\x0f\x64\x65rived_dataset\x18\x02 \x01(\x0b\x32-.fennel.proto.dataset.DatasetPipelineLineagesH\x00\x12\x0c\n\x04tags\x18\x03 \x03(\tB\x06\n\x04kindb\x06proto3')

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
  _globals['_ASSIGNEXPR_EXPRSENTRY']._options = None
  _globals['_ASSIGNEXPR_EXPRSENTRY']._serialized_options = b'8\001'
  _globals['_ASSIGNEXPR_OUTPUTTYPESENTRY']._options = None
  _globals['_ASSIGNEXPR_OUTPUTTYPESENTRY']._serialized_options = b'8\001'
  _globals['_RENAME_COLUMNMAPENTRY']._options = None
  _globals['_RENAME_COLUMNMAPENTRY']._serialized_options = b'8\001'
  _globals['_COREDATASET']._serialized_start=154
  _globals['_COREDATASET']._serialized_end=639
  _globals['_COREDATASET_FIELDMETADATAENTRY']._serialized_start=554
  _globals['_COREDATASET_FIELDMETADATAENTRY']._serialized_end=639
  _globals['_ONDEMAND']._serialized_start=641
  _globals['_ONDEMAND']._serialized_end=722
  _globals['_PIPELINE']._serialized_start=725
  _globals['_PIPELINE']._serialized_end=935
  _globals['_OPERATOR']._serialized_start=938
  _globals['_OPERATOR']._serialized_end=1977
  _globals['_AGGREGATE']._serialized_start=1980
  _globals['_AGGREGATE']._serialized_end=2227
  _globals['_AGGREGATE_EMITSTRATEGY']._serialized_start=2181
  _globals['_AGGREGATE_EMITSTRATEGY']._serialized_end=2217
  _globals['_JOIN']._serialized_start=2230
  _globals['_JOIN']._serialized_end=2648
  _globals['_JOIN_ONENTRY']._serialized_start=2548
  _globals['_JOIN_ONENTRY']._serialized_end=2589
  _globals['_JOIN_HOW']._serialized_start=2591
  _globals['_JOIN_HOW']._serialized_end=2617
  _globals['_TRANSFORM']._serialized_start=2651
  _globals['_TRANSFORM']._serialized_end=2888
  _globals['_TRANSFORM_SCHEMAENTRY']._serialized_start=2812
  _globals['_TRANSFORM_SCHEMAENTRY']._serialized_end=2888
  _globals['_FILTEREXPR']._serialized_start=2890
  _globals['_FILTEREXPR']._serialized_end=2983
  _globals['_FILTER']._serialized_start=2985
  _globals['_FILTER']._serialized_end=3080
  _globals['_ASSIGN']._serialized_start=3083
  _globals['_ASSIGN']._serialized_end=3251
  _globals['_ASSIGNEXPR']._serialized_start=3254
  _globals['_ASSIGNEXPR']._serialized_end=3595
  _globals['_ASSIGNEXPR_EXPRSENTRY']._serialized_start=3443
  _globals['_ASSIGNEXPR_EXPRSENTRY']._serialized_end=3512
  _globals['_ASSIGNEXPR_OUTPUTTYPESENTRY']._serialized_start=3514
  _globals['_ASSIGNEXPR_OUTPUTTYPESENTRY']._serialized_end=3595
  _globals['_DROPNULL']._serialized_start=3597
  _globals['_DROPNULL']._serialized_end=3666
  _globals['_DROP']._serialized_start=3668
  _globals['_DROP']._serialized_end=3734
  _globals['_RENAME']._serialized_start=3737
  _globals['_RENAME']._serialized_end=3902
  _globals['_RENAME_COLUMNMAPENTRY']._serialized_start=3854
  _globals['_RENAME_COLUMNMAPENTRY']._serialized_end=3902
  _globals['_UNION']._serialized_start=3904
  _globals['_UNION']._serialized_end=3955
  _globals['_DEDUP']._serialized_start=3957
  _globals['_DEDUP']._serialized_end=4023
  _globals['_EXPLODE']._serialized_start=4025
  _globals['_EXPLODE']._serialized_end=4093
  _globals['_FIRST']._serialized_start=4095
  _globals['_FIRST']._serialized_end=4156
  _globals['_LATEST']._serialized_start=4158
  _globals['_LATEST']._serialized_end=4220
  _globals['_CHANGELOG']._serialized_start=4222
  _globals['_CHANGELOG']._serialized_end=4298
  _globals['_WINDOWOPERATORKIND']._serialized_start=4301
  _globals['_WINDOWOPERATORKIND']._serialized_end=4504
  _globals['_DATASETREF']._serialized_start=4506
  _globals['_DATASETREF']._serialized_end=4550
  _globals['_DATAFLOW']._serialized_start=4553
  _globals['_DATAFLOW']._serialized_end=4809
  _globals['_DATAFLOW_PIPELINEDATAFLOW']._serialized_start=4681
  _globals['_DATAFLOW_PIPELINEDATAFLOW']._serialized_end=4801
  _globals['_PIPELINELINEAGES']._serialized_start=4812
  _globals['_PIPELINELINEAGES']._serialized_end=4968
  _globals['_DATASETPIPELINELINEAGES']._serialized_start=4970
  _globals['_DATASETPIPELINELINEAGES']._serialized_end=5062
  _globals['_DATASETLINEAGES']._serialized_start=5065
  _globals['_DATASETLINEAGES']._serialized_end=5204
# @@protoc_insertion_point(module_scope)
