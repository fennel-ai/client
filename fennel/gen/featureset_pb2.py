# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: featureset.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.status_pb2 as status__pb2
import fennel.gen.metadata_pb2 as metadata__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x66\x65\x61tureset.proto\x12\x0c\x66\x65nnel.proto\x1a\x0cstatus.proto\x1a\x0emetadata.proto\"\xaf\x01\n\x07\x46\x65\x61ture\x12\n\n\x02id\x18\x01 \x01(\x05\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\r\n\x05\x64type\x18\x03 \x01(\t\x12\r\n\x05owner\x18\x04 \x01(\t\x12\x13\n\x0b\x64\x65scription\x18\x05 \x01(\t\x12\x0b\n\x03wip\x18\x06 \x01(\x08\x12\x12\n\ndeprecated\x18\x07 \x01(\x08\x12\x0c\n\x04tags\x18\x08 \x03(\t\x12(\n\x08metadata\x18\t \x01(\x0b\x32\x16.fennel.proto.Metadata\"\xb4\x01\n\tExtractor\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x0c\n\x04\x66unc\x18\x02 \x01(\x0c\x12\x18\n\x10\x66unc_source_code\x18\x03 \x01(\t\x12\x10\n\x08\x64\x61tasets\x18\x04 \x03(\t\x12#\n\x06inputs\x18\x05 \x03(\x0b\x32\x13.fennel.proto.Input\x12\x10\n\x08\x66\x65\x61tures\x18\x06 \x03(\t\x12(\n\x08metadata\x18\x07 \x01(\x0b\x32\x16.fennel.proto.Metadata\"\xcb\x01\n\x17\x43reateFeaturesetRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\'\n\x08\x66\x65\x61tures\x18\x02 \x03(\x0b\x32\x15.fennel.proto.Feature\x12+\n\nextractors\x18\x03 \x03(\x0b\x32\x17.fennel.proto.Extractor\x12\x0f\n\x07version\x18\x04 \x01(\x05\x12\x11\n\tsignature\x18\x05 \x01(\t\x12(\n\x08metadata\x18\x06 \x01(\x0b\x32\x16.fennel.proto.Metadata\"N\n\x18\x43reateFeatureSetResponse\x12\x0c\n\x04name\x18\x01 \x01(\t\x12$\n\x06status\x18\x02 \x01(\x0b\x32\x14.fennel.proto.Status\"\xe1\x01\n\x05Input\x12\x35\n\x0b\x66\x65\x61ture_set\x18\x01 \x01(\x0b\x32\x1e.fennel.proto.Input.FeatureSetH\x00\x12.\n\x07\x66\x65\x61ture\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.Input.FeatureH\x00\x1a\x1a\n\nFeatureSet\x12\x0c\n\x04name\x18\x01 \x01(\t\x1aL\n\x07\x46\x65\x61ture\x12\x33\n\x0b\x66\x65\x61ture_set\x18\x01 \x01(\x0b\x32\x1e.fennel.proto.Input.FeatureSet\x12\x0c\n\x04name\x18\x02 \x01(\tB\x07\n\x05inputb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'featureset_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _FEATURE._serialized_start=65
  _FEATURE._serialized_end=240
  _EXTRACTOR._serialized_start=243
  _EXTRACTOR._serialized_end=423
  _CREATEFEATURESETREQUEST._serialized_start=426
  _CREATEFEATURESETREQUEST._serialized_end=629
  _CREATEFEATURESETRESPONSE._serialized_start=631
  _CREATEFEATURESETRESPONSE._serialized_end=709
  _INPUT._serialized_start=712
  _INPUT._serialized_end=937
  _INPUT_FEATURESET._serialized_start=824
  _INPUT_FEATURESET._serialized_end=850
  _INPUT_FEATURE._serialized_start=852
  _INPUT_FEATURE._serialized_end=928
# @@protoc_insertion_point(module_scope)
