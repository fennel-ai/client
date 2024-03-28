# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: window.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
import fennel.gen.pycode_pb2 as pycode__pb2
import fennel.gen.schema_pb2 as schema__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cwindow.proto\x12\x13\x66\x65nnel.proto.window\x1a\x1egoogle/protobuf/duration.proto\x1a\x0cpycode.proto\x1a\x0cschema.proto\"\x8a\x02\n\x06Window\x12/\n\x07sliding\x18\x01 \x01(\x0b\x32\x1c.fennel.proto.window.SlidingH\x00\x12/\n\x07\x66orever\x18\x02 \x01(\x0b\x32\x1c.fennel.proto.window.ForeverH\x00\x12/\n\x07session\x18\x03 \x01(\x0b\x32\x1c.fennel.proto.window.SessionH\x00\x12\x31\n\x08tumbling\x18\x04 \x01(\x0b\x32\x1d.fennel.proto.window.TumblingH\x00\x12/\n\x07hopping\x18\x05 \x01(\x0b\x32\x1c.fennel.proto.window.HoppingH\x00\x42\t\n\x07variant\"6\n\x07Sliding\x12+\n\x08\x64uration\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\"\t\n\x07\x46orever\"7\n\x08Tumbling\x12+\n\x08\x64uration\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\"a\n\x07Hopping\x12+\n\x08\x64uration\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\x12)\n\x06stride\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\"1\n\x07Session\x12&\n\x03gap\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\"\x7f\n\x07Summary\x12\x13\n\x0b\x63olumn_name\x18\x01 \x01(\t\x12\x32\n\x0boutput_type\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\x12+\n\x06pycode\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCodeb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'window_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _WINDOW._serialized_start=98
  _WINDOW._serialized_end=364
  _SLIDING._serialized_start=366
  _SLIDING._serialized_end=420
  _FOREVER._serialized_start=422
  _FOREVER._serialized_end=431
  _TUMBLING._serialized_start=433
  _TUMBLING._serialized_end=488
  _HOPPING._serialized_start=490
  _HOPPING._serialized_end=587
  _SESSION._serialized_start=589
  _SESSION._serialized_end=638
  _SUMMARY._serialized_start=640
  _SUMMARY._serialized_end=767
# @@protoc_insertion_point(module_scope)
