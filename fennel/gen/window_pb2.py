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


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0cwindow.proto\x12\x13\x66\x65nnel.proto.window\x1a\x1egoogle/protobuf/duration.proto\"u\n\x06Window\x12/\n\x07sliding\x18\x01 \x01(\x0b\x32\x1c.fennel.proto.window.SlidingH\x00\x12/\n\x07\x66orever\x18\x02 \x01(\x0b\x32\x1c.fennel.proto.window.ForeverH\x00\x42\t\n\x07variant\"6\n\x07Sliding\x12+\n\x08\x64uration\x18\x01 \x01(\x0b\x32\x19.google.protobuf.Duration\"\t\n\x07\x46oreverb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'window_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _WINDOW._serialized_start=69
  _WINDOW._serialized_end=186
  _SLIDING._serialized_start=188
  _SLIDING._serialized_end=242
  _FOREVER._serialized_start=244
  _FOREVER._serialized_end=253
# @@protoc_insertion_point(module_scope)
