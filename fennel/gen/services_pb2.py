# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: services.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()

import fennel.gen.status_pb2 as status__pb2
import fennel.gen.stream_pb2 as stream__pb2

DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x0eservices.proto\x12\x0c\x66\x65nnel.proto\x1a\x0cstream.proto\x1a\x0cstatus.proto2\xb0\x01\n\x12\x46\x65nnelFeatureStore\x12I\n\x0eRegisterSource\x12!.fennel.proto.CreateSourceRequest\x1a\x14.fennel.proto.Status\x12O\n\x11RegisterConnector\x12$.fennel.proto.CreateConnectorRequest\x1a\x14.fennel.proto.Statusb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'services_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _FENNELFEATURESTORE._serialized_start = 61
    _FENNELFEATURESTORE._serialized_end = 237
# @@protoc_insertion_point(module_scope)
