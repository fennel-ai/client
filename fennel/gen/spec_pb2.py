# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: spec.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.window_pb2 as window__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nspec.proto\x12\x11\x66\x65nnel.proto.spec\x1a\x0cwindow.proto\"\x9e\x03\n\x07PreSpec\x12%\n\x03sum\x18\x01 \x01(\x0b\x32\x16.fennel.proto.spec.SumH\x00\x12-\n\x07\x61verage\x18\x02 \x01(\x0b\x32\x1a.fennel.proto.spec.AverageH\x00\x12)\n\x05\x63ount\x18\x03 \x01(\x0b\x32\x18.fennel.proto.spec.CountH\x00\x12*\n\x06last_k\x18\x04 \x01(\x0b\x32\x18.fennel.proto.spec.LastKH\x00\x12%\n\x03min\x18\x05 \x01(\x0b\x32\x16.fennel.proto.spec.MinH\x00\x12%\n\x03max\x18\x06 \x01(\x0b\x32\x16.fennel.proto.spec.MaxH\x00\x12+\n\x06stddev\x18\x07 \x01(\x0b\x32\x19.fennel.proto.spec.StddevH\x00\x12/\n\x08\x64istinct\x18\x08 \x01(\x0b\x32\x1b.fennel.proto.spec.DistinctH\x00\x12/\n\x08quantile\x18\t \x01(\x0b\x32\x1b.fennel.proto.spec.QuantileH\x00\x42\t\n\x07variant\"L\n\x03Sum\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\"a\n\x07\x41verage\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"n\n\x05\x43ount\x12\x0c\n\x04name\x18\x01 \x01(\t\x12+\n\x06window\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0e\n\x06unique\x18\x03 \x01(\x08\x12\x0e\n\x06\x61pprox\x18\x04 \x01(\x08\x12\n\n\x02of\x18\x05 \x01(\t\"l\n\x05LastK\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\r\n\x05limit\x18\x03 \x01(\r\x12\r\n\x05\x64\x65\x64up\x18\x04 \x01(\x08\x12+\n\x06window\x18\x05 \x01(\x0b\x32\x1b.fennel.proto.window.Window\"]\n\x03Min\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"]\n\x03Max\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"`\n\x06Stddev\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"Q\n\x08\x44istinct\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\"\x95\x01\n\x08Quantile\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x14\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01H\x00\x88\x01\x01\x12\x10\n\x08quantile\x18\x05 \x01(\x01\x12\x0e\n\x06\x61pprox\x18\x06 \x01(\x08\x42\n\n\x08_defaultb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'spec_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _PRESPEC._serialized_start=48
  _PRESPEC._serialized_end=462
  _SUM._serialized_start=464
  _SUM._serialized_end=540
  _AVERAGE._serialized_start=542
  _AVERAGE._serialized_end=639
  _COUNT._serialized_start=641
  _COUNT._serialized_end=751
  _LASTK._serialized_start=753
  _LASTK._serialized_end=861
  _MIN._serialized_start=863
  _MIN._serialized_end=956
  _MAX._serialized_start=958
  _MAX._serialized_end=1051
  _STDDEV._serialized_start=1053
  _STDDEV._serialized_end=1149
  _DISTINCT._serialized_start=1151
  _DISTINCT._serialized_end=1232
  _QUANTILE._serialized_start=1235
  _QUANTILE._serialized_end=1384
# @@protoc_insertion_point(module_scope)
