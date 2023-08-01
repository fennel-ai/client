# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: spec.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.window_pb2 as window__pb2
import fennel.gen.schema_pb2 as schema__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nspec.proto\x12\x11\x66\x65nnel.proto.spec\x1a\x0cwindow.proto\x1a\x0cschema.proto\"\xbc\x02\n\x07PreSpec\x12%\n\x03sum\x18\x01 \x01(\x0b\x32\x16.fennel.proto.spec.SumH\x00\x12-\n\x07\x61verage\x18\x02 \x01(\x0b\x32\x1a.fennel.proto.spec.AverageH\x00\x12)\n\x05\x63ount\x18\x03 \x01(\x0b\x32\x18.fennel.proto.spec.CountH\x00\x12*\n\x06last_k\x18\x04 \x01(\x0b\x32\x18.fennel.proto.spec.LastKH\x00\x12%\n\x03min\x18\x05 \x01(\x0b\x32\x16.fennel.proto.spec.MinH\x00\x12%\n\x03max\x18\x06 \x01(\x0b\x32\x16.fennel.proto.spec.MaxH\x00\x12+\n\x06stddev\x18\x07 \x01(\x0b\x32\x19.fennel.proto.spec.StddevH\x00\x42\t\n\x07variant\"L\n\x03Sum\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\"a\n\x07\x41verage\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"n\n\x05\x43ount\x12\x0c\n\x04name\x18\x01 \x01(\t\x12+\n\x06window\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0e\n\x06unique\x18\x03 \x01(\x08\x12\x0e\n\x06\x61pprox\x18\x04 \x01(\x08\x12\n\n\x02of\x18\x05 \x01(\t\"l\n\x05LastK\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\r\n\x05limit\x18\x03 \x01(\r\x12\r\n\x05\x64\x65\x64up\x18\x04 \x01(\x08\x12+\n\x06window\x18\x05 \x01(\x0b\x32\x1b.fennel.proto.window.Window\"]\n\x03Min\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"]\n\x03Max\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\"`\n\x06Stddev\x12\n\n\x02of\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12+\n\x06window\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.window.Window\x12\x0f\n\x07\x64\x65\x66\x61ult\x18\x04 \x01(\x01\x62\x06proto3')



_PRESPEC = DESCRIPTOR.message_types_by_name['PreSpec']
_SUM = DESCRIPTOR.message_types_by_name['Sum']
_AVERAGE = DESCRIPTOR.message_types_by_name['Average']
_COUNT = DESCRIPTOR.message_types_by_name['Count']
_LASTK = DESCRIPTOR.message_types_by_name['LastK']
_MIN = DESCRIPTOR.message_types_by_name['Min']
_MAX = DESCRIPTOR.message_types_by_name['Max']
_STDDEV = DESCRIPTOR.message_types_by_name['Stddev']
PreSpec = _reflection.GeneratedProtocolMessageType('PreSpec', (_message.Message,), {
  'DESCRIPTOR' : _PRESPEC,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.PreSpec)
  })
_sym_db.RegisterMessage(PreSpec)

Sum = _reflection.GeneratedProtocolMessageType('Sum', (_message.Message,), {
  'DESCRIPTOR' : _SUM,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.Sum)
  })
_sym_db.RegisterMessage(Sum)

Average = _reflection.GeneratedProtocolMessageType('Average', (_message.Message,), {
  'DESCRIPTOR' : _AVERAGE,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.Average)
  })
_sym_db.RegisterMessage(Average)

Count = _reflection.GeneratedProtocolMessageType('Count', (_message.Message,), {
  'DESCRIPTOR' : _COUNT,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.Count)
  })
_sym_db.RegisterMessage(Count)

LastK = _reflection.GeneratedProtocolMessageType('LastK', (_message.Message,), {
  'DESCRIPTOR' : _LASTK,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.LastK)
  })
_sym_db.RegisterMessage(LastK)

Min = _reflection.GeneratedProtocolMessageType('Min', (_message.Message,), {
  'DESCRIPTOR' : _MIN,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.Min)
  })
_sym_db.RegisterMessage(Min)

Max = _reflection.GeneratedProtocolMessageType('Max', (_message.Message,), {
  'DESCRIPTOR' : _MAX,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.Max)
  })
_sym_db.RegisterMessage(Max)

Stddev = _reflection.GeneratedProtocolMessageType('Stddev', (_message.Message,), {
  'DESCRIPTOR' : _STDDEV,
  '__module__' : 'spec_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.spec.Stddev)
  })
_sym_db.RegisterMessage(Stddev)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _PRESPEC._serialized_start=62
  _PRESPEC._serialized_end=378
  _SUM._serialized_start=380
  _SUM._serialized_end=456
  _AVERAGE._serialized_start=458
  _AVERAGE._serialized_end=555
  _COUNT._serialized_start=557
  _COUNT._serialized_end=667
  _LASTK._serialized_start=669
  _LASTK._serialized_end=777
  _MIN._serialized_start=779
  _MIN._serialized_end=872
  _MAX._serialized_start=874
  _MAX._serialized_end=967
  _STDDEV._serialized_start=969
  _STDDEV._serialized_end=1065
# @@protoc_insertion_point(module_scope)
