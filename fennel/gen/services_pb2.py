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


import fennel.gen.metadata_pb2 as metadata__pb2
import fennel.gen.status_pb2 as status__pb2
import fennel.gen.source_pb2 as source__pb2
import fennel.gen.dataset_pb2 as dataset__pb2
import fennel.gen.featureset_pb2 as featureset__pb2
import fennel.gen.expectations_pb2 as expectations__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0eservices.proto\x12\x15\x66\x65nnel.proto.services\x1a\x0emetadata.proto\x1a\x0cstatus.proto\x1a\x0csource.proto\x1a\rdataset.proto\x1a\x10\x66\x65\x61tureset.proto\x1a\x12\x65xpectations.proto\"\x80\x04\n\x14\x43reateDatasetRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12+\n\x06\x66ields\x18\x02 \x03(\x0b\x32\x1b.fennel.proto.dataset.Field\x12\x31\n\tpipelines\x18\x03 \x03(\x0b\x32\x1e.fennel.proto.dataset.Pipeline\x12<\n\x10input_connectors\x18\x04 \x03(\x0b\x32\".fennel.proto.source.DataConnector\x12=\n\x11output_connectors\x18\x05 \x03(\x0b\x32\".fennel.proto.source.DataConnector\x12\x11\n\tsignature\x18\x06 \x01(\t\x12\x31\n\x08metadata\x18\x07 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata\x12\x0c\n\x04mode\x18\x08 \x01(\t\x12\x0f\n\x07version\x18\t \x01(\r\x12\x0f\n\x07history\x18\n \x01(\x03\x12\x15\n\rmax_staleness\x18\x0b \x01(\x03\x12\x31\n\ton_demand\x18\x0c \x01(\x0b\x32\x1e.fennel.proto.dataset.OnDemand\x12=\n\x0c\x65xpectations\x18\r \x01(\x0b\x32\'.fennel.proto.expectations.Expectations\"R\n\x15\x43reateDatasetResponse\x12\x0c\n\x04name\x18\x01 \x01(\t\x12+\n\x06status\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.status.Status\"\xd1\x01\n\x0bSyncRequest\x12\x45\n\x10\x64\x61taset_requests\x18\x01 \x03(\x0b\x32+.fennel.proto.services.CreateDatasetRequest\x12K\n\x13\x66\x65\x61tureset_requests\x18\x03 \x03(\x0b\x32..fennel.proto.services.CreateFeaturesetRequest\x12.\n\x06models\x18\x04 \x03(\x0b\x32\x1e.fennel.proto.featureset.Model\"\xb9\x02\n\x17\x43reateFeaturesetRequest\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x32\n\x08\x66\x65\x61tures\x18\x02 \x03(\x0b\x32 .fennel.proto.featureset.Feature\x12\x36\n\nextractors\x18\x03 \x03(\x0b\x32\".fennel.proto.featureset.Extractor\x12\x0f\n\x07version\x18\x04 \x01(\x05\x12\x11\n\tsignature\x18\x05 \x01(\t\x12\x31\n\x08metadata\x18\x06 \x01(\x0b\x32\x1f.fennel.proto.metadata.Metadata\x12\x0e\n\x06schema\x18\x07 \x01(\x0c\x12=\n\x0c\x65xpectations\x18\x08 \x01(\x0b\x32\'.fennel.proto.expectations.Expectations\"U\n\x18\x43reateFeatureSetResponse\x12\x0c\n\x04name\x18\x01 \x01(\t\x12+\n\x06status\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.status.Status\"\xa7\x01\n\x0cSyncResponse\x12G\n\x11\x64\x61taset_responses\x18\x01 \x03(\x0b\x32,.fennel.proto.services.CreateDatasetResponse\x12N\n\x15\x66\x65\x61ture_set_responses\x18\x03 \x03(\x0b\x32/.fennel.proto.services.CreateFeatureSetResponse2e\n\x12\x46\x65nnelFeatureStore\x12O\n\x04Sync\x12\".fennel.proto.services.SyncRequest\x1a#.fennel.proto.services.SyncResponseb\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'services_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _CREATEDATASETREQUEST._serialized_start=139
  _CREATEDATASETREQUEST._serialized_end=651
  _CREATEDATASETRESPONSE._serialized_start=653
  _CREATEDATASETRESPONSE._serialized_end=735
  _SYNCREQUEST._serialized_start=738
  _SYNCREQUEST._serialized_end=947
  _CREATEFEATURESETREQUEST._serialized_start=950
  _CREATEFEATURESETREQUEST._serialized_end=1263
  _CREATEFEATURESETRESPONSE._serialized_start=1265
  _CREATEFEATURESETRESPONSE._serialized_end=1350
  _SYNCRESPONSE._serialized_start=1353
  _SYNCRESPONSE._serialized_end=1520
  _FENNELFEATURESTORE._serialized_start=1522
  _FENNELFEATURESTORE._serialized_end=1623
# @@protoc_insertion_point(module_scope)
