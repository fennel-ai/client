# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: connector.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import wrappers_pb2 as google_dot_protobuf_dot_wrappers__pb2
import fennel.gen.kinesis_pb2 as kinesis__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0f\x63onnector.proto\x12\x16\x66\x65nnel.proto.connector\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/wrappers.proto\x1a\rkinesis.proto\"\xf4\x03\n\x0b\x45xtDatabase\x12\x0c\n\x04name\x18\x01 \x01(\t\x12.\n\x05mysql\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.connector.MySQLH\x00\x12\x34\n\x08postgres\x18\x03 \x01(\x0b\x32 .fennel.proto.connector.PostgresH\x00\x12\x36\n\treference\x18\x04 \x01(\x0b\x32!.fennel.proto.connector.ReferenceH\x00\x12(\n\x02s3\x18\x05 \x01(\x0b\x32\x1a.fennel.proto.connector.S3H\x00\x12\x34\n\x08\x62igquery\x18\x06 \x01(\x0b\x32 .fennel.proto.connector.BigqueryH\x00\x12\x36\n\tsnowflake\x18\x07 \x01(\x0b\x32!.fennel.proto.connector.SnowflakeH\x00\x12.\n\x05kafka\x18\x08 \x01(\x0b\x32\x1d.fennel.proto.connector.KafkaH\x00\x12\x32\n\x07webhook\x18\t \x01(\x0b\x32\x1f.fennel.proto.connector.WebhookH\x00\x12\x32\n\x07kinesis\x18\n \x01(\x0b\x32\x1f.fennel.proto.connector.KinesisH\x00\x42\t\n\x07variant\"\xb8\x01\n\tReference\x12;\n\x06\x64\x62type\x18\x01 \x01(\x0e\x32+.fennel.proto.connector.Reference.ExtDBType\"n\n\tExtDBType\x12\t\n\x05MYSQL\x10\x00\x12\x0c\n\x08POSTGRES\x10\x01\x12\x06\n\x02S3\x10\x02\x12\t\n\x05KAFKA\x10\x03\x12\x0c\n\x08\x42IGQUERY\x10\x04\x12\r\n\tSNOWFLAKE\x10\x05\x12\x0b\n\x07WEBHOOK\x10\x06\x12\x0b\n\x07KINESIS\x10\x07\"\x17\n\x07Webhook\x12\x0c\n\x04name\x18\x01 \x01(\t\"j\n\x05MySQL\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0c\n\x04user\x18\x03 \x01(\t\x12\x10\n\x08password\x18\x04 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\r\x12\x13\n\x0bjdbc_params\x18\x06 \x01(\t\"m\n\x08Postgres\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0c\n\x04user\x18\x03 \x01(\t\x12\x10\n\x08password\x18\x04 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\r\x12\x13\n\x0bjdbc_params\x18\x06 \x01(\t\">\n\x02S3\x12\x1d\n\x15\x61ws_secret_access_key\x18\x01 \x01(\t\x12\x19\n\x11\x61ws_access_key_id\x18\x02 \x01(\t\"I\n\x08\x42igquery\x12\x0f\n\x07\x64\x61taset\x18\x01 \x01(\t\x12\x18\n\x10\x63redentials_json\x18\x02 \x01(\t\x12\x12\n\nproject_id\x18\x03 \x01(\t\"\x94\x01\n\tSnowflake\x12\x0f\n\x07\x61\x63\x63ount\x18\x01 \x01(\t\x12\x0c\n\x04user\x18\x02 \x01(\t\x12\x10\n\x08password\x18\x03 \x01(\t\x12\x0e\n\x06schema\x18\x04 \x01(\t\x12\x11\n\twarehouse\x18\x05 \x01(\t\x12\x0c\n\x04role\x18\x06 \x01(\t\x12\x13\n\x0bjdbc_params\x18\x07 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\t \x01(\t\"\x8c\x02\n\x05Kafka\x12\x19\n\x11\x62ootstrap_servers\x18\x01 \x01(\t\x12\x19\n\x11security_protocol\x18\x02 \x01(\t\x12\x16\n\x0esasl_mechanism\x18\x03 \x01(\t\x12\x1c\n\x10sasl_jaas_config\x18\x04 \x01(\tB\x02\x18\x01\x12\x1b\n\x13sasl_plain_username\x18\x05 \x01(\t\x12\x1b\n\x13sasl_plain_password\x18\x06 \x01(\t\x12\x14\n\x08group_id\x18\x07 \x01(\tB\x02\x18\x01\x12G\n#enable_ssl_certificate_verification\x18\x08 \x01(\x0b\x32\x1a.google.protobuf.BoolValue\"\x1b\n\x07Kinesis\x12\x10\n\x08role_arn\x18\x01 \x01(\t\"\xfd\x03\n\x08\x45xtTable\x12\x39\n\x0bmysql_table\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.MySQLTableH\x00\x12\x39\n\x08pg_table\x18\x02 \x01(\x0b\x32%.fennel.proto.connector.PostgresTableH\x00\x12\x33\n\x08s3_table\x18\x03 \x01(\x0b\x32\x1f.fennel.proto.connector.S3TableH\x00\x12\x39\n\x0bkafka_topic\x18\x04 \x01(\x0b\x32\".fennel.proto.connector.KafkaTopicH\x00\x12\x41\n\x0fsnowflake_table\x18\x05 \x01(\x0b\x32&.fennel.proto.connector.SnowflakeTableH\x00\x12?\n\x0e\x62igquery_table\x18\x06 \x01(\x0b\x32%.fennel.proto.connector.BigqueryTableH\x00\x12;\n\x08\x65ndpoint\x18\x07 \x01(\x0b\x32\'.fennel.proto.connector.WebhookEndpointH\x00\x12?\n\x0ekinesis_stream\x18\x08 \x01(\x0b\x32%.fennel.proto.connector.KinesisStreamH\x00\x42\t\n\x07variant\"Q\n\nMySQLTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"T\n\rPostgresTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"\x82\x01\n\x07S3Table\x12\x0e\n\x06\x62ucket\x18\x01 \x01(\t\x12\x13\n\x0bpath_prefix\x18\x02 \x01(\t\x12\x11\n\tdelimiter\x18\x04 \x01(\t\x12\x0e\n\x06\x66ormat\x18\x05 \x01(\t\x12/\n\x02\x64\x62\x18\x06 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\"L\n\nKafkaTopic\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\r\n\x05topic\x18\x02 \x01(\t\"T\n\rBigqueryTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"U\n\x0eSnowflakeTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"T\n\x0fWebhookEndpoint\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x10\n\x08\x65ndpoint\x18\x02 \x01(\t\"\xd3\x01\n\rKinesisStream\x12\x12\n\nstream_arn\x18\x01 \x01(\t\x12\x39\n\rinit_position\x18\x02 \x01(\x0e\x32\".fennel.proto.kinesis.InitPosition\x12\x32\n\x0einit_timestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06\x66ormat\x18\x04 \x01(\t\x12/\n\x02\x64\x62\x18\x05 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\"\xda\x01\n\x06Source\x12/\n\x05table\x18\x01 \x01(\x0b\x32 .fennel.proto.connector.ExtTable\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\t\x12(\n\x05\x65very\x18\x03 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x13\n\x06\x63ursor\x18\x04 \x01(\tH\x00\x88\x01\x01\x12+\n\x08lateness\x18\x05 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x17\n\x0ftimestamp_field\x18\x06 \x01(\tB\t\n\x07_cursor\"H\n\x04Sink\x12/\n\x05table\x18\x01 \x01(\x0b\x32 .fennel.proto.connector.ExtTable\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\tb\x06proto3')



_EXTDATABASE = DESCRIPTOR.message_types_by_name['ExtDatabase']
_REFERENCE = DESCRIPTOR.message_types_by_name['Reference']
_WEBHOOK = DESCRIPTOR.message_types_by_name['Webhook']
_MYSQL = DESCRIPTOR.message_types_by_name['MySQL']
_POSTGRES = DESCRIPTOR.message_types_by_name['Postgres']
_S3 = DESCRIPTOR.message_types_by_name['S3']
_BIGQUERY = DESCRIPTOR.message_types_by_name['Bigquery']
_SNOWFLAKE = DESCRIPTOR.message_types_by_name['Snowflake']
_KAFKA = DESCRIPTOR.message_types_by_name['Kafka']
_KINESIS = DESCRIPTOR.message_types_by_name['Kinesis']
_EXTTABLE = DESCRIPTOR.message_types_by_name['ExtTable']
_MYSQLTABLE = DESCRIPTOR.message_types_by_name['MySQLTable']
_POSTGRESTABLE = DESCRIPTOR.message_types_by_name['PostgresTable']
_S3TABLE = DESCRIPTOR.message_types_by_name['S3Table']
_KAFKATOPIC = DESCRIPTOR.message_types_by_name['KafkaTopic']
_BIGQUERYTABLE = DESCRIPTOR.message_types_by_name['BigqueryTable']
_SNOWFLAKETABLE = DESCRIPTOR.message_types_by_name['SnowflakeTable']
_WEBHOOKENDPOINT = DESCRIPTOR.message_types_by_name['WebhookEndpoint']
_KINESISSTREAM = DESCRIPTOR.message_types_by_name['KinesisStream']
_SOURCE = DESCRIPTOR.message_types_by_name['Source']
_SINK = DESCRIPTOR.message_types_by_name['Sink']
_REFERENCE_EXTDBTYPE = _REFERENCE.enum_types_by_name['ExtDBType']
ExtDatabase = _reflection.GeneratedProtocolMessageType('ExtDatabase', (_message.Message,), {
  'DESCRIPTOR' : _EXTDATABASE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.ExtDatabase)
  })
_sym_db.RegisterMessage(ExtDatabase)

Reference = _reflection.GeneratedProtocolMessageType('Reference', (_message.Message,), {
  'DESCRIPTOR' : _REFERENCE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Reference)
  })
_sym_db.RegisterMessage(Reference)

Webhook = _reflection.GeneratedProtocolMessageType('Webhook', (_message.Message,), {
  'DESCRIPTOR' : _WEBHOOK,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Webhook)
  })
_sym_db.RegisterMessage(Webhook)

MySQL = _reflection.GeneratedProtocolMessageType('MySQL', (_message.Message,), {
  'DESCRIPTOR' : _MYSQL,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.MySQL)
  })
_sym_db.RegisterMessage(MySQL)

Postgres = _reflection.GeneratedProtocolMessageType('Postgres', (_message.Message,), {
  'DESCRIPTOR' : _POSTGRES,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Postgres)
  })
_sym_db.RegisterMessage(Postgres)

S3 = _reflection.GeneratedProtocolMessageType('S3', (_message.Message,), {
  'DESCRIPTOR' : _S3,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.S3)
  })
_sym_db.RegisterMessage(S3)

Bigquery = _reflection.GeneratedProtocolMessageType('Bigquery', (_message.Message,), {
  'DESCRIPTOR' : _BIGQUERY,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Bigquery)
  })
_sym_db.RegisterMessage(Bigquery)

Snowflake = _reflection.GeneratedProtocolMessageType('Snowflake', (_message.Message,), {
  'DESCRIPTOR' : _SNOWFLAKE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Snowflake)
  })
_sym_db.RegisterMessage(Snowflake)

Kafka = _reflection.GeneratedProtocolMessageType('Kafka', (_message.Message,), {
  'DESCRIPTOR' : _KAFKA,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Kafka)
  })
_sym_db.RegisterMessage(Kafka)

Kinesis = _reflection.GeneratedProtocolMessageType('Kinesis', (_message.Message,), {
  'DESCRIPTOR' : _KINESIS,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Kinesis)
  })
_sym_db.RegisterMessage(Kinesis)

ExtTable = _reflection.GeneratedProtocolMessageType('ExtTable', (_message.Message,), {
  'DESCRIPTOR' : _EXTTABLE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.ExtTable)
  })
_sym_db.RegisterMessage(ExtTable)

MySQLTable = _reflection.GeneratedProtocolMessageType('MySQLTable', (_message.Message,), {
  'DESCRIPTOR' : _MYSQLTABLE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.MySQLTable)
  })
_sym_db.RegisterMessage(MySQLTable)

PostgresTable = _reflection.GeneratedProtocolMessageType('PostgresTable', (_message.Message,), {
  'DESCRIPTOR' : _POSTGRESTABLE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.PostgresTable)
  })
_sym_db.RegisterMessage(PostgresTable)

S3Table = _reflection.GeneratedProtocolMessageType('S3Table', (_message.Message,), {
  'DESCRIPTOR' : _S3TABLE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.S3Table)
  })
_sym_db.RegisterMessage(S3Table)

KafkaTopic = _reflection.GeneratedProtocolMessageType('KafkaTopic', (_message.Message,), {
  'DESCRIPTOR' : _KAFKATOPIC,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.KafkaTopic)
  })
_sym_db.RegisterMessage(KafkaTopic)

BigqueryTable = _reflection.GeneratedProtocolMessageType('BigqueryTable', (_message.Message,), {
  'DESCRIPTOR' : _BIGQUERYTABLE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.BigqueryTable)
  })
_sym_db.RegisterMessage(BigqueryTable)

SnowflakeTable = _reflection.GeneratedProtocolMessageType('SnowflakeTable', (_message.Message,), {
  'DESCRIPTOR' : _SNOWFLAKETABLE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.SnowflakeTable)
  })
_sym_db.RegisterMessage(SnowflakeTable)

WebhookEndpoint = _reflection.GeneratedProtocolMessageType('WebhookEndpoint', (_message.Message,), {
  'DESCRIPTOR' : _WEBHOOKENDPOINT,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.WebhookEndpoint)
  })
_sym_db.RegisterMessage(WebhookEndpoint)

KinesisStream = _reflection.GeneratedProtocolMessageType('KinesisStream', (_message.Message,), {
  'DESCRIPTOR' : _KINESISSTREAM,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.KinesisStream)
  })
_sym_db.RegisterMessage(KinesisStream)

Source = _reflection.GeneratedProtocolMessageType('Source', (_message.Message,), {
  'DESCRIPTOR' : _SOURCE,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Source)
  })
_sym_db.RegisterMessage(Source)

Sink = _reflection.GeneratedProtocolMessageType('Sink', (_message.Message,), {
  'DESCRIPTOR' : _SINK,
  '__module__' : 'connector_pb2'
  # @@protoc_insertion_point(class_scope:fennel.proto.connector.Sink)
  })
_sym_db.RegisterMessage(Sink)

if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _KAFKA.fields_by_name['sasl_jaas_config']._options = None
  _KAFKA.fields_by_name['sasl_jaas_config']._serialized_options = b'\030\001'
  _KAFKA.fields_by_name['group_id']._options = None
  _KAFKA.fields_by_name['group_id']._serialized_options = b'\030\001'
  _EXTDATABASE._serialized_start=156
  _EXTDATABASE._serialized_end=656
  _REFERENCE._serialized_start=659
  _REFERENCE._serialized_end=843
  _REFERENCE_EXTDBTYPE._serialized_start=733
  _REFERENCE_EXTDBTYPE._serialized_end=843
  _WEBHOOK._serialized_start=845
  _WEBHOOK._serialized_end=868
  _MYSQL._serialized_start=870
  _MYSQL._serialized_end=976
  _POSTGRES._serialized_start=978
  _POSTGRES._serialized_end=1087
  _S3._serialized_start=1089
  _S3._serialized_end=1151
  _BIGQUERY._serialized_start=1153
  _BIGQUERY._serialized_end=1226
  _SNOWFLAKE._serialized_start=1229
  _SNOWFLAKE._serialized_end=1377
  _KAFKA._serialized_start=1380
  _KAFKA._serialized_end=1648
  _KINESIS._serialized_start=1650
  _KINESIS._serialized_end=1677
  _EXTTABLE._serialized_start=1680
  _EXTTABLE._serialized_end=2189
  _MYSQLTABLE._serialized_start=2191
  _MYSQLTABLE._serialized_end=2272
  _POSTGRESTABLE._serialized_start=2274
  _POSTGRESTABLE._serialized_end=2358
  _S3TABLE._serialized_start=2361
  _S3TABLE._serialized_end=2491
  _KAFKATOPIC._serialized_start=2493
  _KAFKATOPIC._serialized_end=2569
  _BIGQUERYTABLE._serialized_start=2571
  _BIGQUERYTABLE._serialized_end=2655
  _SNOWFLAKETABLE._serialized_start=2657
  _SNOWFLAKETABLE._serialized_end=2742
  _WEBHOOKENDPOINT._serialized_start=2744
  _WEBHOOKENDPOINT._serialized_end=2828
  _KINESISSTREAM._serialized_start=2831
  _KINESISSTREAM._serialized_end=3042
  _SOURCE._serialized_start=3045
  _SOURCE._serialized_end=3263
  _SINK._serialized_start=3265
  _SINK._serialized_end=3337
# @@protoc_insertion_point(module_scope)
