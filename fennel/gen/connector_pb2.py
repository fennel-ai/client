# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: connector.proto
# Protobuf Python Version: 4.25.4
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
import fennel.gen.expression_pb2 as expression__pb2
import fennel.gen.kinesis_pb2 as kinesis__pb2
import fennel.gen.pycode_pb2 as pycode__pb2
import fennel.gen.schema_registry_pb2 as schema__registry__pb2
import fennel.gen.schema_pb2 as schema__pb2
import fennel.gen.secret_pb2 as secret__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0f\x63onnector.proto\x12\x16\x66\x65nnel.proto.connector\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x10\x65xpression.proto\x1a\rkinesis.proto\x1a\x0cpycode.proto\x1a\x15schema_registry.proto\x1a\x0cschema.proto\x1a\x0csecret.proto\"\x8c\x05\n\x0b\x45xtDatabase\x12\x0c\n\x04name\x18\x01 \x01(\t\x12.\n\x05mysql\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.connector.MySQLH\x00\x12\x34\n\x08postgres\x18\x03 \x01(\x0b\x32 .fennel.proto.connector.PostgresH\x00\x12\x36\n\treference\x18\x04 \x01(\x0b\x32!.fennel.proto.connector.ReferenceH\x00\x12(\n\x02s3\x18\x05 \x01(\x0b\x32\x1a.fennel.proto.connector.S3H\x00\x12\x34\n\x08\x62igquery\x18\x06 \x01(\x0b\x32 .fennel.proto.connector.BigqueryH\x00\x12\x36\n\tsnowflake\x18\x07 \x01(\x0b\x32!.fennel.proto.connector.SnowflakeH\x00\x12.\n\x05kafka\x18\x08 \x01(\x0b\x32\x1d.fennel.proto.connector.KafkaH\x00\x12\x32\n\x07webhook\x18\t \x01(\x0b\x32\x1f.fennel.proto.connector.WebhookH\x00\x12\x32\n\x07kinesis\x18\n \x01(\x0b\x32\x1f.fennel.proto.connector.KinesisH\x00\x12\x34\n\x08redshift\x18\x0b \x01(\x0b\x32 .fennel.proto.connector.RedshiftH\x00\x12.\n\x05mongo\x18\x0c \x01(\x0b\x32\x1d.fennel.proto.connector.MongoH\x00\x12\x30\n\x06pubsub\x18\r \x01(\x0b\x32\x1e.fennel.proto.connector.PubSubH\x00\x42\t\n\x07variant\"M\n\x0cPubSubFormat\x12\x32\n\x04json\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.JsonFormatH\x00\x42\t\n\x07variant\"\xbc\x01\n\x0bKafkaFormat\x12\x32\n\x04json\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.JsonFormatH\x00\x12\x32\n\x04\x61vro\x18\x02 \x01(\x0b\x32\".fennel.proto.connector.AvroFormatH\x00\x12:\n\x08protobuf\x18\x03 \x01(\x0b\x32&.fennel.proto.connector.ProtobufFormatH\x00\x42\t\n\x07variant\"\x0c\n\nJsonFormat\"S\n\nAvroFormat\x12\x45\n\x0fschema_registry\x18\x01 \x01(\x0b\x32,.fennel.proto.schema_registry.SchemaRegistry\"W\n\x0eProtobufFormat\x12\x45\n\x0fschema_registry\x18\x01 \x01(\x0b\x32,.fennel.proto.schema_registry.SchemaRegistry\"\xde\x01\n\tReference\x12;\n\x06\x64\x62type\x18\x01 \x01(\x0e\x32+.fennel.proto.connector.Reference.ExtDBType\"\x93\x01\n\tExtDBType\x12\t\n\x05MYSQL\x10\x00\x12\x0c\n\x08POSTGRES\x10\x01\x12\x06\n\x02S3\x10\x02\x12\t\n\x05KAFKA\x10\x03\x12\x0c\n\x08\x42IGQUERY\x10\x04\x12\r\n\tSNOWFLAKE\x10\x05\x12\x0b\n\x07WEBHOOK\x10\x06\x12\x0b\n\x07KINESIS\x10\x07\x12\x0c\n\x08REDSHIFT\x10\x08\x12\t\n\x05MONGO\x10\t\x12\n\n\x06PUBSUB\x10\n\"E\n\x07Webhook\x12\x0c\n\x04name\x18\x01 \x01(\t\x12,\n\tretention\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\"\x8c\x02\n\x05MySQL\x12\x0e\n\x04user\x18\x03 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x07 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x04 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\r\x12\x13\n\x0bjdbc_params\x18\x06 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\x8f\x02\n\x08Postgres\x12\x0e\n\x04user\x18\x03 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x07 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x04 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\r\x12\x13\n\x0bjdbc_params\x18\x06 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\xb0\x02\n\x02S3\x12\x1f\n\x15\x61ws_secret_access_key\x18\x01 \x01(\tH\x00\x12\x46\n\x1c\x61ws_secret_access_key_secret\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x1b\n\x11\x61ws_access_key_id\x18\x02 \x01(\tH\x01\x12\x42\n\x18\x61ws_access_key_id_secret\x18\x05 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x15\n\x08role_arn\x18\x03 \x01(\tH\x02\x88\x01\x01\x42\x1f\n\x1d\x61ws_secret_access_key_variantB\x1b\n\x19\x61ws_access_key_id_variantB\x0b\n\t_role_arn\"\xb6\x01\n\x08\x42igquery\x12\x1d\n\x13service_account_key\x18\x02 \x01(\tH\x00\x12\x44\n\x1aservice_account_key_secret\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\ndataset_id\x18\x01 \x01(\t\x12\x12\n\nproject_id\x18\x03 \x01(\tB\x1d\n\x1bservice_account_key_variant\"\xa1\x02\n\tSnowflake\x12\x0e\n\x04user\x18\x02 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x03 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\t \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0f\n\x07\x61\x63\x63ount\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x04 \x01(\t\x12\x11\n\twarehouse\x18\x05 \x01(\t\x12\x0c\n\x04role\x18\x06 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x07 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\xf9\x02\n\x05Kafka\x12\x1d\n\x13sasl_plain_username\x18\x05 \x01(\tH\x00\x12>\n\x14sasl_username_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x1d\n\x13sasl_plain_password\x18\x06 \x01(\tH\x01\x12>\n\x14sasl_password_secret\x18\t \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x19\n\x11\x62ootstrap_servers\x18\x01 \x01(\t\x12\x19\n\x11security_protocol\x18\x02 \x01(\t\x12\x16\n\x0esasl_mechanism\x18\x03 \x01(\t\x12\x1c\n\x10sasl_jaas_config\x18\x04 \x01(\tB\x02\x18\x01\x12\x14\n\x08group_id\x18\x07 \x01(\tB\x02\x18\x01\x42\x17\n\x15sasl_username_variantB\x17\n\x15sasl_password_variant\"\x1b\n\x07Kinesis\x12\x10\n\x08role_arn\x18\x01 \x01(\t\"\xd3\x01\n\x0b\x43redentials\x12\x12\n\x08username\x18\x01 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x03 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x02 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x42\x12\n\x10username_variantB\x12\n\x10password_variant\"}\n\x16RedshiftAuthentication\x12\x1c\n\x12s3_access_role_arn\x18\x01 \x01(\tH\x00\x12:\n\x0b\x63redentials\x18\x02 \x01(\x0b\x32#.fennel.proto.connector.CredentialsH\x00\x42\t\n\x07variant\"\x99\x01\n\x08Redshift\x12\x10\n\x08\x64\x61tabase\x18\x01 \x01(\t\x12\x0c\n\x04host\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x03 \x01(\r\x12\x0e\n\x06schema\x18\x04 \x01(\t\x12O\n\x17redshift_authentication\x18\x05 \x01(\x0b\x32..fennel.proto.connector.RedshiftAuthentication\"\xe9\x01\n\x05Mongo\x12\x0e\n\x04user\x18\x03 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x05 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x04 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x06 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\xa0\x01\n\x06PubSub\x12\x1d\n\x13service_account_key\x18\x02 \x01(\tH\x00\x12\x44\n\x1aservice_account_key_secret\x18\x03 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\nproject_id\x18\x01 \x01(\tB\x1d\n\x1bservice_account_key_variant\"\xc0\x05\n\x08\x45xtTable\x12\x39\n\x0bmysql_table\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.MySQLTableH\x00\x12\x39\n\x08pg_table\x18\x02 \x01(\x0b\x32%.fennel.proto.connector.PostgresTableH\x00\x12\x33\n\x08s3_table\x18\x03 \x01(\x0b\x32\x1f.fennel.proto.connector.S3TableH\x00\x12\x39\n\x0bkafka_topic\x18\x04 \x01(\x0b\x32\".fennel.proto.connector.KafkaTopicH\x00\x12\x41\n\x0fsnowflake_table\x18\x05 \x01(\x0b\x32&.fennel.proto.connector.SnowflakeTableH\x00\x12?\n\x0e\x62igquery_table\x18\x06 \x01(\x0b\x32%.fennel.proto.connector.BigqueryTableH\x00\x12;\n\x08\x65ndpoint\x18\x07 \x01(\x0b\x32\'.fennel.proto.connector.WebhookEndpointH\x00\x12?\n\x0ekinesis_stream\x18\x08 \x01(\x0b\x32%.fennel.proto.connector.KinesisStreamH\x00\x12?\n\x0eredshift_table\x18\t \x01(\x0b\x32%.fennel.proto.connector.RedshiftTableH\x00\x12\x43\n\x10mongo_collection\x18\n \x01(\x0b\x32\'.fennel.proto.connector.MongoCollectionH\x00\x12;\n\x0cpubsub_topic\x18\x0b \x01(\x0b\x32#.fennel.proto.connector.PubSubTopicH\x00\x42\t\n\x07variant\"Q\n\nMySQLTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"z\n\rPostgresTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\x12\x16\n\tslot_name\x18\x03 \x01(\tH\x00\x88\x01\x01\x42\x0c\n\n_slot_name\"\xf7\x01\n\x07S3Table\x12\x0e\n\x06\x62ucket\x18\x01 \x01(\t\x12\x13\n\x0bpath_prefix\x18\x02 \x01(\t\x12\x11\n\tdelimiter\x18\x04 \x01(\t\x12\x0e\n\x06\x66ormat\x18\x05 \x01(\t\x12/\n\x02\x64\x62\x18\x06 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\npre_sorted\x18\x07 \x01(\x08\x12\x13\n\x0bpath_suffix\x18\x08 \x01(\t\x12.\n\x06spread\x18\x03 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x88\x01\x01\x12\x0f\n\x07headers\x18\t \x03(\tB\t\n\x07_spread\"\x81\x01\n\nKafkaTopic\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\r\n\x05topic\x18\x02 \x01(\t\x12\x33\n\x06\x66ormat\x18\x03 \x01(\x0b\x32#.fennel.proto.connector.KafkaFormat\"T\n\rBigqueryTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"U\n\x0eSnowflakeTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"\x81\x01\n\x0fWebhookEndpoint\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x10\n\x08\x65ndpoint\x18\x02 \x01(\t\x12+\n\x08\x64uration\x18\x03 \x01(\x0b\x32\x19.google.protobuf.Duration\"\xd3\x01\n\rKinesisStream\x12\x12\n\nstream_arn\x18\x01 \x01(\t\x12\x39\n\rinit_position\x18\x02 \x01(\x0e\x32\".fennel.proto.kinesis.InitPosition\x12\x32\n\x0einit_timestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06\x66ormat\x18\x04 \x01(\t\x12/\n\x02\x64\x62\x18\x05 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\"T\n\rRedshiftTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"\x86\x01\n\x0bPubSubTopic\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x10\n\x08topic_id\x18\x02 \x01(\t\x12\x34\n\x06\x66ormat\x18\x03 \x01(\x0b\x32$.fennel.proto.connector.PubSubFormat\"\x9e\x01\n\x04\x45val\x12+\n\x06schema\x18\x01 \x01(\x0b\x32\x1b.fennel.proto.schema.Schema\x12-\n\x04\x65xpr\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.ExprH\x00\x12-\n\x06pycode\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCodeH\x00\x42\x0b\n\teval_type\"\x83\x01\n\x0cPreProcValue\x12\r\n\x03ref\x18\x01 \x01(\tH\x00\x12+\n\x05value\x18\x02 \x01(\x0b\x32\x1a.fennel.proto.schema.ValueH\x00\x12,\n\x04\x65val\x18\x03 \x01(\x0b\x32\x1c.fennel.proto.connector.EvalH\x00\x42\t\n\x07variant\"t\n\nWhereValue\x12-\n\x06pycode\x18\x01 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCodeH\x00\x12,\n\x04\x65val\x18\x02 \x01(\x0b\x32\x1c.fennel.proto.connector.EvalH\x00\x42\t\n\x07variant\"[\n\x0fMongoCollection\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x17\n\x0f\x63ollection_name\x18\x02 \x01(\t\"2\n\x0cSnapshotData\x12\x0e\n\x06marker\x18\x01 \x01(\t\x12\x12\n\nnum_retain\x18\x02 \x01(\r\"\r\n\x0bIncremental\"\n\n\x08Recreate\"\xbc\x01\n\x05Style\x12:\n\x0bincremental\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.IncrementalH\x00\x12\x34\n\x08recreate\x18\x02 \x01(\x0b\x32 .fennel.proto.connector.RecreateH\x00\x12\x38\n\x08snapshot\x18\x03 \x01(\x0b\x32$.fennel.proto.connector.SnapshotDataH\x00\x42\x07\n\x05Style\"\x83\x06\n\x06Source\x12/\n\x05table\x18\x01 \x01(\x0b\x32 .fennel.proto.connector.ExtTable\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\t\x12\x12\n\nds_version\x18\x03 \x01(\r\x12(\n\x05\x65very\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x13\n\x06\x63ursor\x18\x05 \x01(\tH\x00\x88\x01\x01\x12+\n\x08\x64isorder\x18\x06 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x17\n\x0ftimestamp_field\x18\x07 \x01(\t\x12\x30\n\x03\x63\x64\x63\x18\x08 \x01(\x0e\x32#.fennel.proto.connector.CDCStrategy\x12\x31\n\rstarting_from\x18\t \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12=\n\x08pre_proc\x18\n \x03(\x0b\x32+.fennel.proto.connector.Source.PreProcEntry\x12\x0f\n\x07version\x18\x0b \x01(\r\x12\x0f\n\x07\x62ounded\x18\x0c \x01(\x08\x12\x30\n\x08idleness\x18\r \x01(\x0b\x32\x19.google.protobuf.DurationH\x01\x88\x01\x01\x12)\n\x05until\x18\x0e \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x34\n\x06\x66ilter\x18\x0f \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCodeB\x02\x18\x01H\x02\x88\x01\x01\x12<\n\x0bwhere_value\x18\x10 \x01(\x0b\x32\".fennel.proto.connector.WhereValueH\x03\x88\x01\x01\x1aT\n\x0cPreProcEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x33\n\x05value\x18\x02 \x01(\x0b\x32$.fennel.proto.connector.PreProcValue:\x02\x38\x01\x42\t\n\x07_cursorB\x0b\n\t_idlenessB\t\n\x07_filterB\x0e\n\x0c_where_value\"\xee\x03\n\x04Sink\x12/\n\x05table\x18\x01 \x01(\x0b\x32 .fennel.proto.connector.ExtTable\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\t\x12\x12\n\nds_version\x18\x03 \x01(\r\x12\x35\n\x03\x63\x64\x63\x18\x04 \x01(\x0e\x32#.fennel.proto.connector.CDCStrategyH\x00\x88\x01\x01\x12(\n\x05\x65very\x18\x05 \x01(\x0b\x32\x19.google.protobuf.Duration\x12/\n\x03how\x18\x06 \x01(\x0b\x32\x1d.fennel.proto.connector.StyleH\x01\x88\x01\x01\x12\x0e\n\x06\x63reate\x18\x07 \x01(\x08\x12:\n\x07renames\x18\x08 \x03(\x0b\x32).fennel.proto.connector.Sink.RenamesEntry\x12.\n\x05since\x18\t \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x02\x88\x01\x01\x12.\n\x05until\x18\n \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x03\x88\x01\x01\x1a.\n\x0cRenamesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x06\n\x04_cdcB\x06\n\x04_howB\x08\n\x06_sinceB\x08\n\x06_until*K\n\x0b\x43\x44\x43Strategy\x12\n\n\x06\x41ppend\x10\x00\x12\n\n\x06Upsert\x10\x01\x12\x0c\n\x08\x44\x65\x62\x65zium\x10\x02\x12\n\n\x06Native\x10\x03\x12\n\n\x06\x44\x65lete\x10\x04\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'connector_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_KAFKA'].fields_by_name['sasl_jaas_config']._options = None
  _globals['_KAFKA'].fields_by_name['sasl_jaas_config']._serialized_options = b'\030\001'
  _globals['_KAFKA'].fields_by_name['group_id']._options = None
  _globals['_KAFKA'].fields_by_name['group_id']._serialized_options = b'\030\001'
  _globals['_SOURCE_PREPROCENTRY']._options = None
  _globals['_SOURCE_PREPROCENTRY']._serialized_options = b'8\001'
  _globals['_SOURCE'].fields_by_name['filter']._options = None
  _globals['_SOURCE'].fields_by_name['filter']._serialized_options = b'\030\001'
  _globals['_SINK_RENAMESENTRY']._options = None
  _globals['_SINK_RENAMESENTRY']._serialized_options = b'8\001'
  _globals['_CDCSTRATEGY']._serialized_start=8334
  _globals['_CDCSTRATEGY']._serialized_end=8409
  _globals['_EXTDATABASE']._serialized_start=207
  _globals['_EXTDATABASE']._serialized_end=859
  _globals['_PUBSUBFORMAT']._serialized_start=861
  _globals['_PUBSUBFORMAT']._serialized_end=938
  _globals['_KAFKAFORMAT']._serialized_start=941
  _globals['_KAFKAFORMAT']._serialized_end=1129
  _globals['_JSONFORMAT']._serialized_start=1131
  _globals['_JSONFORMAT']._serialized_end=1143
  _globals['_AVROFORMAT']._serialized_start=1145
  _globals['_AVROFORMAT']._serialized_end=1228
  _globals['_PROTOBUFFORMAT']._serialized_start=1230
  _globals['_PROTOBUFFORMAT']._serialized_end=1317
  _globals['_REFERENCE']._serialized_start=1320
  _globals['_REFERENCE']._serialized_end=1542
  _globals['_REFERENCE_EXTDBTYPE']._serialized_start=1395
  _globals['_REFERENCE_EXTDBTYPE']._serialized_end=1542
  _globals['_WEBHOOK']._serialized_start=1544
  _globals['_WEBHOOK']._serialized_end=1613
  _globals['_MYSQL']._serialized_start=1616
  _globals['_MYSQL']._serialized_end=1884
  _globals['_POSTGRES']._serialized_start=1887
  _globals['_POSTGRES']._serialized_end=2158
  _globals['_S3']._serialized_start=2161
  _globals['_S3']._serialized_end=2465
  _globals['_BIGQUERY']._serialized_start=2468
  _globals['_BIGQUERY']._serialized_end=2650
  _globals['_SNOWFLAKE']._serialized_start=2653
  _globals['_SNOWFLAKE']._serialized_end=2942
  _globals['_KAFKA']._serialized_start=2945
  _globals['_KAFKA']._serialized_end=3322
  _globals['_KINESIS']._serialized_start=3324
  _globals['_KINESIS']._serialized_end=3351
  _globals['_CREDENTIALS']._serialized_start=3354
  _globals['_CREDENTIALS']._serialized_end=3565
  _globals['_REDSHIFTAUTHENTICATION']._serialized_start=3567
  _globals['_REDSHIFTAUTHENTICATION']._serialized_end=3692
  _globals['_REDSHIFT']._serialized_start=3695
  _globals['_REDSHIFT']._serialized_end=3848
  _globals['_MONGO']._serialized_start=3851
  _globals['_MONGO']._serialized_end=4084
  _globals['_PUBSUB']._serialized_start=4087
  _globals['_PUBSUB']._serialized_end=4247
  _globals['_EXTTABLE']._serialized_start=4250
  _globals['_EXTTABLE']._serialized_end=4954
  _globals['_MYSQLTABLE']._serialized_start=4956
  _globals['_MYSQLTABLE']._serialized_end=5037
  _globals['_POSTGRESTABLE']._serialized_start=5039
  _globals['_POSTGRESTABLE']._serialized_end=5161
  _globals['_S3TABLE']._serialized_start=5164
  _globals['_S3TABLE']._serialized_end=5411
  _globals['_KAFKATOPIC']._serialized_start=5414
  _globals['_KAFKATOPIC']._serialized_end=5543
  _globals['_BIGQUERYTABLE']._serialized_start=5545
  _globals['_BIGQUERYTABLE']._serialized_end=5629
  _globals['_SNOWFLAKETABLE']._serialized_start=5631
  _globals['_SNOWFLAKETABLE']._serialized_end=5716
  _globals['_WEBHOOKENDPOINT']._serialized_start=5719
  _globals['_WEBHOOKENDPOINT']._serialized_end=5848
  _globals['_KINESISSTREAM']._serialized_start=5851
  _globals['_KINESISSTREAM']._serialized_end=6062
  _globals['_REDSHIFTTABLE']._serialized_start=6064
  _globals['_REDSHIFTTABLE']._serialized_end=6148
  _globals['_PUBSUBTOPIC']._serialized_start=6151
  _globals['_PUBSUBTOPIC']._serialized_end=6285
  _globals['_EVAL']._serialized_start=6288
  _globals['_EVAL']._serialized_end=6446
  _globals['_PREPROCVALUE']._serialized_start=6449
  _globals['_PREPROCVALUE']._serialized_end=6580
  _globals['_WHEREVALUE']._serialized_start=6582
  _globals['_WHEREVALUE']._serialized_end=6698
  _globals['_MONGOCOLLECTION']._serialized_start=6700
  _globals['_MONGOCOLLECTION']._serialized_end=6791
  _globals['_SNAPSHOTDATA']._serialized_start=6793
  _globals['_SNAPSHOTDATA']._serialized_end=6843
  _globals['_INCREMENTAL']._serialized_start=6845
  _globals['_INCREMENTAL']._serialized_end=6858
  _globals['_RECREATE']._serialized_start=6860
  _globals['_RECREATE']._serialized_end=6870
  _globals['_STYLE']._serialized_start=6873
  _globals['_STYLE']._serialized_end=7061
  _globals['_SOURCE']._serialized_start=7064
  _globals['_SOURCE']._serialized_end=7835
  _globals['_SOURCE_PREPROCENTRY']._serialized_start=7700
  _globals['_SOURCE_PREPROCENTRY']._serialized_end=7784
  _globals['_SINK']._serialized_start=7838
  _globals['_SINK']._serialized_end=8332
  _globals['_SINK_RENAMESENTRY']._serialized_start=8250
  _globals['_SINK_RENAMESENTRY']._serialized_end=8296
# @@protoc_insertion_point(module_scope)
