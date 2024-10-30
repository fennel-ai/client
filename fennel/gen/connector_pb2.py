# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: connector.proto
# Protobuf Python Version: 5.26.1
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


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0f\x63onnector.proto\x12\x16\x66\x65nnel.proto.connector\x1a\x1egoogle/protobuf/duration.proto\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x10\x65xpression.proto\x1a\rkinesis.proto\x1a\x0cpycode.proto\x1a\x15schema_registry.proto\x1a\x0cschema.proto\x1a\x0csecret.proto\"\xba\x05\n\x0b\x45xtDatabase\x12\x0c\n\x04name\x18\x01 \x01(\t\x12.\n\x05mysql\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.connector.MySQLH\x00\x12\x34\n\x08postgres\x18\x03 \x01(\x0b\x32 .fennel.proto.connector.PostgresH\x00\x12\x36\n\treference\x18\x04 \x01(\x0b\x32!.fennel.proto.connector.ReferenceH\x00\x12(\n\x02s3\x18\x05 \x01(\x0b\x32\x1a.fennel.proto.connector.S3H\x00\x12\x34\n\x08\x62igquery\x18\x06 \x01(\x0b\x32 .fennel.proto.connector.BigqueryH\x00\x12\x36\n\tsnowflake\x18\x07 \x01(\x0b\x32!.fennel.proto.connector.SnowflakeH\x00\x12.\n\x05kafka\x18\x08 \x01(\x0b\x32\x1d.fennel.proto.connector.KafkaH\x00\x12\x32\n\x07webhook\x18\t \x01(\x0b\x32\x1f.fennel.proto.connector.WebhookH\x00\x12\x32\n\x07kinesis\x18\n \x01(\x0b\x32\x1f.fennel.proto.connector.KinesisH\x00\x12\x34\n\x08redshift\x18\x0b \x01(\x0b\x32 .fennel.proto.connector.RedshiftH\x00\x12.\n\x05mongo\x18\x0c \x01(\x0b\x32\x1d.fennel.proto.connector.MongoH\x00\x12\x30\n\x06pubsub\x18\r \x01(\x0b\x32\x1e.fennel.proto.connector.PubSubH\x00\x12,\n\x04http\x18\x0e \x01(\x0b\x32\x1c.fennel.proto.connector.HttpH\x00\x42\t\n\x07variant\"?\n\x10SamplingStrategy\x12\x15\n\rsampling_rate\x18\x01 \x01(\x01\x12\x14\n\x0c\x63olumns_used\x18\x02 \x03(\t\"M\n\x0cPubSubFormat\x12\x32\n\x04json\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.JsonFormatH\x00\x42\t\n\x07variant\"\xbc\x01\n\x0bKafkaFormat\x12\x32\n\x04json\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.JsonFormatH\x00\x12\x32\n\x04\x61vro\x18\x02 \x01(\x0b\x32\".fennel.proto.connector.AvroFormatH\x00\x12:\n\x08protobuf\x18\x03 \x01(\x0b\x32&.fennel.proto.connector.ProtobufFormatH\x00\x42\t\n\x07variant\"\x10\n\x0e\x44\x65\x62\x65ziumFormat\"\x0c\n\nJsonFormat\"S\n\nAvroFormat\x12\x45\n\x0fschema_registry\x18\x01 \x01(\x0b\x32,.fennel.proto.schema_registry.SchemaRegistry\"W\n\x0eProtobufFormat\x12\x45\n\x0fschema_registry\x18\x01 \x01(\x0b\x32,.fennel.proto.schema_registry.SchemaRegistry\"\xde\x01\n\tReference\x12;\n\x06\x64\x62type\x18\x01 \x01(\x0e\x32+.fennel.proto.connector.Reference.ExtDBType\"\x93\x01\n\tExtDBType\x12\t\n\x05MYSQL\x10\x00\x12\x0c\n\x08POSTGRES\x10\x01\x12\x06\n\x02S3\x10\x02\x12\t\n\x05KAFKA\x10\x03\x12\x0c\n\x08\x42IGQUERY\x10\x04\x12\r\n\tSNOWFLAKE\x10\x05\x12\x0b\n\x07WEBHOOK\x10\x06\x12\x0b\n\x07KINESIS\x10\x07\x12\x0c\n\x08REDSHIFT\x10\x08\x12\t\n\x05MONGO\x10\t\x12\n\n\x06PUBSUB\x10\n\"E\n\x07Webhook\x12\x0c\n\x04name\x18\x01 \x01(\t\x12,\n\tretention\x18\x02 \x01(\x0b\x32\x19.google.protobuf.Duration\"\x8c\x02\n\x05MySQL\x12\x0e\n\x04user\x18\x03 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x07 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x04 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\r\x12\x13\n\x0bjdbc_params\x18\x06 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\x8f\x02\n\x08Postgres\x12\x0e\n\x04user\x18\x03 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x07 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x04 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x05 \x01(\r\x12\x13\n\x0bjdbc_params\x18\x06 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\xb0\x02\n\x02S3\x12\x1f\n\x15\x61ws_secret_access_key\x18\x01 \x01(\tH\x00\x12\x46\n\x1c\x61ws_secret_access_key_secret\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x1b\n\x11\x61ws_access_key_id\x18\x02 \x01(\tH\x01\x12\x42\n\x18\x61ws_access_key_id_secret\x18\x05 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x15\n\x08role_arn\x18\x03 \x01(\tH\x02\x88\x01\x01\x42\x1f\n\x1d\x61ws_secret_access_key_variantB\x1b\n\x19\x61ws_access_key_id_variantB\x0b\n\t_role_arn\"\xb6\x01\n\x08\x42igquery\x12\x1d\n\x13service_account_key\x18\x02 \x01(\tH\x00\x12\x44\n\x1aservice_account_key_secret\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\ndataset_id\x18\x01 \x01(\t\x12\x12\n\nproject_id\x18\x03 \x01(\tB\x1d\n\x1bservice_account_key_variant\"\xa1\x02\n\tSnowflake\x12\x0e\n\x04user\x18\x02 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x03 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\t \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0f\n\x07\x61\x63\x63ount\x18\x01 \x01(\t\x12\x0e\n\x06schema\x18\x04 \x01(\t\x12\x11\n\twarehouse\x18\x05 \x01(\t\x12\x0c\n\x04role\x18\x06 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x07 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\xf9\x02\n\x05Kafka\x12\x1d\n\x13sasl_plain_username\x18\x05 \x01(\tH\x00\x12>\n\x14sasl_username_secret\x18\x08 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x1d\n\x13sasl_plain_password\x18\x06 \x01(\tH\x01\x12>\n\x14sasl_password_secret\x18\t \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x19\n\x11\x62ootstrap_servers\x18\x01 \x01(\t\x12\x19\n\x11security_protocol\x18\x02 \x01(\t\x12\x16\n\x0esasl_mechanism\x18\x03 \x01(\t\x12\x1c\n\x10sasl_jaas_config\x18\x04 \x01(\tB\x02\x18\x01\x12\x14\n\x08group_id\x18\x07 \x01(\tB\x02\x18\x01\x42\x17\n\x15sasl_username_variantB\x17\n\x15sasl_password_variant\"\x1b\n\x07Kinesis\x12\x10\n\x08role_arn\x18\x01 \x01(\t\"\xd3\x01\n\x0b\x43redentials\x12\x12\n\x08username\x18\x01 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x03 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x02 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x42\x12\n\x10username_variantB\x12\n\x10password_variant\"}\n\x16RedshiftAuthentication\x12\x1c\n\x12s3_access_role_arn\x18\x01 \x01(\tH\x00\x12:\n\x0b\x63redentials\x18\x02 \x01(\x0b\x32#.fennel.proto.connector.CredentialsH\x00\x42\t\n\x07variant\"\x99\x01\n\x08Redshift\x12\x10\n\x08\x64\x61tabase\x18\x01 \x01(\t\x12\x0c\n\x04host\x18\x02 \x01(\t\x12\x0c\n\x04port\x18\x03 \x01(\r\x12\x0e\n\x06schema\x18\x04 \x01(\t\x12O\n\x17redshift_authentication\x18\x05 \x01(\x0b\x32..fennel.proto.connector.RedshiftAuthentication\"\xe9\x01\n\x05Mongo\x12\x0e\n\x04user\x18\x03 \x01(\tH\x00\x12\x39\n\x0fusername_secret\x18\x05 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\x08password\x18\x04 \x01(\tH\x01\x12\x39\n\x0fpassword_secret\x18\x06 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x01\x12\x0c\n\x04host\x18\x01 \x01(\t\x12\x10\n\x08\x64\x61tabase\x18\x02 \x01(\tB\x12\n\x10username_variantB\x12\n\x10password_variant\"\xa0\x01\n\x06PubSub\x12\x1d\n\x13service_account_key\x18\x02 \x01(\tH\x00\x12\x44\n\x1aservice_account_key_secret\x18\x03 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x12\n\nproject_id\x18\x01 \x01(\tB\x1d\n\x1bservice_account_key_variant\"m\n\x0b\x43\x65rtificate\x12\x11\n\x07\x63\x61_cert\x18\x01 \x01(\tH\x00\x12\x38\n\x0e\x63\x61_cert_secret\x18\x02 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x42\x11\n\x0f\x63\x61_cert_variant\"\xa1\x01\n\x04Http\x12\x0e\n\x04host\x18\x01 \x01(\tH\x00\x12\x35\n\x0bhost_secret\x18\x02 \x01(\x0b\x32\x1e.fennel.proto.secret.SecretRefH\x00\x12\x0f\n\x07healthz\x18\x03 \x01(\t\x12\x31\n\x04\x61uth\x18\x04 \x01(\x0b\x32#.fennel.proto.connector.CertificateB\x0e\n\x0chost_variant\"\xf7\x05\n\x08\x45xtTable\x12\x39\n\x0bmysql_table\x18\x01 \x01(\x0b\x32\".fennel.proto.connector.MySQLTableH\x00\x12\x39\n\x08pg_table\x18\x02 \x01(\x0b\x32%.fennel.proto.connector.PostgresTableH\x00\x12\x33\n\x08s3_table\x18\x03 \x01(\x0b\x32\x1f.fennel.proto.connector.S3TableH\x00\x12\x39\n\x0bkafka_topic\x18\x04 \x01(\x0b\x32\".fennel.proto.connector.KafkaTopicH\x00\x12\x41\n\x0fsnowflake_table\x18\x05 \x01(\x0b\x32&.fennel.proto.connector.SnowflakeTableH\x00\x12?\n\x0e\x62igquery_table\x18\x06 \x01(\x0b\x32%.fennel.proto.connector.BigqueryTableH\x00\x12;\n\x08\x65ndpoint\x18\x07 \x01(\x0b\x32\'.fennel.proto.connector.WebhookEndpointH\x00\x12?\n\x0ekinesis_stream\x18\x08 \x01(\x0b\x32%.fennel.proto.connector.KinesisStreamH\x00\x12?\n\x0eredshift_table\x18\t \x01(\x0b\x32%.fennel.proto.connector.RedshiftTableH\x00\x12\x43\n\x10mongo_collection\x18\n \x01(\x0b\x32\'.fennel.proto.connector.MongoCollectionH\x00\x12;\n\x0cpubsub_topic\x18\x0b \x01(\x0b\x32#.fennel.proto.connector.PubSubTopicH\x00\x12\x35\n\thttp_path\x18\x0c \x01(\x0b\x32 .fennel.proto.connector.HttpPathH\x00\x42\t\n\x07variant\"Q\n\nMySQLTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"z\n\rPostgresTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\x12\x16\n\tslot_name\x18\x03 \x01(\tH\x00\x88\x01\x01\x42\x0c\n\n_slot_name\"\xf7\x01\n\x07S3Table\x12\x0e\n\x06\x62ucket\x18\x01 \x01(\t\x12\x13\n\x0bpath_prefix\x18\x02 \x01(\t\x12\x11\n\tdelimiter\x18\x04 \x01(\t\x12\x0e\n\x06\x66ormat\x18\x05 \x01(\t\x12/\n\x02\x64\x62\x18\x06 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\npre_sorted\x18\x07 \x01(\x08\x12\x13\n\x0bpath_suffix\x18\x08 \x01(\t\x12.\n\x06spread\x18\x03 \x01(\x0b\x32\x19.google.protobuf.DurationH\x00\x88\x01\x01\x12\x0f\n\x07headers\x18\t \x03(\tB\t\n\x07_spread\"\x81\x01\n\nKafkaTopic\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\r\n\x05topic\x18\x02 \x01(\t\x12\x33\n\x06\x66ormat\x18\x03 \x01(\x0b\x32#.fennel.proto.connector.KafkaFormat\"T\n\rBigqueryTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"U\n\x0eSnowflakeTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"\x81\x01\n\x0fWebhookEndpoint\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x10\n\x08\x65ndpoint\x18\x02 \x01(\t\x12+\n\x08\x64uration\x18\x03 \x01(\x0b\x32\x19.google.protobuf.Duration\"\xd3\x01\n\rKinesisStream\x12\x12\n\nstream_arn\x18\x01 \x01(\t\x12\x39\n\rinit_position\x18\x02 \x01(\x0e\x32\".fennel.proto.kinesis.InitPosition\x12\x32\n\x0einit_timestamp\x18\x03 \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x0e\n\x06\x66ormat\x18\x04 \x01(\t\x12/\n\x02\x64\x62\x18\x05 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\"T\n\rRedshiftTable\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x12\n\ntable_name\x18\x02 \x01(\t\"\x86\x01\n\x0bPubSubTopic\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x10\n\x08topic_id\x18\x02 \x01(\t\x12\x34\n\x06\x66ormat\x18\x03 \x01(\x0b\x32$.fennel.proto.connector.PubSubFormat\"\xdb\x01\n\x08HttpPath\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x10\n\x08\x65ndpoint\x18\x02 \x01(\t\x12\x12\n\x05limit\x18\x03 \x01(\rH\x00\x88\x01\x01\x12>\n\x07headers\x18\x04 \x03(\x0b\x32-.fennel.proto.connector.HttpPath.HeadersEntry\x1a.\n\x0cHeadersEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x08\n\x06_limit\"\xb1\x02\n\x0cPreProcValue\x12\r\n\x03ref\x18\x01 \x01(\tH\x00\x12+\n\x05value\x18\x02 \x01(\x0b\x32\x1a.fennel.proto.schema.ValueH\x00\x12\x39\n\x04\x65val\x18\x03 \x01(\x0b\x32).fennel.proto.connector.PreProcValue.EvalH\x00\x1a\x9e\x01\n\x04\x45val\x12+\n\x06schema\x18\x01 \x01(\x0b\x32\x1b.fennel.proto.schema.Schema\x12-\n\x04\x65xpr\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.ExprH\x00\x12-\n\x06pycode\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCodeH\x00\x42\x0b\n\teval_typeB\t\n\x07variant\"[\n\x0fMongoCollection\x12/\n\x02\x64\x62\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.ExtDatabase\x12\x17\n\x0f\x63ollection_name\x18\x02 \x01(\t\"2\n\x0cSnapshotData\x12\x0e\n\x06marker\x18\x01 \x01(\t\x12\x12\n\nnum_retain\x18\x02 \x01(\r\"\r\n\x0bIncremental\"\n\n\x08Recreate\"\xbc\x01\n\x05Style\x12:\n\x0bincremental\x18\x01 \x01(\x0b\x32#.fennel.proto.connector.IncrementalH\x00\x12\x34\n\x08recreate\x18\x02 \x01(\x0b\x32 .fennel.proto.connector.RecreateH\x00\x12\x38\n\x08snapshot\x18\x03 \x01(\x0b\x32$.fennel.proto.connector.SnapshotDataH\x00\x42\x07\n\x05Style\"\x91\x06\n\x06Source\x12/\n\x05table\x18\x01 \x01(\x0b\x32 .fennel.proto.connector.ExtTable\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\t\x12\x12\n\nds_version\x18\x03 \x01(\r\x12(\n\x05\x65very\x18\x04 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x13\n\x06\x63ursor\x18\x05 \x01(\tH\x00\x88\x01\x01\x12+\n\x08\x64isorder\x18\x06 \x01(\x0b\x32\x19.google.protobuf.Duration\x12\x17\n\x0ftimestamp_field\x18\x07 \x01(\t\x12\x30\n\x03\x63\x64\x63\x18\x08 \x01(\x0e\x32#.fennel.proto.connector.CDCStrategy\x12\x31\n\rstarting_from\x18\t \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12=\n\x08pre_proc\x18\n \x03(\x0b\x32+.fennel.proto.connector.Source.PreProcEntry\x12\x0f\n\x07version\x18\x0b \x01(\r\x12\x0f\n\x07\x62ounded\x18\x0c \x01(\x08\x12\x30\n\x08idleness\x18\r \x01(\x0b\x32\x19.google.protobuf.DurationH\x01\x88\x01\x01\x12)\n\x05until\x18\x0e \x01(\x0b\x32\x1a.google.protobuf.Timestamp\x12\x30\n\x06\x66ilter\x18\x0f \x01(\x0b\x32\x1b.fennel.proto.pycode.PyCodeH\x02\x88\x01\x01\x12H\n\x11sampling_strategy\x18\x10 \x01(\x0b\x32(.fennel.proto.connector.SamplingStrategyH\x03\x88\x01\x01\x1aT\n\x0cPreProcEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\x33\n\x05value\x18\x02 \x01(\x0b\x32$.fennel.proto.connector.PreProcValue:\x02\x38\x01\x42\t\n\x07_cursorB\x0b\n\t_idlenessB\t\n\x07_filterB\x14\n\x12_sampling_strategy\"\xee\x03\n\x04Sink\x12/\n\x05table\x18\x01 \x01(\x0b\x32 .fennel.proto.connector.ExtTable\x12\x0f\n\x07\x64\x61taset\x18\x02 \x01(\t\x12\x12\n\nds_version\x18\x03 \x01(\r\x12\x35\n\x03\x63\x64\x63\x18\x04 \x01(\x0e\x32#.fennel.proto.connector.CDCStrategyH\x00\x88\x01\x01\x12(\n\x05\x65very\x18\x05 \x01(\x0b\x32\x19.google.protobuf.Duration\x12/\n\x03how\x18\x06 \x01(\x0b\x32\x1d.fennel.proto.connector.StyleH\x01\x88\x01\x01\x12\x0e\n\x06\x63reate\x18\x07 \x01(\x08\x12:\n\x07renames\x18\x08 \x03(\x0b\x32).fennel.proto.connector.Sink.RenamesEntry\x12.\n\x05since\x18\t \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x02\x88\x01\x01\x12.\n\x05until\x18\n \x01(\x0b\x32\x1a.google.protobuf.TimestampH\x03\x88\x01\x01\x1a.\n\x0cRenamesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12\r\n\x05value\x18\x02 \x01(\t:\x02\x38\x01\x42\x06\n\x04_cdcB\x06\n\x04_howB\x08\n\x06_sinceB\x08\n\x06_until*K\n\x0b\x43\x44\x43Strategy\x12\n\n\x06\x41ppend\x10\x00\x12\n\n\x06Upsert\x10\x01\x12\x0c\n\x08\x44\x65\x62\x65zium\x10\x02\x12\n\n\x06Native\x10\x03\x12\n\n\x06\x44\x65lete\x10\x04\x62\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'connector_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_KAFKA'].fields_by_name['sasl_jaas_config']._loaded_options = None
  _globals['_KAFKA'].fields_by_name['sasl_jaas_config']._serialized_options = b'\030\001'
  _globals['_KAFKA'].fields_by_name['group_id']._loaded_options = None
  _globals['_KAFKA'].fields_by_name['group_id']._serialized_options = b'\030\001'
  _globals['_HTTPPATH_HEADERSENTRY']._loaded_options = None
  _globals['_HTTPPATH_HEADERSENTRY']._serialized_options = b'8\001'
  _globals['_SOURCE_PREPROCENTRY']._loaded_options = None
  _globals['_SOURCE_PREPROCENTRY']._serialized_options = b'8\001'
  _globals['_SINK_RENAMESENTRY']._loaded_options = None
  _globals['_SINK_RENAMESENTRY']._serialized_options = b'8\001'
  _globals['_CDCSTRATEGY']._serialized_start=8924
  _globals['_CDCSTRATEGY']._serialized_end=8999
  _globals['_EXTDATABASE']._serialized_start=207
  _globals['_EXTDATABASE']._serialized_end=905
  _globals['_SAMPLINGSTRATEGY']._serialized_start=907
  _globals['_SAMPLINGSTRATEGY']._serialized_end=970
  _globals['_PUBSUBFORMAT']._serialized_start=972
  _globals['_PUBSUBFORMAT']._serialized_end=1049
  _globals['_KAFKAFORMAT']._serialized_start=1052
  _globals['_KAFKAFORMAT']._serialized_end=1240
  _globals['_DEBEZIUMFORMAT']._serialized_start=1242
  _globals['_DEBEZIUMFORMAT']._serialized_end=1258
  _globals['_JSONFORMAT']._serialized_start=1260
  _globals['_JSONFORMAT']._serialized_end=1272
  _globals['_AVROFORMAT']._serialized_start=1274
  _globals['_AVROFORMAT']._serialized_end=1357
  _globals['_PROTOBUFFORMAT']._serialized_start=1359
  _globals['_PROTOBUFFORMAT']._serialized_end=1446
  _globals['_REFERENCE']._serialized_start=1449
  _globals['_REFERENCE']._serialized_end=1671
  _globals['_REFERENCE_EXTDBTYPE']._serialized_start=1524
  _globals['_REFERENCE_EXTDBTYPE']._serialized_end=1671
  _globals['_WEBHOOK']._serialized_start=1673
  _globals['_WEBHOOK']._serialized_end=1742
  _globals['_MYSQL']._serialized_start=1745
  _globals['_MYSQL']._serialized_end=2013
  _globals['_POSTGRES']._serialized_start=2016
  _globals['_POSTGRES']._serialized_end=2287
  _globals['_S3']._serialized_start=2290
  _globals['_S3']._serialized_end=2594
  _globals['_BIGQUERY']._serialized_start=2597
  _globals['_BIGQUERY']._serialized_end=2779
  _globals['_SNOWFLAKE']._serialized_start=2782
  _globals['_SNOWFLAKE']._serialized_end=3071
  _globals['_KAFKA']._serialized_start=3074
  _globals['_KAFKA']._serialized_end=3451
  _globals['_KINESIS']._serialized_start=3453
  _globals['_KINESIS']._serialized_end=3480
  _globals['_CREDENTIALS']._serialized_start=3483
  _globals['_CREDENTIALS']._serialized_end=3694
  _globals['_REDSHIFTAUTHENTICATION']._serialized_start=3696
  _globals['_REDSHIFTAUTHENTICATION']._serialized_end=3821
  _globals['_REDSHIFT']._serialized_start=3824
  _globals['_REDSHIFT']._serialized_end=3977
  _globals['_MONGO']._serialized_start=3980
  _globals['_MONGO']._serialized_end=4213
  _globals['_PUBSUB']._serialized_start=4216
  _globals['_PUBSUB']._serialized_end=4376
  _globals['_CERTIFICATE']._serialized_start=4378
  _globals['_CERTIFICATE']._serialized_end=4487
  _globals['_HTTP']._serialized_start=4490
  _globals['_HTTP']._serialized_end=4651
  _globals['_EXTTABLE']._serialized_start=4654
  _globals['_EXTTABLE']._serialized_end=5413
  _globals['_MYSQLTABLE']._serialized_start=5415
  _globals['_MYSQLTABLE']._serialized_end=5496
  _globals['_POSTGRESTABLE']._serialized_start=5498
  _globals['_POSTGRESTABLE']._serialized_end=5620
  _globals['_S3TABLE']._serialized_start=5623
  _globals['_S3TABLE']._serialized_end=5870
  _globals['_KAFKATOPIC']._serialized_start=5873
  _globals['_KAFKATOPIC']._serialized_end=6002
  _globals['_BIGQUERYTABLE']._serialized_start=6004
  _globals['_BIGQUERYTABLE']._serialized_end=6088
  _globals['_SNOWFLAKETABLE']._serialized_start=6090
  _globals['_SNOWFLAKETABLE']._serialized_end=6175
  _globals['_WEBHOOKENDPOINT']._serialized_start=6178
  _globals['_WEBHOOKENDPOINT']._serialized_end=6307
  _globals['_KINESISSTREAM']._serialized_start=6310
  _globals['_KINESISSTREAM']._serialized_end=6521
  _globals['_REDSHIFTTABLE']._serialized_start=6523
  _globals['_REDSHIFTTABLE']._serialized_end=6607
  _globals['_PUBSUBTOPIC']._serialized_start=6610
  _globals['_PUBSUBTOPIC']._serialized_end=6744
  _globals['_HTTPPATH']._serialized_start=6747
  _globals['_HTTPPATH']._serialized_end=6966
  _globals['_HTTPPATH_HEADERSENTRY']._serialized_start=6910
  _globals['_HTTPPATH_HEADERSENTRY']._serialized_end=6956
  _globals['_PREPROCVALUE']._serialized_start=6969
  _globals['_PREPROCVALUE']._serialized_end=7274
  _globals['_PREPROCVALUE_EVAL']._serialized_start=7105
  _globals['_PREPROCVALUE_EVAL']._serialized_end=7263
  _globals['_MONGOCOLLECTION']._serialized_start=7276
  _globals['_MONGOCOLLECTION']._serialized_end=7367
  _globals['_SNAPSHOTDATA']._serialized_start=7369
  _globals['_SNAPSHOTDATA']._serialized_end=7419
  _globals['_INCREMENTAL']._serialized_start=7421
  _globals['_INCREMENTAL']._serialized_end=7434
  _globals['_RECREATE']._serialized_start=7436
  _globals['_RECREATE']._serialized_end=7446
  _globals['_STYLE']._serialized_start=7449
  _globals['_STYLE']._serialized_end=7637
  _globals['_SOURCE']._serialized_start=7640
  _globals['_SOURCE']._serialized_end=8425
  _globals['_SOURCE_PREPROCENTRY']._serialized_start=8284
  _globals['_SOURCE_PREPROCENTRY']._serialized_end=8368
  _globals['_SINK']._serialized_start=8428
  _globals['_SINK']._serialized_end=8922
  _globals['_SINK_RENAMESENTRY']._serialized_start=8840
  _globals['_SINK_RENAMESENTRY']._serialized_end=8886
# @@protoc_insertion_point(module_scope)
