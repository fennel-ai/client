# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: expr.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.schema_pb2 as schema__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\nexpr.proto\x12\x11\x66\x65nnel.proto.expr\x1a\x0cschema.proto\"\x9a\x06\n\x04\x45xpr\x12%\n\x03ref\x18\x01 \x01(\x0b\x32\x16.fennel.proto.expr.RefH\x00\x12\x36\n\x0cjson_literal\x18\x02 \x01(\x0b\x32\x1e.fennel.proto.expr.JsonLiteralH\x00\x12)\n\x05unary\x18\x04 \x01(\x0b\x32\x18.fennel.proto.expr.UnaryH\x00\x12\'\n\x04\x63\x61se\x18\x05 \x01(\x0b\x32\x17.fennel.proto.expr.CaseH\x00\x12+\n\x06\x62inary\x18\x06 \x01(\x0b\x32\x19.fennel.proto.expr.BinaryH\x00\x12+\n\x06isnull\x18\x07 \x01(\x0b\x32\x19.fennel.proto.expr.IsNullH\x00\x12/\n\x08\x66illnull\x18\x08 \x01(\x0b\x32\x1b.fennel.proto.expr.FillNullH\x00\x12,\n\x07list_fn\x18\t \x01(\x0b\x32\x19.fennel.proto.expr.ListFnH\x00\x12,\n\x07math_fn\x18\n \x01(\x0b\x32\x19.fennel.proto.expr.MathFnH\x00\x12\x30\n\tstruct_fn\x18\x0b \x01(\x0b\x32\x1b.fennel.proto.expr.StructFnH\x00\x12,\n\x07\x64ict_fn\x18\x0c \x01(\x0b\x32\x19.fennel.proto.expr.DictFnH\x00\x12\x30\n\tstring_fn\x18\r \x01(\x0b\x32\x1b.fennel.proto.expr.StringFnH\x00\x12\x34\n\x0b\x64\x61tetime_fn\x18\x0e \x01(\x0b\x32\x1d.fennel.proto.expr.DateTimeFnH\x00\x12>\n\x10\x64\x61tetime_literal\x18\x0f \x01(\x0b\x32\".fennel.proto.expr.DatetimeLiteralH\x00\x12\x34\n\x0bmake_struct\x18\x10 \x01(\x0b\x32\x1d.fennel.proto.expr.MakeStructH\x00\x12\x32\n\nfrom_epoch\x18\x11 \x01(\x0b\x32\x1c.fennel.proto.expr.FromEpochH\x00\x42\x06\n\x04node\"a\n\tFromEpoch\x12)\n\x08\x64uration\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12)\n\x04unit\x18\x02 \x01(\x0e\x32\x1b.fennel.proto.expr.TimeUnit\"\xad\x01\n\x0f\x44\x61tetimeLiteral\x12\x0c\n\x04year\x18\x01 \x01(\r\x12\r\n\x05month\x18\x02 \x01(\r\x12\x0b\n\x03\x64\x61y\x18\x03 \x01(\r\x12\x0c\n\x04hour\x18\x04 \x01(\r\x12\x0e\n\x06minute\x18\x05 \x01(\r\x12\x0e\n\x06second\x18\x06 \x01(\r\x12\x13\n\x0bmicrosecond\x18\x07 \x01(\r\x12-\n\x08timezone\x18\x08 \x01(\x0b\x32\x1b.fennel.proto.expr.Timezone\"\xc5\x01\n\nMakeStruct\x12\x34\n\x0bstruct_type\x18\x01 \x01(\x0b\x32\x1f.fennel.proto.schema.StructType\x12\x39\n\x06\x66ields\x18\x02 \x03(\x0b\x32).fennel.proto.expr.MakeStruct.FieldsEntry\x1a\x46\n\x0b\x46ieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12&\n\x05value\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr:\x02\x38\x01\"L\n\x0bJsonLiteral\x12\x0f\n\x07literal\x18\x01 \x01(\t\x12,\n\x05\x64type\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\"\x13\n\x03Ref\x12\x0c\n\x04name\x18\x01 \x01(\t\"Y\n\x05Unary\x12&\n\x02op\x18\x01 \x01(\x0e\x32\x1a.fennel.proto.expr.UnaryOp\x12(\n\x07operand\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"}\n\x06\x42inary\x12%\n\x04left\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12&\n\x05right\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12$\n\x02op\x18\x03 \x01(\x0e\x32\x18.fennel.proto.expr.BinOp\"b\n\x04\x43\x61se\x12.\n\twhen_then\x18\x01 \x03(\x0b\x32\x1b.fennel.proto.expr.WhenThen\x12*\n\totherwise\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"X\n\x08WhenThen\x12%\n\x04when\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12%\n\x04then\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"2\n\x06IsNull\x12(\n\x07operand\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"[\n\x08\x46illNull\x12(\n\x07operand\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12%\n\x04\x66ill\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"\x93\x01\n\x06ListOp\x12%\n\x03len\x18\x01 \x01(\x0b\x32\x16.fennel.proto.expr.LenH\x00\x12&\n\x03get\x18\x02 \x01(\x0b\x32\x17.fennel.proto.expr.ExprH\x00\x12/\n\x08\x63ontains\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.expr.ContainsH\x00\x42\t\n\x07\x66n_type\"\x05\n\x03Len\"4\n\x08\x43ontains\x12(\n\x07\x65lement\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"V\n\x06ListFn\x12%\n\x04list\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12%\n\x02\x66n\x18\x02 \x01(\x0b\x32\x19.fennel.proto.expr.ListOp\"\xb9\x01\n\x06MathOp\x12)\n\x05round\x18\x01 \x01(\x0b\x32\x18.fennel.proto.expr.RoundH\x00\x12%\n\x03\x61\x62s\x18\x02 \x01(\x0b\x32\x16.fennel.proto.expr.AbsH\x00\x12\'\n\x04\x63\x65il\x18\x03 \x01(\x0b\x32\x17.fennel.proto.expr.CeilH\x00\x12)\n\x05\x66loor\x18\x04 \x01(\x0b\x32\x18.fennel.proto.expr.FloorH\x00\x42\t\n\x07\x66n_type\"\x1a\n\x05Round\x12\x11\n\tprecision\x18\x01 \x01(\x05\"\x05\n\x03\x41\x62s\"\x06\n\x04\x43\x65il\"\x07\n\x05\x46loor\"Y\n\x06MathFn\x12(\n\x07operand\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12%\n\x02\x66n\x18\x02 \x01(\x0b\x32\x19.fennel.proto.expr.MathOp\"&\n\x08StructOp\x12\x0f\n\x05\x66ield\x18\x01 \x01(\tH\x00\x42\t\n\x07\x66n_type\"\\\n\x08StructFn\x12\'\n\x06struct\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12\'\n\x02\x66n\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.expr.StructOp\"a\n\x07\x44ictGet\x12&\n\x05\x66ield\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12.\n\rdefault_value\x18\x03 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"\x96\x01\n\x06\x44ictOp\x12%\n\x03len\x18\x01 \x01(\x0b\x32\x16.fennel.proto.expr.LenH\x00\x12)\n\x03get\x18\x02 \x01(\x0b\x32\x1a.fennel.proto.expr.DictGetH\x00\x12/\n\x08\x63ontains\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.expr.ContainsH\x00\x42\t\n\x07\x66n_type\"V\n\x06\x44ictFn\x12%\n\x04\x64ict\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12%\n\x02\x66n\x18\x02 \x01(\x0b\x32\x19.fennel.proto.expr.DictOp\"\xc5\x03\n\x08StringOp\x12%\n\x03len\x18\x01 \x01(\x0b\x32\x16.fennel.proto.expr.LenH\x00\x12-\n\x07tolower\x18\x02 \x01(\x0b\x32\x1a.fennel.proto.expr.ToLowerH\x00\x12-\n\x07toupper\x18\x03 \x01(\x0b\x32\x1a.fennel.proto.expr.ToUpperH\x00\x12/\n\x08\x63ontains\x18\x04 \x01(\x0b\x32\x1b.fennel.proto.expr.ContainsH\x00\x12\x33\n\nstartswith\x18\x05 \x01(\x0b\x32\x1d.fennel.proto.expr.StartsWithH\x00\x12/\n\x08\x65ndswith\x18\x06 \x01(\x0b\x32\x1b.fennel.proto.expr.EndsWithH\x00\x12+\n\x06\x63oncat\x18\x07 \x01(\x0b\x32\x19.fennel.proto.expr.ConcatH\x00\x12/\n\x08strptime\x18\x08 \x01(\x0b\x32\x1b.fennel.proto.expr.StrptimeH\x00\x12\x34\n\x0bjson_decode\x18\t \x01(\x0b\x32\x1d.fennel.proto.expr.JsonDecodeH\x00\x42\t\n\x07\x66n_type\"\x1c\n\x08Timezone\x12\x10\n\x08timezone\x18\x01 \x01(\t\":\n\nJsonDecode\x12,\n\x05\x64type\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\"I\n\x08Strptime\x12\x0e\n\x06\x66ormat\x18\x01 \x01(\t\x12-\n\x08timezone\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.expr.Timezone\"\t\n\x07ToLower\"\t\n\x07ToUpper\"2\n\nStartsWith\x12$\n\x03key\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"0\n\x08\x45ndsWith\x12$\n\x03key\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"0\n\x06\x43oncat\x12&\n\x05other\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\"\\\n\x08StringFn\x12\'\n\x06string\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12\'\n\x02\x66n\x18\x02 \x01(\x0b\x32\x1b.fennel.proto.expr.StringOp\"b\n\nDateTimeFn\x12)\n\x08\x64\x61tetime\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12)\n\x02\x66n\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expr.DateTimeOp\"\xd2\x01\n\nDateTimeOp\x12)\n\x05since\x18\x01 \x01(\x0b\x32\x18.fennel.proto.expr.SinceH\x00\x12\x34\n\x0bsince_epoch\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expr.SinceEpochH\x00\x12/\n\x08strftime\x18\x03 \x01(\x0b\x32\x1b.fennel.proto.expr.StrftimeH\x00\x12\'\n\x04part\x18\x04 \x01(\x0b\x32\x17.fennel.proto.expr.PartH\x00\x42\t\n\x07\x66n_type\"Z\n\x05Since\x12&\n\x05other\x18\x01 \x01(\x0b\x32\x17.fennel.proto.expr.Expr\x12)\n\x04unit\x18\x02 \x01(\x0e\x32\x1b.fennel.proto.expr.TimeUnit\"7\n\nSinceEpoch\x12)\n\x04unit\x18\x01 \x01(\x0e\x32\x1b.fennel.proto.expr.TimeUnit\"\x1a\n\x08Strftime\x12\x0e\n\x06\x66ormat\x18\x01 \x01(\t\"1\n\x04Part\x12)\n\x04unit\x18\x01 \x01(\x0e\x32\x1b.fennel.proto.expr.TimeUnit*\x1b\n\x07UnaryOp\x12\x07\n\x03NEG\x10\x00\x12\x07\n\x03NOT\x10\x01*\x86\x01\n\x05\x42inOp\x12\x07\n\x03\x41\x44\x44\x10\x00\x12\x07\n\x03SUB\x10\x01\x12\x07\n\x03MUL\x10\x02\x12\x07\n\x03\x44IV\x10\x03\x12\x07\n\x03MOD\x10\x04\x12\r\n\tFLOOR_DIV\x10\x05\x12\x06\n\x02\x45Q\x10\x06\x12\x06\n\x02NE\x10\x07\x12\x06\n\x02GT\x10\x08\x12\x07\n\x03GTE\x10\t\x12\x06\n\x02LT\x10\n\x12\x07\n\x03LTE\x10\x0b\x12\x07\n\x03\x41ND\x10\x0c\x12\x06\n\x02OR\x10\r*\x83\x01\n\x08TimeUnit\x12\x0b\n\x07UNKNOWN\x10\x00\x12\n\n\x06SECOND\x10\x01\x12\n\n\x06MINUTE\x10\x02\x12\x08\n\x04HOUR\x10\x03\x12\x07\n\x03\x44\x41Y\x10\x04\x12\x08\n\x04WEEK\x10\x05\x12\t\n\x05MONTH\x10\x06\x12\x08\n\x04YEAR\x10\x07\x12\x0f\n\x0bMICROSECOND\x10\x08\x12\x0f\n\x0bMILLISECOND\x10\tb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'expr_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
  DESCRIPTOR._options = None
  _globals['_MAKESTRUCT_FIELDSENTRY']._options = None
  _globals['_MAKESTRUCT_FIELDSENTRY']._serialized_options = b'8\001'
  _globals['_UNARYOP']._serialized_start=4505
  _globals['_UNARYOP']._serialized_end=4532
  _globals['_BINOP']._serialized_start=4535
  _globals['_BINOP']._serialized_end=4669
  _globals['_TIMEUNIT']._serialized_start=4672
  _globals['_TIMEUNIT']._serialized_end=4803
  _globals['_EXPR']._serialized_start=48
  _globals['_EXPR']._serialized_end=842
  _globals['_FROMEPOCH']._serialized_start=844
  _globals['_FROMEPOCH']._serialized_end=941
  _globals['_DATETIMELITERAL']._serialized_start=944
  _globals['_DATETIMELITERAL']._serialized_end=1117
  _globals['_MAKESTRUCT']._serialized_start=1120
  _globals['_MAKESTRUCT']._serialized_end=1317
  _globals['_MAKESTRUCT_FIELDSENTRY']._serialized_start=1247
  _globals['_MAKESTRUCT_FIELDSENTRY']._serialized_end=1317
  _globals['_JSONLITERAL']._serialized_start=1319
  _globals['_JSONLITERAL']._serialized_end=1395
  _globals['_REF']._serialized_start=1397
  _globals['_REF']._serialized_end=1416
  _globals['_UNARY']._serialized_start=1418
  _globals['_UNARY']._serialized_end=1507
  _globals['_BINARY']._serialized_start=1509
  _globals['_BINARY']._serialized_end=1634
  _globals['_CASE']._serialized_start=1636
  _globals['_CASE']._serialized_end=1734
  _globals['_WHENTHEN']._serialized_start=1736
  _globals['_WHENTHEN']._serialized_end=1824
  _globals['_ISNULL']._serialized_start=1826
  _globals['_ISNULL']._serialized_end=1876
  _globals['_FILLNULL']._serialized_start=1878
  _globals['_FILLNULL']._serialized_end=1969
  _globals['_LISTOP']._serialized_start=1972
  _globals['_LISTOP']._serialized_end=2119
  _globals['_LEN']._serialized_start=2121
  _globals['_LEN']._serialized_end=2126
  _globals['_CONTAINS']._serialized_start=2128
  _globals['_CONTAINS']._serialized_end=2180
  _globals['_LISTFN']._serialized_start=2182
  _globals['_LISTFN']._serialized_end=2268
  _globals['_MATHOP']._serialized_start=2271
  _globals['_MATHOP']._serialized_end=2456
  _globals['_ROUND']._serialized_start=2458
  _globals['_ROUND']._serialized_end=2484
  _globals['_ABS']._serialized_start=2486
  _globals['_ABS']._serialized_end=2491
  _globals['_CEIL']._serialized_start=2493
  _globals['_CEIL']._serialized_end=2499
  _globals['_FLOOR']._serialized_start=2501
  _globals['_FLOOR']._serialized_end=2508
  _globals['_MATHFN']._serialized_start=2510
  _globals['_MATHFN']._serialized_end=2599
  _globals['_STRUCTOP']._serialized_start=2601
  _globals['_STRUCTOP']._serialized_end=2639
  _globals['_STRUCTFN']._serialized_start=2641
  _globals['_STRUCTFN']._serialized_end=2733
  _globals['_DICTGET']._serialized_start=2735
  _globals['_DICTGET']._serialized_end=2832
  _globals['_DICTOP']._serialized_start=2835
  _globals['_DICTOP']._serialized_end=2985
  _globals['_DICTFN']._serialized_start=2987
  _globals['_DICTFN']._serialized_end=3073
  _globals['_STRINGOP']._serialized_start=3076
  _globals['_STRINGOP']._serialized_end=3529
  _globals['_TIMEZONE']._serialized_start=3531
  _globals['_TIMEZONE']._serialized_end=3559
  _globals['_JSONDECODE']._serialized_start=3561
  _globals['_JSONDECODE']._serialized_end=3619
  _globals['_STRPTIME']._serialized_start=3621
  _globals['_STRPTIME']._serialized_end=3694
  _globals['_TOLOWER']._serialized_start=3696
  _globals['_TOLOWER']._serialized_end=3705
  _globals['_TOUPPER']._serialized_start=3707
  _globals['_TOUPPER']._serialized_end=3716
  _globals['_STARTSWITH']._serialized_start=3718
  _globals['_STARTSWITH']._serialized_end=3768
  _globals['_ENDSWITH']._serialized_start=3770
  _globals['_ENDSWITH']._serialized_end=3818
  _globals['_CONCAT']._serialized_start=3820
  _globals['_CONCAT']._serialized_end=3868
  _globals['_STRINGFN']._serialized_start=3870
  _globals['_STRINGFN']._serialized_end=3962
  _globals['_DATETIMEFN']._serialized_start=3964
  _globals['_DATETIMEFN']._serialized_end=4062
  _globals['_DATETIMEOP']._serialized_start=4065
  _globals['_DATETIMEOP']._serialized_end=4275
  _globals['_SINCE']._serialized_start=4277
  _globals['_SINCE']._serialized_end=4367
  _globals['_SINCEEPOCH']._serialized_start=4369
  _globals['_SINCEEPOCH']._serialized_end=4424
  _globals['_STRFTIME']._serialized_start=4426
  _globals['_STRFTIME']._serialized_end=4452
  _globals['_PART']._serialized_start=4454
  _globals['_PART']._serialized_end=4503
# @@protoc_insertion_point(module_scope)
