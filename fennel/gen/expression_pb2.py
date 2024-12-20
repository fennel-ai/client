# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: expression.proto
# Protobuf Python Version: 5.26.1
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


import fennel.gen.schema_pb2 as schema__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x10\x65xpression.proto\x12\x17\x66\x65nnel.proto.expression\x1a\x0cschema.proto\"9\n\x0b\x45valContext\x12\x19\n\x0cnow_col_name\x18\x01 \x01(\tH\x00\x88\x01\x01\x42\x0f\n\r_now_col_name\"\xd4\x07\n\x04\x45xpr\x12+\n\x03ref\x18\x01 \x01(\x0b\x32\x1c.fennel.proto.expression.RefH\x00\x12<\n\x0cjson_literal\x18\x02 \x01(\x0b\x32$.fennel.proto.expression.JsonLiteralH\x00\x12/\n\x05unary\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.expression.UnaryH\x00\x12-\n\x04\x63\x61se\x18\x05 \x01(\x0b\x32\x1d.fennel.proto.expression.CaseH\x00\x12\x31\n\x06\x62inary\x18\x06 \x01(\x0b\x32\x1f.fennel.proto.expression.BinaryH\x00\x12\x31\n\x06isnull\x18\x07 \x01(\x0b\x32\x1f.fennel.proto.expression.IsNullH\x00\x12\x35\n\x08\x66illnull\x18\x08 \x01(\x0b\x32!.fennel.proto.expression.FillNullH\x00\x12\x32\n\x07list_fn\x18\t \x01(\x0b\x32\x1f.fennel.proto.expression.ListFnH\x00\x12\x32\n\x07math_fn\x18\n \x01(\x0b\x32\x1f.fennel.proto.expression.MathFnH\x00\x12\x36\n\tstruct_fn\x18\x0b \x01(\x0b\x32!.fennel.proto.expression.StructFnH\x00\x12\x32\n\x07\x64ict_fn\x18\x0c \x01(\x0b\x32\x1f.fennel.proto.expression.DictFnH\x00\x12\x36\n\tstring_fn\x18\r \x01(\x0b\x32!.fennel.proto.expression.StringFnH\x00\x12:\n\x0b\x64\x61tetime_fn\x18\x0e \x01(\x0b\x32#.fennel.proto.expression.DateTimeFnH\x00\x12\x44\n\x10\x64\x61tetime_literal\x18\x0f \x01(\x0b\x32(.fennel.proto.expression.DatetimeLiteralH\x00\x12:\n\x0bmake_struct\x18\x10 \x01(\x0b\x32#.fennel.proto.expression.MakeStructH\x00\x12\x38\n\nfrom_epoch\x18\x11 \x01(\x0b\x32\".fennel.proto.expression.FromEpochH\x00\x12+\n\x03var\x18\x12 \x01(\x0b\x32\x1c.fennel.proto.expression.VarH\x00\x12+\n\x03now\x18\x13 \x01(\x0b\x32\x1c.fennel.proto.expression.NowH\x00\x42\x06\n\x04node\"\x05\n\x03Now\"\x13\n\x03Var\x12\x0c\n\x04name\x18\x01 \x01(\t\"m\n\tFromEpoch\x12/\n\x08\x64uration\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12/\n\x04unit\x18\x02 \x01(\x0e\x32!.fennel.proto.expression.TimeUnit\"\xb3\x01\n\x0f\x44\x61tetimeLiteral\x12\x0c\n\x04year\x18\x01 \x01(\r\x12\r\n\x05month\x18\x02 \x01(\r\x12\x0b\n\x03\x64\x61y\x18\x03 \x01(\r\x12\x0c\n\x04hour\x18\x04 \x01(\r\x12\x0e\n\x06minute\x18\x05 \x01(\r\x12\x0e\n\x06second\x18\x06 \x01(\r\x12\x13\n\x0bmicrosecond\x18\x07 \x01(\r\x12\x33\n\x08timezone\x18\x08 \x01(\x0b\x32!.fennel.proto.expression.Timezone\"\xd1\x01\n\nMakeStruct\x12\x34\n\x0bstruct_type\x18\x01 \x01(\x0b\x32\x1f.fennel.proto.schema.StructType\x12?\n\x06\x66ields\x18\x02 \x03(\x0b\x32/.fennel.proto.expression.MakeStruct.FieldsEntry\x1aL\n\x0b\x46ieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12,\n\x05value\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr:\x02\x38\x01\"L\n\x0bJsonLiteral\x12\x0f\n\x07literal\x18\x01 \x01(\t\x12,\n\x05\x64type\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\"\x13\n\x03Ref\x12\x0c\n\x04name\x18\x01 \x01(\t\"e\n\x05Unary\x12,\n\x02op\x18\x01 \x01(\x0e\x32 .fennel.proto.expression.UnaryOp\x12.\n\x07operand\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"\x8f\x01\n\x06\x42inary\x12+\n\x04left\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12,\n\x05right\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12*\n\x02op\x18\x03 \x01(\x0e\x32\x1e.fennel.proto.expression.BinOp\"n\n\x04\x43\x61se\x12\x34\n\twhen_then\x18\x01 \x03(\x0b\x32!.fennel.proto.expression.WhenThen\x12\x30\n\totherwise\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"d\n\x08WhenThen\x12+\n\x04when\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12+\n\x04then\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"8\n\x06IsNull\x12.\n\x07operand\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"g\n\x08\x46illNull\x12.\n\x07operand\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12+\n\x04\x66ill\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"\xeb\x04\n\x06ListOp\x12+\n\x03len\x18\x01 \x01(\x0b\x32\x1c.fennel.proto.expression.LenH\x00\x12,\n\x03get\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.ExprH\x00\x12\x35\n\x08\x63ontains\x18\x03 \x01(\x0b\x32!.fennel.proto.expression.ContainsH\x00\x12\x34\n\x08has_null\x18\x04 \x01(\x0b\x32 .fennel.proto.expression.HasNullH\x00\x12/\n\x03sum\x18\x05 \x01(\x0b\x32 .fennel.proto.expression.ListSumH\x00\x12/\n\x03min\x18\x06 \x01(\x0b\x32 .fennel.proto.expression.ListMinH\x00\x12/\n\x03max\x18\x07 \x01(\x0b\x32 .fennel.proto.expression.ListMaxH\x00\x12/\n\x03\x61ll\x18\x08 \x01(\x0b\x32 .fennel.proto.expression.ListAllH\x00\x12/\n\x03\x61ny\x18\t \x01(\x0b\x32 .fennel.proto.expression.ListAnyH\x00\x12\x31\n\x04mean\x18\n \x01(\x0b\x32!.fennel.proto.expression.ListMeanH\x00\x12\x35\n\x06\x66ilter\x18\x0b \x01(\x0b\x32#.fennel.proto.expression.ListFilterH\x00\x12/\n\x03map\x18\x0c \x01(\x0b\x32 .fennel.proto.expression.ListMapH\x00\x42\t\n\x07\x66n_type\"K\n\nListFilter\x12\x0b\n\x03var\x18\x01 \x01(\t\x12\x30\n\tpredicate\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"G\n\x07ListMap\x12\x0b\n\x03var\x18\x01 \x01(\t\x12/\n\x08map_expr\x18\x02 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"\t\n\x07ListSum\"\t\n\x07ListMin\"\n\n\x08ListMean\"\t\n\x07ListMax\"\t\n\x07ListAll\"\t\n\x07ListAny\"\x05\n\x03Len\"\t\n\x07HasNull\":\n\x08\x43ontains\x12.\n\x07\x65lement\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"b\n\x06ListFn\x12+\n\x04list\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12+\n\x02\x66n\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.expression.ListOp\"\x89\x02\n\x06MathOp\x12/\n\x05round\x18\x01 \x01(\x0b\x32\x1e.fennel.proto.expression.RoundH\x00\x12+\n\x03\x61\x62s\x18\x02 \x01(\x0b\x32\x1c.fennel.proto.expression.AbsH\x00\x12-\n\x04\x63\x65il\x18\x03 \x01(\x0b\x32\x1d.fennel.proto.expression.CeilH\x00\x12/\n\x05\x66loor\x18\x04 \x01(\x0b\x32\x1e.fennel.proto.expression.FloorH\x00\x12\x36\n\tto_string\x18\x05 \x01(\x0b\x32!.fennel.proto.expression.ToStringH\x00\x42\t\n\x07\x66n_type\"\x1a\n\x05Round\x12\x11\n\tprecision\x18\x01 \x01(\x05\"\x05\n\x03\x41\x62s\"\x06\n\x04\x43\x65il\"\x07\n\x05\x46loor\"\n\n\x08ToString\"e\n\x06MathFn\x12.\n\x07operand\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12+\n\x02\x66n\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.expression.MathOp\"&\n\x08StructOp\x12\x0f\n\x05\x66ield\x18\x01 \x01(\tH\x00\x42\t\n\x07\x66n_type\"h\n\x08StructFn\x12-\n\x06struct\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12-\n\x02\x66n\x18\x02 \x01(\x0b\x32!.fennel.proto.expression.StructOp\"m\n\x07\x44ictGet\x12,\n\x05\x66ield\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12\x34\n\rdefault_value\x18\x03 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"\xa8\x01\n\x06\x44ictOp\x12+\n\x03len\x18\x01 \x01(\x0b\x32\x1c.fennel.proto.expression.LenH\x00\x12/\n\x03get\x18\x02 \x01(\x0b\x32 .fennel.proto.expression.DictGetH\x00\x12\x35\n\x08\x63ontains\x18\x03 \x01(\x0b\x32!.fennel.proto.expression.ContainsH\x00\x42\t\n\x07\x66n_type\"b\n\x06\x44ictFn\x12+\n\x04\x64ict\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12+\n\x02\x66n\x18\x02 \x01(\x0b\x32\x1f.fennel.proto.expression.DictOp\"\x9c\x05\n\x08StringOp\x12+\n\x03len\x18\x01 \x01(\x0b\x32\x1c.fennel.proto.expression.LenH\x00\x12\x33\n\x07tolower\x18\x02 \x01(\x0b\x32 .fennel.proto.expression.ToLowerH\x00\x12\x33\n\x07toupper\x18\x03 \x01(\x0b\x32 .fennel.proto.expression.ToUpperH\x00\x12\x35\n\x08\x63ontains\x18\x04 \x01(\x0b\x32!.fennel.proto.expression.ContainsH\x00\x12\x39\n\nstartswith\x18\x05 \x01(\x0b\x32#.fennel.proto.expression.StartsWithH\x00\x12\x35\n\x08\x65ndswith\x18\x06 \x01(\x0b\x32!.fennel.proto.expression.EndsWithH\x00\x12\x31\n\x06\x63oncat\x18\x07 \x01(\x0b\x32\x1f.fennel.proto.expression.ConcatH\x00\x12\x35\n\x08strptime\x18\x08 \x01(\x0b\x32!.fennel.proto.expression.StrptimeH\x00\x12:\n\x0bjson_decode\x18\t \x01(\x0b\x32#.fennel.proto.expression.JsonDecodeH\x00\x12/\n\x05split\x18\n \x01(\x0b\x32\x1e.fennel.proto.expression.SplitH\x00\x12<\n\x0cjson_extract\x18\x0b \x01(\x0b\x32$.fennel.proto.expression.JsonExtractH\x00\x12\x30\n\x06to_int\x18\x0c \x01(\x0b\x32\x1e.fennel.proto.expression.ToIntH\x00\x42\t\n\x07\x66n_type\"\x1c\n\x08Timezone\x12\x10\n\x08timezone\x18\x01 \x01(\t\":\n\nJsonDecode\x12,\n\x05\x64type\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.schema.DataType\"O\n\x08Strptime\x12\x0e\n\x06\x66ormat\x18\x01 \x01(\t\x12\x33\n\x08timezone\x18\x02 \x01(\x0b\x32!.fennel.proto.expression.Timezone\"\t\n\x07ToLower\"\t\n\x07ToUpper\"8\n\nStartsWith\x12*\n\x03key\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"6\n\x08\x45ndsWith\x12*\n\x03key\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"6\n\x06\x43oncat\x12,\n\x05other\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\"h\n\x08StringFn\x12-\n\x06string\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12-\n\x02\x66n\x18\x02 \x01(\x0b\x32!.fennel.proto.expression.StringOp\"n\n\nDateTimeFn\x12/\n\x08\x64\x61tetime\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12/\n\x02\x66n\x18\x02 \x01(\x0b\x32#.fennel.proto.expression.DateTimeOp\"\xea\x01\n\nDateTimeOp\x12/\n\x05since\x18\x01 \x01(\x0b\x32\x1e.fennel.proto.expression.SinceH\x00\x12:\n\x0bsince_epoch\x18\x02 \x01(\x0b\x32#.fennel.proto.expression.SinceEpochH\x00\x12\x35\n\x08strftime\x18\x03 \x01(\x0b\x32!.fennel.proto.expression.StrftimeH\x00\x12-\n\x04part\x18\x04 \x01(\x0b\x32\x1d.fennel.proto.expression.PartH\x00\x42\t\n\x07\x66n_type\"f\n\x05Since\x12,\n\x05other\x18\x01 \x01(\x0b\x32\x1d.fennel.proto.expression.Expr\x12/\n\x04unit\x18\x02 \x01(\x0e\x32!.fennel.proto.expression.TimeUnit\"=\n\nSinceEpoch\x12/\n\x04unit\x18\x01 \x01(\x0e\x32!.fennel.proto.expression.TimeUnit\"O\n\x08Strftime\x12\x0e\n\x06\x66ormat\x18\x01 \x01(\t\x12\x33\n\x08timezone\x18\x02 \x01(\x0b\x32!.fennel.proto.expression.Timezone\"l\n\x04Part\x12/\n\x04unit\x18\x01 \x01(\x0e\x32!.fennel.proto.expression.TimeUnit\x12\x33\n\x08timezone\x18\x02 \x01(\x0b\x32!.fennel.proto.expression.Timezone\"\x14\n\x05Split\x12\x0b\n\x03sep\x18\x01 \x01(\t\"\x1b\n\x0bJsonExtract\x12\x0c\n\x04path\x18\x01 \x01(\t\"\x07\n\x05ToInt*\x1b\n\x07UnaryOp\x12\x07\n\x03NEG\x10\x00\x12\x07\n\x03NOT\x10\x01*\x86\x01\n\x05\x42inOp\x12\x07\n\x03\x41\x44\x44\x10\x00\x12\x07\n\x03SUB\x10\x01\x12\x07\n\x03MUL\x10\x02\x12\x07\n\x03\x44IV\x10\x03\x12\x07\n\x03MOD\x10\x04\x12\r\n\tFLOOR_DIV\x10\x05\x12\x06\n\x02\x45Q\x10\x06\x12\x06\n\x02NE\x10\x07\x12\x06\n\x02GT\x10\x08\x12\x07\n\x03GTE\x10\t\x12\x06\n\x02LT\x10\n\x12\x07\n\x03LTE\x10\x0b\x12\x07\n\x03\x41ND\x10\x0c\x12\x06\n\x02OR\x10\r*\x83\x01\n\x08TimeUnit\x12\x0b\n\x07UNKNOWN\x10\x00\x12\n\n\x06SECOND\x10\x01\x12\n\n\x06MINUTE\x10\x02\x12\x08\n\x04HOUR\x10\x03\x12\x07\n\x03\x44\x41Y\x10\x04\x12\x08\n\x04WEEK\x10\x05\x12\t\n\x05MONTH\x10\x06\x12\x08\n\x04YEAR\x10\x07\x12\x0f\n\x0bMICROSECOND\x10\x08\x12\x0f\n\x0bMILLISECOND\x10\tb\x06proto3')

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'expression_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
  DESCRIPTOR._loaded_options = None
  _globals['_MAKESTRUCT_FIELDSENTRY']._loaded_options = None
  _globals['_MAKESTRUCT_FIELDSENTRY']._serialized_options = b'8\001'
  _globals['_UNARYOP']._serialized_start=6246
  _globals['_UNARYOP']._serialized_end=6273
  _globals['_BINOP']._serialized_start=6276
  _globals['_BINOP']._serialized_end=6410
  _globals['_TIMEUNIT']._serialized_start=6413
  _globals['_TIMEUNIT']._serialized_end=6544
  _globals['_EVALCONTEXT']._serialized_start=59
  _globals['_EVALCONTEXT']._serialized_end=116
  _globals['_EXPR']._serialized_start=119
  _globals['_EXPR']._serialized_end=1099
  _globals['_NOW']._serialized_start=1101
  _globals['_NOW']._serialized_end=1106
  _globals['_VAR']._serialized_start=1108
  _globals['_VAR']._serialized_end=1127
  _globals['_FROMEPOCH']._serialized_start=1129
  _globals['_FROMEPOCH']._serialized_end=1238
  _globals['_DATETIMELITERAL']._serialized_start=1241
  _globals['_DATETIMELITERAL']._serialized_end=1420
  _globals['_MAKESTRUCT']._serialized_start=1423
  _globals['_MAKESTRUCT']._serialized_end=1632
  _globals['_MAKESTRUCT_FIELDSENTRY']._serialized_start=1556
  _globals['_MAKESTRUCT_FIELDSENTRY']._serialized_end=1632
  _globals['_JSONLITERAL']._serialized_start=1634
  _globals['_JSONLITERAL']._serialized_end=1710
  _globals['_REF']._serialized_start=1712
  _globals['_REF']._serialized_end=1731
  _globals['_UNARY']._serialized_start=1733
  _globals['_UNARY']._serialized_end=1834
  _globals['_BINARY']._serialized_start=1837
  _globals['_BINARY']._serialized_end=1980
  _globals['_CASE']._serialized_start=1982
  _globals['_CASE']._serialized_end=2092
  _globals['_WHENTHEN']._serialized_start=2094
  _globals['_WHENTHEN']._serialized_end=2194
  _globals['_ISNULL']._serialized_start=2196
  _globals['_ISNULL']._serialized_end=2252
  _globals['_FILLNULL']._serialized_start=2254
  _globals['_FILLNULL']._serialized_end=2357
  _globals['_LISTOP']._serialized_start=2360
  _globals['_LISTOP']._serialized_end=2979
  _globals['_LISTFILTER']._serialized_start=2981
  _globals['_LISTFILTER']._serialized_end=3056
  _globals['_LISTMAP']._serialized_start=3058
  _globals['_LISTMAP']._serialized_end=3129
  _globals['_LISTSUM']._serialized_start=3131
  _globals['_LISTSUM']._serialized_end=3140
  _globals['_LISTMIN']._serialized_start=3142
  _globals['_LISTMIN']._serialized_end=3151
  _globals['_LISTMEAN']._serialized_start=3153
  _globals['_LISTMEAN']._serialized_end=3163
  _globals['_LISTMAX']._serialized_start=3165
  _globals['_LISTMAX']._serialized_end=3174
  _globals['_LISTALL']._serialized_start=3176
  _globals['_LISTALL']._serialized_end=3185
  _globals['_LISTANY']._serialized_start=3187
  _globals['_LISTANY']._serialized_end=3196
  _globals['_LEN']._serialized_start=3198
  _globals['_LEN']._serialized_end=3203
  _globals['_HASNULL']._serialized_start=3205
  _globals['_HASNULL']._serialized_end=3214
  _globals['_CONTAINS']._serialized_start=3216
  _globals['_CONTAINS']._serialized_end=3274
  _globals['_LISTFN']._serialized_start=3276
  _globals['_LISTFN']._serialized_end=3374
  _globals['_MATHOP']._serialized_start=3377
  _globals['_MATHOP']._serialized_end=3642
  _globals['_ROUND']._serialized_start=3644
  _globals['_ROUND']._serialized_end=3670
  _globals['_ABS']._serialized_start=3672
  _globals['_ABS']._serialized_end=3677
  _globals['_CEIL']._serialized_start=3679
  _globals['_CEIL']._serialized_end=3685
  _globals['_FLOOR']._serialized_start=3687
  _globals['_FLOOR']._serialized_end=3694
  _globals['_TOSTRING']._serialized_start=3696
  _globals['_TOSTRING']._serialized_end=3706
  _globals['_MATHFN']._serialized_start=3708
  _globals['_MATHFN']._serialized_end=3809
  _globals['_STRUCTOP']._serialized_start=3811
  _globals['_STRUCTOP']._serialized_end=3849
  _globals['_STRUCTFN']._serialized_start=3851
  _globals['_STRUCTFN']._serialized_end=3955
  _globals['_DICTGET']._serialized_start=3957
  _globals['_DICTGET']._serialized_end=4066
  _globals['_DICTOP']._serialized_start=4069
  _globals['_DICTOP']._serialized_end=4237
  _globals['_DICTFN']._serialized_start=4239
  _globals['_DICTFN']._serialized_end=4337
  _globals['_STRINGOP']._serialized_start=4340
  _globals['_STRINGOP']._serialized_end=5008
  _globals['_TIMEZONE']._serialized_start=5010
  _globals['_TIMEZONE']._serialized_end=5038
  _globals['_JSONDECODE']._serialized_start=5040
  _globals['_JSONDECODE']._serialized_end=5098
  _globals['_STRPTIME']._serialized_start=5100
  _globals['_STRPTIME']._serialized_end=5179
  _globals['_TOLOWER']._serialized_start=5181
  _globals['_TOLOWER']._serialized_end=5190
  _globals['_TOUPPER']._serialized_start=5192
  _globals['_TOUPPER']._serialized_end=5201
  _globals['_STARTSWITH']._serialized_start=5203
  _globals['_STARTSWITH']._serialized_end=5259
  _globals['_ENDSWITH']._serialized_start=5261
  _globals['_ENDSWITH']._serialized_end=5315
  _globals['_CONCAT']._serialized_start=5317
  _globals['_CONCAT']._serialized_end=5371
  _globals['_STRINGFN']._serialized_start=5373
  _globals['_STRINGFN']._serialized_end=5477
  _globals['_DATETIMEFN']._serialized_start=5479
  _globals['_DATETIMEFN']._serialized_end=5589
  _globals['_DATETIMEOP']._serialized_start=5592
  _globals['_DATETIMEOP']._serialized_end=5826
  _globals['_SINCE']._serialized_start=5828
  _globals['_SINCE']._serialized_end=5930
  _globals['_SINCEEPOCH']._serialized_start=5932
  _globals['_SINCEEPOCH']._serialized_end=5993
  _globals['_STRFTIME']._serialized_start=5995
  _globals['_STRFTIME']._serialized_end=6074
  _globals['_PART']._serialized_start=6076
  _globals['_PART']._serialized_end=6184
  _globals['_SPLIT']._serialized_start=6186
  _globals['_SPLIT']._serialized_end=6206
  _globals['_JSONEXTRACT']._serialized_start=6208
  _globals['_JSONEXTRACT']._serialized_end=6235
  _globals['_TOINT']._serialized_start=6237
  _globals['_TOINT']._serialized_end=6244
# @@protoc_insertion_point(module_scope)
