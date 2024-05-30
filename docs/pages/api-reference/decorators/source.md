---
title: Source Decorator
order: 0
status: published
---
### Source
All Fennel sources are wrapped in the `@source` decorator applied on top of the
datasets. This decorator specifies a bunch of options to configure the ingestion
mechanism that apply to most data sources.

#### Parameters

<Expandable title="every" type="Duration" defaultVal='"1h"'>
The frequency with which the ingestion should be carried out. Streaming sources 
like Kafka, Kinesis, Webhook ignore it since they do continuous polling.

Note that some Fennel sources make multiple round-trips of limited size in a single
iteration so as to not overload the system - `every` only applies across full 
iterations of ingestion.

</Expandable>

<Expandable title="since" type="Optional[datetime]" defaultVal="None">
When `since` is set, the source only admits those rows that where the value
corresponding to the `timestamp` column of the dataset will be >= `since`.

Fennel reads as little data as possible given this constraint - for instance, when
reading parquet files, the filter is pushed all the way down. However, in 
several cases, it's still necessary to read all the data before rejecting rows 
that are older than `since`.
</Expandable>

<Expandable title="until" type="Optional[datetime]" defaultVal="None">
When `until` is set, the source only admits those rows that where the value
corresponding to the `timestamp` column of the dataset will be < `until`.

Fennel reads as little data as possible given this constraint - for instance, when
reading parquet files, the filter is pushed all the way down. However, in 
several cases, it's still necessary to read all the data before rejecting rows 
that are newer than `until`.
</Expandable>

<Expandable title="disorder" type="Duration">
Specifies how out of order can data from this source arrive. 

Analogous to `MaxOutOfOrderness` in Flink, this provides Fennel a guarantee that
if some row with timestamp `t` has arrived, no other row with timestamp < `t-disorder`
can ever arrive. And if such rows do arrive, Fennel has the liberty of discarding
them and not including them in the computation.
</Expandable>

<Expandable title="cdc" type='"append" | "native" | "debezium"'>
Specifies how should valid change data be constructed from the ingested data.

`"append"` means that data should be interpreted as sequence of append operations
with no deletes and no updates. Append can only be applied to keyless datasets (
to prevent situations where multiple inserts arrive with the same key fields). As
of right now, all SQL sources, Kafka, Kinesis, S3, and webhook support `append`
mode.

`"upsert"` means that incoming data will only have inserts but should be 
interpreted as sequence of upsert operations. It can only be used for keyed 
datasets and works for every source where append works. Note that in order to 
support `"upsert"`, Fennel needs to maintain the last seen row for each key which
has some overhead. As a result, pre-prepared `"debezium"` data should be preferred
over `"upsert"`.

`"native"` means that the underlying system exposes CDC natively and that Fennel
should tap into that. As of right now, native CDC is only available for 
[Deltalake](/api-reference/connectors/deltalake) & [Hudi](/api-reference/connectors/hudi)
and will soon be available for more sources including MySQL and Postgres.

`"debezium"` means that the raw data itself is laid out in debezium layout out
of which valid CDC data can be constructed. This is only possible for sources
that expose raw schemaless data, namely, [s3](/api-reference/connectors/s3), 
[kinesis](/api-reference/connectors/kinesis), [kafka](/api-reference/connectors/kafka), 
and [webhook](/api-reference/connectors/webhook).
</Expandable>

<Expandable title="env" type="None | str | List[str]" defaultVal="None">
When present, marks this source to be selected during `commit` only when `commit`
operation itself is made for a `env` that matches this env. Primary use case is to
decorate a single dataset with many `@source` decorators and choose only one of 
them to commit depending on the environment.
</Expandable>

<Expandable title="preproc" type="Optional[Dict[str, Union[Ref, Any]]]" defaultVal="None">
When present, specifies the preproc behavior for the columns referred to by the
keys of the dictionary. 

As of right now, there are two kinds of values of preproc:
* `ref: Ref`: written as `ref(str)` and means that the column denoted
  by the key of this value is aliased to another column in the sourced data. This
  is useful, for instance, when you want to rename columns while bringing them
  to Fennel. With this, you can also perform indirections of kind A[B][C] and 
  renaming them while brining to fennel.

* `Any`: means that the column denoted by the key of this value should be given
  a constant value.

:::info
Fennel supports preproc ref(str) values of type A[B][C] only for the JSON format.
If you have data in other format, please reach out to Fennel support.
:::

</Expandable>


<Expandable title="bounded" type="bool" defaultVal="False">
When not set or set as `False`, it indicates that the source possesses infinite 
amount of data that is continuously increasing. 

When set as `True`, it indicates that the source possesses finite amount of data
and that it will exhaust at some point. In such cases, `idleness` must also be set.

</Expandable>


<Expandable title="idleness" type="Optional[Duration]" defaultVal="None">
Only relevant when `bounded` is set to `True` - in such cases, the bounded source
is assumed to have exhausted after Fennel is unable to obtain any new data despite
continuously asking for at least `idleness` period.

</Expandable>

<pre snippet="api-reference/sources/source#source_decorator"
    status="success" message="Specifying options in source decorator"
>
</pre>