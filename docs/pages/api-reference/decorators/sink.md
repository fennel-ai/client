---
title: Sink Decorator
order: 0
status: published
---
### Sink
All Fennel sinks are wrapped in the `@sink` decorator applied on top of the
datasets. This decorator specifies a bunch of options to configure the sink
mechanism that apply to most data sinks.

#### Parameters

<Expandable title="every" type="Duration" defaultVal='"1h"'>
The frequency with which the sink should be carried out. Streaming sinks 
like Kafka, Kinesis, Webhook ignore it since they do continuous polling.

Note that some Fennel sinks make multiple round-trips of limited size in a single
iteration so as to not overload the system - `every` only applies across full 
iterations of ingestion.

</Expandable>

<Expandable title="since" type="Optional[datetime]" defaultVal="None">
When `since` is set, the sink only admits those rows that where the value
corresponding to the `timestamp` column of the dataset will be >= `since`.
</Expandable>

<Expandable title="until" type="Optional[datetime]" defaultVal="None">
When `until` is set, the sink only admits those rows that where the value
corresponding to the `timestamp` column of the dataset will be < `until`.
</Expandable>

<Expandable title="cdc" type='Optional[Literal["debezium"]]'>
Specifies how should valid change data be used for the sink data.

`"debezium"` means that the raw data itself is laid out in debezium layout out
of which valid CDC data can be constructed. This is only possible for kafka sink
as of now
</Expandable>

<Expandable title="env" type="None | str | List[str]" defaultVal="None">
When present, marks this sink to be selected during `commit` only when `commit`
operation itself is made for a `env` that matches this env. Primary use case is to
decorate a single dataset with many `@sink` decorators and choose only one of 
them to commit depending on the environment.
</Expandable>

<Expandable title="how" type='Optional[Literal["Incremental", "Recreate"] | SnapshotData]' defaultVal="None">
This denotes the style of sink
* Incremental denotes we incrementally sink the new changes to data
* Recreate denotes we recreate the entire sink every time
* SnapshotData denotes we sink only the current snapshot of dataset

:::info
Fennel supports only Increment style of sink. If you want the style to be either Recreate or SnapshotData,
please reach out to Fennel support.
:::
</Expandable>

<Expandable title="renames" type="Optional[Dict[str, str]]" defaultVal="None">
This means that the column denoted by the key is aliased to another column in the sink data.
This is useful, for instance, when you want to rename columns while sinking them.
</Expandable>

<pre snippet="api-reference/sinks/sink#sink_decorator"
    status="success" message="Specifying options in sink decorator"
>
</pre>