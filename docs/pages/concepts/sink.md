---
title: 'Sink'
order: 0
status: 'published'
---

# Sink

Analogous to [Sources](/concepts/source), Fennel also supports sinks to export 
data out of Fennel into your data stores.

<pre snippet="concepts/introduction#sink_main" status="success" 
  message="Writing a Fennel dataset to a Kafka topic">
</pre>

In this example, a regular Fennel dataset is being created using a pipeline. But 
it's desired to write it out to a Kafka topic as new updates arrive in the dataset.

Like Sources, first an object is created that knows how to connect with your
Kafka cluster. And `sink` decorator is applied on the dataset that needs to be 
written out - this decorator knows that destination if a Kafka topic and that
the CDC data needs to be written out in the debezium format.

That's it - once this is written, `UserLocationFiltered` dataset will start 
publishing changes to your Kafka.

Fennel ships with data sinks to a couple of [common datastores](/api-reference/sink_connectors) 
so that you can 'sink' from your Fennel datasets to your external datasets.
Sinks to many other common data stores will be added soon.

## Stacking Multiple Sinks
Stacking two (or more) sinks on top of a dataset simply leads to the dataset
getting sinked to all of them.

<pre snippet="concepts/sink#stacked" status="success" 
  message="Writing data to Snowflake and Kafka"
></pre>