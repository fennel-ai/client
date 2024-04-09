---
title: Kafka
order: 0
status: published
---
### Kafka
Data connector to any data store that speaks the Kafka protocol (e.g. Native 
Kafka, MSK, Redpanda etc.)

#### Cluster Parameters
<Expandable title="name" type="str">
A name to identify the source. This name should be unique across ALL sources.
</Expandable>

<Expandable title="bootstrap_servers" type="str">
This is a list of the addresses of the Kafka brokers in a "bootstrap" Kafka 
cluster that a Kafka client connects to initially to bootstrap itself and discover
the rest of the brokers in the cluster.

Addresses are written as host & port pairs and can be specified either as a 
single server (e.g. `localhost:9092`) or a comma separated list of several 
servers (e.g. `localhost:9092,another.host:9092`).
</Expandable>


<Expandable title="security_protocol" type='"PLAINTEXT" | "SASL_PLAINTEXT" | "SASL_SSL"'>
Protocol used to communicate with the brokers. 
</Expandable>

<Expandable title="sasl_mechanism" type='"PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"'>
SASL mechanism to use for authentication. 
</Expandable>

<Expandable title="sasl_plain_username" type="str">
SASL username.
</Expandable>

<Expandable title="sasl_plain_password" type="str">
SASL password.
</Expandable>

#### Topic Parameters

<Expandable title="topic" type="str">
The name of the kafka topic that needs to be sourced into the dataset.
</Expandable>

<Expandable title="format" type='"json" | Avro' defaultVal="json">
The format of the data in Kafka topic. Both `"json"` and 
[Avro](/api-reference/sources/avro) supported.
</Expandable>

<pre snippet="api-reference/sources/kafka#basic"
    status="success" message="Sourcing json data from kafka to a dataset"
></pre>

<pre snippet="api-reference/sinks/kafka_sinks#basic"
    status="success" message="Capturing change from a dataset to a Kafka Sink"
></pre>

#### Errors
<Expandable title="Connectivity problems">
Fennel server tries to connect with the Kafka broker during the `commit` operation
itself to validate connectivity - as a result, incorrect URL/Username/Password
etc will be caught at commit time itself as an error.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>

<Expandable title="Schema mismatch errors">
Schema validity of data in Kafka can only be checked at runtime. Any rows that 
can not be parsed are rejected. Please keep an eye on the 'Errors' tab of 
Fennel console after initiating any data sync.
</Expandable>