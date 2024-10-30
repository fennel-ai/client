---
title: Kafka
order: 0
status: published
---
### Kafka
Data sink to any data store that speaks the Kafka protocol (e.g. Native 
Kafka, MSK, Redpanda etc.)

#### Cluster Parameters
<Expandable title="name" type="str">
A name to identify the sink. This name should be unique across ALL connectors.
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

<Expandable title="sasl_mechanism" type='"PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512" | "GSSAPI"'>
SASL mechanism to use for authentication. 
</Expandable>

<Expandable title="sasl_plain_username" type="Optional[str] | Optional[Secret]">
SASL username.
</Expandable>

<Expandable title="sasl_plain_password" type="Optional[str] | Optional[Secret]">
SASL password.
</Expandable>

#### Topic Parameters

<Expandable title="topic" type="str">
The name of the kafka topic that needs to be sinked.
</Expandable>

<pre snippet="api-reference/sinks/kafka_sink#basic"
    status="success" message="Capturing change from a dataset to a Kafka sink"
></pre>

#### Errors
<Expandable title="Connectivity problems">
Fennel server tries to connect with the Kafka broker during the `commit` operation
itself to validate connectivity - as a result, incorrect URL/Username/Password
etc will be caught at commit time itself as an error.

Note: Mock client can not talk to any external data sink and hence is unable to
do this validation at commit time.
</Expandable>

:::info
- Fennel supports kafka sink with only the JSON debezium format. Given the ubiquity 
of debezium connectors, you should be able to further pipe this debezium data
from Kafka to your data store of choice. In case you require support for
other formats, please reach out to Fennel support.
:::
