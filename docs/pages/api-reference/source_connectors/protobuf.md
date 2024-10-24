---
title: Protobuf Registry
order: 0
status: published
---
### Protobuf Registry
Several Fennel sources work with Protobuf format. When using Protobuf, it's common
to keep the schemas in a centralized schema registry instead of including schema
with each message.

Fennel supports integration with protobuf schema registries.

#### Parameters

<Expandable title="registry" type='Literal["confluent"]'>
String denoting the provider of the registry. As of right now, Fennel only supports
"confluent" protobuf registry though more such schema registries may be added over
time.
</Expandable>

<Expandable title="url" type="str">
The URL where the schema registry is hosted.
</Expandable>

<Expandable title="username" type="Optional[str] | Optional[Secret]">
User name to access the schema registry (assuming the registry requires 
authentication). If user name is provided, corresponding password must also be
provided.

Assuming authentication is needed, either username/password must be provided or
a token, but not both.
</Expandable>

<Expandable title="password" type="Optional[str] | Optional[Secret]">
The password associated with the username.
</Expandable>

<Expandable title="token" type="Optional[str] | Optional[Secret]">
Token to be used for authentication with the schema registry. Only one of 
username/password or token must be provided.
</Expandable>

<pre snippet="api-reference/sources/kafka#kafka_with_protobuf"
    status="success" message="Using protobuf registry with kafka">
</pre>