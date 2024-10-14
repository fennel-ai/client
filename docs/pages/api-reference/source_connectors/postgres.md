---
title: Postgres
order: 0
status: published
---
### Postgres
Data connector to Postgres databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel connectors.
</Expandable>

<Expandable title="host" type="str">
The hostname of the database.
</Expandable>

<Expandable title="port" type="Optional[int]" defaultVal="5432">
The port to connect to.
</Expandable>

<Expandable title="db_name" type="str">
The name of the Postgres database to establish a connection with.
</Expandable>

<Expandable title="username" type="str | Secret">
The username which should be used to access the database. This username should 
have access to the database `db_name`.
</Expandable>

<Expandable title="password" type="str | Secret">
The password associated with the username.
</Expandable>

<Expandable title="jdbc_params" type="Optional[str]" defaultVal="None">
Additional properties to pass to the JDBC URL string when connecting to the 
database formatted as `key=value` pairs separated by the symbol `&`. For 
instance: `key1=value1&key2=value2`.
</Expandable>
:::error
If you see a 'Cannot create a PoolableConnectionFactory' error, try setting `jdbc_params`
to `enabledTLSProtocols=TLSv1.2`
:::


#### Table Parameters
<Expandable title="table" type="str">
The name of the table within the database that should be ingested.
</Expandable>

<Expandable title="cursor" type="str">
The name of the field in the table that acts as `cursor` for ingestion i.e. 
a field that is approximately monotonic and only goes up with time. 

Fennel issues queries of the form `select * from table where {cursor} >= {last_cursor - disorder}`
to get data it hasn't seen before. Auto increment IDs or timestamps corresponding
to `modified_at` (vs `created_at` unless the field doesn't change) are good
contenders.

Note that this field doesn't even need to be a part of the Fennel dataset. 
</Expandable>

:::warning
It is recommended to put an index on the `cursor` field so that Fennel ingestion
queries don't create too much load on your Postgres database.
:::

<pre snippet="api-reference/sources/sql#postgres_source"
    status="success" message="Sourcing dataset from a postgres table">
</pre>

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your Postgres during `commit` itself so any
connectivity issue (e.g. wrong host name, username, password etc) is flagged as
as an error during commit with the real Fennel servers.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>

<Expandable title="Schema mismatch errors">
Schema validity of data in Postgres is checked at runtime. Any rows that 
can not be parsed are rejected. Please keep an eye on the 'Errors' tab of 
Fennel console after initiating any data sync.
</Expandable>
