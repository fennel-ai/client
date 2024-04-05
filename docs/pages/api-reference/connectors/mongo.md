---
title: MongoDB
order: 0
status: published
---
### MongoDB
Data connector to MongoDB databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel sources.
</Expandable>

<Expandable title="host" type="str">
The hostname of the database.
</Expandable>

<Expandable title="db_name" type="str">
The name of the Mongo database to establish a connection with.
</Expandable>

<Expandable title="username" type="str">
The username which should be used to access the database. This username should 
have access to the database `db_name`.
</Expandable>

<Expandable title="password" type="str">
The password associated with the username.
</Expandable>

:::info
Fennel uses [SRV connection format](https://www.mongodb.com/docs/manual/reference/connection-string/#std-label-connections-dns-seedlist) 
for authentication which is supported in Mongo versions 3.6 and later. If you have 
a self-hosted DB with version earlier than 3.6, please reach out to Fennel support.
:::

#### Table Parameters
<Expandable title="table" type="str">
The name of the table within the database that should be ingested.
</Expandable>

<Expandable title="cursor" type="str">
The name of the field in the table that acts as `cursor` for ingestion i.e. 
a field that is approximately monotonic and only goes up with time. 

Fennel issues queries of the form `db.collection.find({"cursor": { "$gte": last_cursor - disorder } })`
to get data it hasn't seen before. Auto increment IDs or timestamps corresponding
to `modified_at` (vs `created_at` unless the field doesn't change) are good
contenders.

Note that this field doesn't even need to be a part of the Fennel dataset. 
</Expandable>

:::warning
It is recommended to put an index on the `cursor` field so that Fennel ingestion
queries don't create too much load on your MongoDB database.
:::

<pre snippet="api-reference/sources/sql#mongo_source"
    status="success" message="Sourcing dataset from a mongo collection">
</pre>

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your MongoDB during `commit` itself so any
connectivity issue (e.g. wrong host name, username, password etc) is flagged
as an error during commit with the real Fennel servers.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>