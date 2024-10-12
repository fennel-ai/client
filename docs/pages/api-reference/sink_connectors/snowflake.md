---
title: Snowflake
order: 0
status: published
---
### Snowflake
Data sink to Snowflake databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the sink. The name should be unique across all Fennel connectors.
</Expandable>

<Expandable title="account" type="str">

Snowflake account identifier. This is the first part of the URL used to access 
Snowflake. For example, if the URL is `https://<account>.snowflakecomputing.com`, 
then the account is `<account>`. 

This is usually of the form `<ORG_ID>-<ACCOUNT_ID>`. Refer to the 
[Snowflake documentation](https://docs.snowflake.com/en/user-guide/admin-account-identifier#finding-the-organization-and-account-name-for-an-account) 
to find the account identifier.
</Expandable>

<Expandable title="role" type="str">
The role that should be used by Fennel to access Snowflake.
</Expandable>

<Expandable title="warehouse" type="str">
The warehouse that should be used to access Snowflake.
</Expandable>

<Expandable title="db_name" type="str">
The name of the database where the data has to be sinked.
</Expandable>

<Expandable title="schema" type="str">
The schema where the required data has to be sinked.
</Expandable>

<Expandable title="username" type="str">
The username which should be used to access Snowflake. This username should 
have required permissions to assume the provided `role`.
</Expandable>

<Expandable title="password" type="str">
The password associated with the username.
</Expandable>

#### Table Parameters
<Expandable title="table" type="str">
The prefix of the table within the database to which the data should be sinked.
</Expandable>

<pre snippet="api-reference/sinks/snowflake_sink#basic"
    status="success" message="Snowflake sink">
</pre>

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your Snowflake during `commit` itself so any
connectivity issue (e.g. wrong host name, username, password etc) is flagged as
as an error during commit with the real Fennel servers.

Note: Mock client can not talk to any external data sink and hence is unable to
do this validation at commit time.
</Expandable>

:::info
- Fennel appends a generated suffix to the provided table name to prevent ambiguities 
when the sink dataset version is updated or when multiple branches have the same 
sink defined. This suffix can be viewed in the 'Sink' tab of the console after 
initiating a data sink.
- Currently, Fennel only supports keyed dataset sinks to Snowflake. Support 
for keyless datasets will be added soon.
:::





