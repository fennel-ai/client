---
title: Redshift
order: 0
status: draft
---
### Redshift

<Divider>
<LeftSection>
Data connector to Snowflake databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel sources.
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
The name of the database where the relevant data resides.
</Expandable>

<Expandable title="src_schema" type="str">
The schema where the required data table(s) resides.
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

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your Snowflake during sync itself so any
connectivity issue (e.g. wrong host name, username, password etc) is flagged as
as an error during sync with the real Fennel servers.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at sync time.
</Expandable>


</LeftSection>
<RightSection>
<pre snippet="api-reference/sources/sql#snowflake_source"></pre>
</RightSection>
</Divider>




