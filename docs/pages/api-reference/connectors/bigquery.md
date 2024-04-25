---
title: BigQuery
order: 0
status: published
---
### BigQuery
Data connector to Google BigQuery databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel sources.
</Expandable>

<Expandable title="project_id" type="str">
The project ID of the Google Cloud project containing the BigQuery dataset.
</Expandable>

<Expandable title="dataset_id" type="str">
The ID of the BigQuery dataset containing the table(s) to replicate.
</Expandable>

<Expandable title="service_account_key" type="Dict[str, str]">
A dictionary containing the credentials for the Service Account to use to access
BigQuery. See below for instructions on how to obtain this.
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
Fennel tries to test the connection with your BigQuery during commit itself so any
connectivity issue (e.g. wrong project_id or credentials etc.) is flagged as
as an error during commit with the real Fennel servers.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>

#### BigQuery Credentials

<Expandable title="How to obtain credentials?">
Interfacing with BigQuery requires credentials for
a [Service Account](https://cloud.google.com/iam/docs/service-accounts) with the "BigQuery User" role at Project level
and "BigQuery Data Editor" role at Dataset level. "BigQuery User" role grants permissions to run BigQuery jobs and 
"BigQuery Data Editor" role grants permissions to read and update table data and its metadata. It is highly recommended 
that this Service Account is exclusive to Fennel for ease of permissions and auditing. However, you can also use a 
preexisting Service Account if you already have one with the correct permissions.

The easiest way to create a Service Account is to follow GCP's guide
for [Creating a Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts). Once you've
created the Service Account, make sure to keep its ID handy, as you will need to reference it when granting roles.
Service Account IDs typically take the form `<account-name>@<project-name>.iam.gserviceaccount.com`

Then, add the service account as a Member of your Google Cloud Project with the "BigQuery User" role. To do this, follow
the instructions
for [Granting Access](https://cloud.google.com/iam/docs/granting-changing-revoking-access#granting-console) in the
Google documentation. The email address of the member you are adding is the same as the Service Account ID you just
created.

At this point, you should have a service account with the "BigQuery User" project-level permission.

Similarly, provide the "BigQuery Data Editor" permission to the service account by following 
[Granting Access to Dataset](https://cloud.google.com/bigquery/docs/control-access-to-resources-iam#grant_access_to_a_dataset)
in the Google documentation.

To obtain a Service Account Key, follow the instructions on
[Creating a Service Account Key](https://cloud.google.com/iam/docs/keys-create-delete#creating).

</Expandable>

<pre snippet="api-reference/sources/sql#bigquery_source"></pre>