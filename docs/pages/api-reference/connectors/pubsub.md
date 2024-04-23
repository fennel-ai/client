---
title: Pub/Sub
order: 0
status: published
---
### Pub/Sub
Data connector to Google Pub/Sub messaging service.

#### Project Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel sources.
</Expandable>

<Expandable title="project_id" type="str">
The project ID of the Google Cloud project containing the Pub/Sub topic
</Expandable>

<Expandable title="credentials_json" type="str">
The JSON string containing the credentials for the Service Account to use to access
Pub/Sub topic. See below for instructions on how to obtain this.
</Expandable>

:::info
Fennel supports only Upsert mode CDC with data in JSON format. If you require support
for schema or CDC data format, please reach out to Fennel support.
:::

#### Topic Parameters
<Expandable title="topic_id" type="str">
The name of the topic from which the data should be ingested.
</Expandable>

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your Pub/Sub topic during commit itself so any
connectivity issue (e.g. wrong project_id or credentials etc.) is flagged as
as an error during commit with the real Fennel servers.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>

#### Pub/Sub Credentials

<Expandable title="How to obtain credentials?">
Interfacing with Pub/Sub requires credentials for
a [Service Account](https://cloud.google.com/iam/docs/service-accounts) with the "Pub/Sub Subscriber" role, 
which grants permissions to create subscription and read messages from the subscribed topic. It is
highly recommended that this Service Account is exclusive to Fennel for ease of permissions and auditing. However, you
can also use a preexisting Service Account if you already have one with the correct permissions.

The easiest way to create a Service Account is to follow GCP's guide
for [Creating a Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts). Once you've
created the Service Account, make sure to keep its ID handy, as you will need to reference it when granting roles.
Service Account IDs typically take the form `<account-name>@<project-name>.iam.gserviceaccount.com`

Then, add the service account as a Member of your Google Cloud Project with the "Pub/Sub Subscriber" role. To do this, follow
the instructions
for [Granting Access](https://cloud.google.com/iam/docs/granting-changing-revoking-access#granting-console) in the
Google documentation. The email address of the member you are adding is the same as the Service Account ID you just
created.

At this point, you should have a service account with the "Pub/Sub Subscriber" project-level permission.

For Service Account Key JSON, enter the Google
Cloud [Service Account Key in JSON format](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

</Expandable>

<pre snippet="api-reference/sources/sql#pubsub_source"></pre>