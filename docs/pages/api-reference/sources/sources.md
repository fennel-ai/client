---
title: Sources
order: 6
status: 'published'
---

# Sources

Here is the description of all the external sources supported by Fennel and how to use them:

### Webhook

The Webhook source operates on a push-based mechanism, making it convenient for sending data to Fennel.
There are two options for pushing data into Fennel: utilizing the Fennel Python SDK or employing the REST API.

The following fields need to be specified:

1. `name` - A name to identify the source. The name should be unique across all sources.

And the following fields need to be defined on the webhook:

1. `endpoint` - The endpoint for the given webhook to which the data will be sent.

<pre snippet="api-reference/source#webhook_source"></pre>

To use the REST api check the [REST API](/api-reference/rest-api) documentation.

### MySQL

The following fields need to be specified:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. **`host`** - The host name of the database.
3. **`port`** - The port to connect to. By default it is 3303 for MySQL and 5432 for Postgres.
4. **`db_name`** - The database name.
5. **`username`** - The username which is used to access the database.
6. **`password`** - The password associated with the username.
7. **`jdbc_params`** - Additional properties to pass to the JDBC URL string when connecting to the database formatted
   as `key=value` pairs separated by the symbol `&`. (example: `key1=value1&key2=value2&key3=value3`).

<pre snippet="api-reference/source#mysql_source"></pre>

:::warning
If you see a `Cannot create a PoolableConnectionFactory`error, try setting `jdbc_params`
to `enabledTLSProtocols=TLSv1.2`
:::

### Postgres

<pre snippet="api-reference/source#postgres_source"></pre>

:::warning
If you see a `Cannot create a PoolableConnectionFactory`error, try setting `jdbc_params`
to `enabledTLSProtocols=TLSv1.2`
:::

### S3

The following fields need to be defined on the source:

1. `name` - A name to identify the source. The name should be unique across all sources.
2. `aws_access_key_id` (optional) - AWS Access Key ID. This field is not required if role-based access is used or if
   the bucket is public.
3. `aws_secret_access_key` (optional) - AWS Secret Access Key. This field is not required if role-based access is
   used or if the bucket is public.

:::info
Fennel creates a special role with name prefixed by `FennelDataAccessRole-` in your AWS account for role-based access.
:::

The following fields need to be defined on the bucket:

1. **`bucket`** - Name of the S3 bucket where the file(s) exist.
2. **`path`** (optional) - **At most 1 of `prefix` or `path` should be specified.** A pattern describing the path structure
   of the objects in S3. When such structure exists in your bucket, this is highly recommended. From this, Fennel can infer 
   the partitioning scheme within the buckets, which the system uses to optimize ingestion. The pattern specified is a 
   path-like string consisting of parts separated by `/`. The valid path parts are:
   - a static string (of alphanumeric characters, underscores, hyphens or dots)
   - the `*` wild card (note this must be the entire path part: `*/*` is valid but `foo*/` is not)
   - a string with a [strftime format specifier](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) (e.g `yyyymmdd=%Y%m%d`)

   
   For example if your bucket has the structure `orders/country={country}/date={date}/store={store}/{file}.json`, provide the
   path `orders/*/date=%Y%m%d/*/*`   
3. **`prefix`** (optional) - **At most 1 of `prefix` or `path` should be specified.** By providing a path-like prefix 
   (e.g., `myFolder/thisTable/`) under which all the relevant files sit, we can optimize finding these in S3. 
   This is optional but recommended if your bucket contains many folders/files&#x20;
4. **`format`** (optional) - The format of the files you'd like to replicate. You can choose between CSV (default),
   Avro, Hudi, Parquet, and JSON &#x20; 
5. **`delimiter`** (optional) - the character delimiting individual cells in the CSV data. The default value is `","`
   and if overridden, this can only be a 1-character string. For example, to use tab-delimited data enter `"\t"`.

Here are 2 examples, one using path and the other using prefix
<pre snippet="api-reference/source#s3_source"></pre>


Fennel uses  `file_last_modified` property exported by S3 to track what data has been seen so far and hence a cursor
field doesn't need to be specified.

### BigQuery

The following fields need to be specified:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. **`project_id`** - The project ID of the Google Cloud project containing the BigQuery dataset.
3. **`dataset_id`** - The ID of the BigQuery dataset containing the table(s) to replicate.
4. **`credentials_json`** - The JSON string containing the credentials for the Service Account to use to access
   BigQuery. See below for instructions on how to obtain this.

<details>

<summary>How to obtain credentials? </summary>

Interfacing with BigQuery requires credentials for
a [Service Account](https://cloud.google.com/iam/docs/service-accounts) with the "BigQuery User" and "BigQuery Data
Editor" roles, which grants permissions to run BigQuery jobs, write to BigQuery Datasets, and read table metadata. It is
highly recommended that this Service Account is exclusive to Fennel for ease of permissions and auditing. However, you
can also use a preexisting Service Account if you already have one with the correct permissions.

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

For Service Account Key JSON, enter the Google
Cloud [Service Account Key in JSON format](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

</details>

### Snowflake

The following fields need to be defined:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. **`account`** - Snowflake account identifier. This is the first part of the URL used to access Snowflake. For example,
   if the URL is `https://<account>.snowflakecomputing.com`, then the account is `<account>`. This is usually of the form
   `<ORG_ID>-<ACCOUNT_ID>`. Refer to the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/admin-account-identifier#finding-the-organization-and-account-name-for-an-account) to find the account identifier.
3. **`role`** - The role that Fennel should use to access Snowflake.
4. **`warehouse`** - The warehouse that Fennel should use to access Snowflake
5. **`db_name`** - The database where the required data resides.
6. **`src_schema`** - The schema where the required data table resides.
7. **`username`**  - The username that should be used to access Snowflake. Please note that the username should have the
   required permissions to assume the role provided.
8. **`password`** - The password associated with the username.

<pre snippet="api-reference/source#snowflake_source"></pre>


:::info
Currently, Fennel only supports OAuth 1 (username and password) authentication. We are happy to prioritize support for
OAuth 2.0 if needed - if so, please talk to us!
:::

### Hudi

Fennel integrates with Apache Hudi via its S3 connector. To use Hudi, simply set the `format` field to "hudi" when
configuring the S3 bucket.

<pre snippet="api-reference/source#s3_hudi_source"></pre>

### Delta Lake

Similar to Hudi, Fennel integrates with Delta Lake via its S3 connector. To use delta lake, simply set the `format` field to "delta" when configuring the S3 bucket.

<pre snippet="api-reference/source#s3_delta_lake_source"></pre>

### Kafka

The following fields need to be defined for the source:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. **`bootstrap_servers`** - A list of broker host or host\:port.
3. **`security_protocol`** - Protocol used to communicate with brokers. Supported PLAINTEXT, SASL_PLAINTEXT, and SASL_SSL.
4. **`sasl_mechanism`** - SASL mechanism to use for authentication. For example, SCRAM-SHA-256, PLAIN.
5. **`sasl_plain_username`** - SASL username.
6. **`sasl_plain_password`** - SASL password.
7. **`verify_cert`** - Enable OpenSSL's builtin broker (server) certificate verification. Default is true.

The following fields need to be defined on the topic:
1. **`topic`** - The kafka topic.


<pre snippet="api-reference/source#kafka_source"></pre>

### Kinesis

#### PARAMETERS FOR DEFINING SOURCE
----
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel sources
across all branches.
</Expandable>

<Expandable title="role_arn" type="str">
The arn of the role that Fennel should use to access the Kinesis stream. The role
must already exist and Fennel's principal must have been given the permission to 
assume this role - talk to Fennel support if you need help. Providing a role
that Fennel is unable to assume will result in an error during the sync operation.
</Expandable>

#### PARAMETERS FOR CONNECTING WITH SOURCE
---

<Expandable title="stream_arn" type="str">
The arn of the Kinesis stream that needs to be sourced. The corresponding 
`role_arn` must have appropriate permissions for this stream. Providing a stream
that either doesn't exist or can not be read using the given `role_arn` will
result in an error during the sync operation.
</Expandable>

<Expandable title="init_position" type="str | datetime | float | int">
The Kinesis `ShardIterator` type used to determine the initial position to start consuming
from. Allowed values are:
- `"latest"` - start streaming from the latest data (starting a few minutes after `sync`)
- `"trim_horizon"`- start streaming at the last untrimmed record in the shard, 
   which is the oldest data record in the shard.
- datetime - start streaming from the position denoted by the time stamp 
  specified in the Timestamp field (i.e. equivalent to AT_TIMESTAMP in Kinesis
  vocabulary). The timestamp can be specified in any of the
  following ways:
   - a `datetime` object
   - an `int` representing seconds since the epoch
   - a `float` representing `{seconds}.{microseconds}` since the epoch
   - an [ISO-8601](https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat) formatted `str`.

See [Kinesis ShardIteratorType](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax) for more context. 
</Expandable>

<Expandable title="format" type='Literal["json"]'>
The format of the incoming data. Currently only `json` is supported
</Expandable>


**Example Using Explicit Timestamp**

<pre snippet="api-reference/source#kinesis_source"></pre>

**Example Using Latest**

<pre snippet="api-reference/source#kinesis_source_latest"></pre>

**Managing Kinesis Access**

Fennel creates a special role with name prefixed by `FennelDataAccessRole-` in 
your AWS account for role-based access. The role with access to the kinesis 
stream should have the following trust policy allowing this role to assume the 
kinesis role. 

<details>

<summary>See Trust Policy </summary>

The role should have the following trust policy. Specify the exact `role_arn` in the form
`arn:aws:iam::<fennel-data-plane-account-id>:role/<FennelDataAccessRole-...>` without any wildcards.
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "<role_arn>"
                ]
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```

Also attach the following permission policy. Add more streams to the Resource 
field if more than one streams need to be consumed via this role. Here 
the `account-id` is your account where the stream lives.

```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowKinesisAccess",
      "Effect": "Allow",
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary",
        "kinesis:DescribeStreamConsumer",
        "kinesis:RegisterStreamConsumer",
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:SubscribeToShard",
        "kinesis:GetRecords"
      ],
      "Resource": [
        "arn:aws:kinesis:<region>:<account-id>:stream/<stream-name>",
        "arn:aws:kinesis:<region>:<account-id>:stream/<stream-name>/*"
      ]
    }
  ]
}
```

</details>
