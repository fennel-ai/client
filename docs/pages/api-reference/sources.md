---
title: Sources
order: 6
status: wip
---

# Sources

Here is the description of all the external sources supported by Fennel and how to use them:

### **MySQL**

The following fields need to be specified:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. **`host`** - The host name of the database.
3. **`port`** - The port to connect to. By default it is 3303 for MySQL and 5432 for Posgres.
4. **`db_name`** - The database name.
5. **`username`** - The username which is used to access the database.
6. **`password`** - The password associated with the username.
7. **`jdbc_params`** - Additional properties to pass to the JDBC URL string when connecting to the database formatted as `key=value` pairs separated by the symbol `&`. (example: `key1=value1&key2=value2&key3=value3`).

```python
from fennel import sources
from source import source

mysql = sources.MySQL(
    name='py_mysql_src',
    host="my-favourite-mysql.us-west-2.rds.amazonaws.com",
    port=3306,
    db_name="some_database_name",
    username="admin",
    password="password",
    jdbc_params="enabledTLSProtocols=TLSv1.2",
)

@source(mysql.table('user'), cursor='update_time', every='1m')
@dataset
class User:
    uid: int = field(key=True)
    email: str
    ...
```

:::warning
If you see a `Cannot create a PoolableConnectionFactory`error, try setting `jdbc_params` to `enabledTLSProtocols=TLSv1.2`&#x20;
:::

TODO

### Postgres

```python
postgres = sources.Postgres(
    name='py_psql_src',
    host="my-favourite-postgres.us-west-2.rds.amazonaws.com",
    db_name="some_database_name",
    username="admin",
    password="password",
)

@source(postgres.table('user'), cursor='update_time', every='1m')
@dataset
class User:
    uid: int
    ...

```

:::warning
If you see a `Cannot create a PoolableConnectionFactory`error, try setting **`jdbc_params` ** to **** `enabledTLSProtocols=TLSv1.2`&#x20;
:::



TODO

### S3

The following fields need to be defined on the source:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. that you don't need to replicate.
3. **`aws_access_key_id`** - In order to access private Buckets stored on AWS S3, this connector requires credentials with the proper permissions. If accessing publicly available data, this field is not required.
4. **`aws_secret_access_key` **_**-**_ In order to access private S3 Buckets, this connector requires credentials with the proper permissions. If accessing publicly available data, this field is not required.

And the following fields need to be defined on the bucket:

1. **`bucket`** - Name of the S3 bucket where the file(s) exist.
2. **`path_prefix`** (optional)- By providing a path-like prefix (e.g., `myFolder/thisTable/`) under which all the relevant files sit, we can optimize finding these in S3. This is optional but recommended if your bucket contains many folders/files&#x20;
3. **`format` ** (optional) **-** The format of the files you'd like to replicate. You can choose between CSV (default), Avro, and Parquet.&#x20;
4. **`delimiter`** (optional) - the character delimiting individual cells in the CSV data. The default value is `","` and if overridden, this can only be a 1-character string. For example, to use tab-delimited data enter `"\t"`.

```python
s3 = sources.S3(
    name='ratings_source',
    aws_access_key_id="<SOME_ACCESS_KEY>",
    aws_secret_access_key="<SOME_SECRET_ACCESS_KEY>",
)

@source(s3.bucket("engagement", prefix="notion"), every="30m")
@meta(owner='abc@email.com')
@dataset
class User:
    uid: int = field(key=True)
    email: str
    ...
```

Fennel uses  `file_last_modified` property exported by S3 to track what data has been seen so far and hence a cursor field doesn't need to be specified.



### BigQuery

**Obtaining the credentials**

Interfacing with BigQuery requires credentials for a [Service Account](https://cloud.google.com/iam/docs/service-accounts) with the "BigQuery User" and "BigQuery Data Editor" roles, which grants permissions to run BigQuery jobs, write to BigQuery Datasets, and read table metadata. It is highly recommended that this Service Account is exclusive to Fennel for ease of permissions and auditing. However, you can also use a pre-existing Service Account if you already have one with the correct permissions.

The easiest way to create a Service Account is to follow GCP's guide for [Creating a Service Account](https://cloud.google.com/iam/docs/creating-managing-service-accounts). Once you've created the Service Account, make sure to keep its ID handy, as you will need to reference it when granting roles. Service Account IDs typically take the form `<account-name>@<project-name>.iam.gserviceaccount.com`

Then, add the service account as a Member of your Google Cloud Project with the "BigQuery User" role. To do this, follow the instructions for [Granting Access](https://cloud.google.com/iam/docs/granting-changing-revoking-access#granting-console) in the Google documentation. The email address of the member you are adding is the same as the Service Account ID you just created.

At this point, you should have a service account with the "BigQuery User" project-level permission.

For Service Account Key JSON, enter the Google Cloud [Service Account Key in JSON format](https://cloud.google.com/iam/docs/creating-managing-service-account-keys).

TODO

### Kafka

TODO

### Redshift

TODO

### Snowflake

The following fields need to be defined:

1. **`name`** - A name to identify the source. The name should be unique across all sources.
2. **`host`** - The host domain of the Snowflake instance (must include the account, region and cloud environment, and end with snowflakecomputing.com). Example: `accountname.us-east-2.aws.snowflakecomputing.com`.
3. **`role`** - The role that Fennel should use to access Snowflake.
4. **`warehouse`** - The warehouse that Fennel should use to access Snowflake
5. **`db_name` **_**-**_ The database where the required data resides.
6. **`schema`** - The default schema used as the target schema for all statements issued from the connection that do not explicitly specify a schema name.
7. **`username`**  - The username that should be used to access Snowflake. Please note that the username should have the required permissions to assume the role provided.
8. **`password` ** **-** The password associated with the username.

```python
from fennel import sources

sf_src = sources.Snowflake(
    name = "snowflake_src",
    host="nhb38793.us-west-2.snowflakecomputing.com", 
    warehouse="TEST",
    schema="PUBLIC",
    role="ACCOUNTADMIN",
    username="<username>",
    password="<password>",
)
```

:::info
Currently, Fennel only supports OAuth 1 (username and password) authentication. We are happy to prioritize support for OAuth 2.0 if needed - if so, please talk to us!
:::

TODO

