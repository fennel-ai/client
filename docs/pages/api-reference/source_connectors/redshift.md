---
title: Redshift
order: 0
status: published
---
### Redshift
Data connector to Redshift databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel connectors.
</Expandable>

<Expandable title="s3_access_role_arn" type="Optional[str]">
To handle potentially large volume of data, Fennel asks Redshift to dump
query results in a temporary S3 bucket (since it's faster to go via S3). But this
requires Redshift to be able to access that S3 bucket. `s3_access_role_arn` is
the IAM role ARN that Redshift should use to access S3. 

This IAM role should be given full access to S3 and should also be assumable by
your Redshift.

Steps to set up IAM role:
- Create an IAM role by following this [documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-creating-an-iam-role). 
- Provide full access to S3 this role
- Associate IAM role with Redshift cluster by following this [documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html#copy-unload-iam-role-associating-with-clusters). 
 
You can refer to a sample policy in the right side code snippets.
Do not set this parameter when using username/password for authentication
</Expandable>

<Expandable title="db_name" type="str">
The name of the database where the relevant data resides.
</Expandable>

<Expandable title="username" type="Optional[str] | Optional[Secret]">
The username which should be used to access the database. This username should have access to the 
database `db_name`. Do not set this parameter when using IAM authentication
</Expandable>

<Expandable title="password" type="Optional[str] | Optional[Secret]">
The password associated with the username. Do not set this parameter when using IAM authentication
</Expandable>

<Expandable title="host" type="str">
The hostname of the database.
</Expandable>

<Expandable title="port" type="Optional[int]" defaultVal="5439">
The port to connect to.
</Expandable>

<Expandable title="schema" type="str">
The name of the schema where the required data table(s) resides.
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


:::info
For large datasets, it is recommended to use IAM-based authentication, as
username/password-based authentication does not store temporary data in S3
:::

#### Errors
<Expandable title="Connectivity Issues">
Fennel tries to test the connection with your Redshift during `commit` itself so any
connectivity issue (e.g. wrong database name, host name, port, etc) is flagged as
as an error during commit with the real Fennel servers.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>

<Expandable title="Schema mismatch errors">
Schema validity of data in Redshift is checked at runtime. Any rows that 
can not be parsed are rejected. Please keep an eye on the 'Errors' tab of 
Fennel console after initiating any data sync.
</Expandable>

<pre snippet="api-reference/sources/sql#redshift_source"
    status="success" message="Bringing Redshift data into Fennel">
</pre>

```JSON message="Sample IAM policy for integrating with Redshift"
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "redshift:DescribeClusters",
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "redshift:ModifyClusterIamRoles",
                "redshift:CreateCluster"
            ],
            "Resource": [
                # Redshift workgroup ARN
                "arn:aws:redshift-serverless:us-west-2:82448945123:workgroup/0541e0ae-2ad1-4fe0-b2f3-4d6c1d3453e" 
            ]
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": [
                # ARN of role created above
                "arn:aws:iam::82448945123:role/RedshiftS3AccessRole", 
            ]
        }
    ]
}
```