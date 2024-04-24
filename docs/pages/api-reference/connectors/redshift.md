---
title: Redshift
order: 0
status: published
---
### Redshift
Data connector to Redshift databases.

#### Database Parameters
<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel sources.
</Expandable>

<Expandable title="s3_access_role_arn" type="str">
IAM role to be used by Redshift to access S3. Redshift uses S3 as middle-man while executing large queries.
Steps to set up IAM role:
- Create an IAM role by following this [documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-creating-an-iam-role). Make sure to provide full access to S3 since we store temporary data in S3 and read from it
- Associate IAM role with Redshift cluster by following this [documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/copy-unload-iam-role.html#copy-unload-iam-role-associating-with-clusters). Refer to a sample policy 
below.
</Expandable>

<Expandable title="db_name" type="str">
The name of the database where the relevant data resides.
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