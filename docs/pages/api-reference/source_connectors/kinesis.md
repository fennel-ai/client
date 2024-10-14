---
title: Kinesis
order: 0
status: published
---
### Kinesis
Data connector to ingest data from AWS Kinesis.

#### Parameters for Defining Source

<Expandable title="name" type="str">
A name to identify the source. The name should be unique across all Fennel connectors.
</Expandable>

<Expandable title="role_arn" type="str">
The arn of the role that Fennel should use to access the Kinesis stream. The role
must already exist and Fennel's principal must have been given the permission to 
assume this role (see below for details or talk to Fennel support if you need help).
</Expandable>


#### Stream Parameters

<Expandable title="stream_arn" type="str">
The arn of the Kinesis stream. The corresponding `role_arn` must have 
appropriate permissions for this stream. Providing a stream that either doesn't 
exist or can not be read using the given `role_arn` will result in an error 
during the commit operation.
</Expandable>

<Expandable title="init_position" type="str | datetime | float | int">
The initial position in the stream from which Fennel should start ingestion. 
See [Kinesis ShardIteratorType](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax) for more context. Allowed values are:
- `"latest"` - start from the latest data (starting a few minutes after `commit`)
- `"trim_horizon"`- start from the oldest data that hasn't been trimmed/expired yet.
- datetime - start from the position denoted by this timestamp (i.e. equivalent 
  to `AT_TIMESTAMP` in Kinesis vocabulary). 

If choosing the datetime option, the timestamp can be specified as a `datetime` 
object, or as an `int` representing seconds since the epoch, or as a `float` 
representing `{seconds}.{microseconds}` since the epoch or as an [ISO-8601](https://docs.python.org/3/library/datetime.html#datetime.date.fromisoformat) formatted `str`.

Note that this timestamp is the time attached with the Kinesis message itself at
the time of production, not any timestamp field inside the message.

</Expandable>

<Expandable title="format" type='"json" | Avro'>
The format of the data in the Kinesis stream. Most common value is `"json"` 
though Fennel also supports [Avro](/api-reference/source_connectors/avro).
</Expandable>

#### Errors
<Expandable title="Connectivity problems">
Fennel server tries to connect with Kinesis during the `commit` operation
itself to validate connectivity - as a result, incorrect stream/role ARNs or 
insufficient permissions will be caught at commit time itself as an error.

Note: Mock client can not talk to any external data source and hence is unable to
do this validation at commit time.
</Expandable>

<Expandable title="Schema mismatch errors">
Schema validity of data only be checked at runtime. Any rows that 
can not be parsed are rejected. Please keep an eye on the 'Errors' tab of 
Fennel console after initiating any data sync.
</Expandable>

<pre snippet="api-reference/sources/kinesis#kinesis_at_timestamp"
    status="success" message="Using explicit timestamp as init position"
></pre>

<pre snippet="api-reference/sources/kinesis#kinesis_latest"
    status="success" message="Using latest as init position">
</pre>

#### Managing Kinesis Access

Fennel creates a special role with name prefixed by `FennelDataAccessRole-` in 
your AWS account for role-based access. The role corresponding to the `role_arn`
passed to Kinesis source should have the following trust policy allowing this 
special Fennel role to assume the kinesis role. 

<details>

<summary>See Trust Policy </summary>

Specify the exact `role_arn` in the form
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

