---
title: 'Source'
order: 1
status: 'published'
---

# Source

Source is the ONLY way to get data into Fennel. There are two ways to source data into Fennel:

You can either log data into Fennel using a Webhook source or you can source data from your external datastores.

Fennel ships with data connectors to all [common datastores](/api-reference/sources) so that you can 
'source' your Fennel datasets from your external datasets. Let's see an example:

### **Example**

```python
from fennel.sources import source, Postgres

postgres = Postgres(host=...<credentials>..)

@source(postgres.table('user'), cursor='update_time', every='1m', lateness='1d')
@meta(owner='xyz@example.com')
@dataset
class UserLocation:
    uid: int
    city: str
    country: str
    update_time: datetime    
```

In this example, line 3 creates an object that knows how to connect with your 
Postgres database. Line 6-12 describe a dataset that needs to be sourced from 
the Postgres. And line 5 declares that this dataset should be sourced from a 
table named `user` within the Postgres database. And that's it - once this 
is written, `UserLocation` dataset will start mirroring your postgres table 
`user`and will update as the underlying Postgres table updates.&#x20;

Most sources take a few additional parameters as described below:

### Every 
The frequency with which Fennel checks the external data source for new data. 
Needed for all sources except Kafka and Webhooks which are ingested continuously.

### Cursor
Fennel uses a cursor to do incremental ingestion of data. It does so 
by remembering the last value of the cursor column (in this case `update_time`) 
and issuing a query of the form `SELECT * FROM user WHERE update_time > {last_update_time}`.
Clearly, this works only when the cursor field is monotonically increasing with 
row updates - which Fennel expects you to ensure. It is also advised to have 
an index of the cursor column so that this query is efficient. All data sources 
except Kafka, Webhooks & S3 require a cursor.

### Lateness
Fennel, like many other streaming systems, is designed to robustly handle out
of order data. If there are no bounds on how out of order data can get, the state
can blow up. Unlike some other systems, Fennel keeps this state on disk which 
eliminates OOM issues. But even then, it's desirable to garbaget collect this state
when all data before a timestamp has been seen. 

This is ususally handled by a technique called [Watermarking](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/) 
where max out of order delay is specified. This max out of order delay of a source 
is called `lateness` in Fennel, and once specified at source level, is respected 
automatically by each downstream pipeline. In this example, by setting `lateness` 
as `1d`, we are telling Fennel that once it sees a data with timestamp `t`, it 
will never see data with timestamp older than `t-1 day` and if it does see older
data, it's free to discard it.


## Schema Matching

Once Fennel obtains data from a source (usually as json string), the data needs to 
be parsed to extract and validate all the schema fields. Fennel expects the names 
of the fields in the dataset to match the schema of ingested json string. In this 
example, it is expected that the `user` table in Postgres will have at leat four 
columns -`uid`, `city`, `country`, and `update_time` with appropriate types. Note 
that the postgres table could have many more columns too - they are simply ignored.
If ingested data doesn't match with the schema of the Fennel dataset, the data 
is discarded and not admitted to the dataset. Fennel maintains logs of how often
it happens and it's possible to set alerts on that.

Here is how various types are matched from the sourced data:

* `int`, `float`, `str`, `bool` respectively match with any integer types, float
 types, string types and boolean types. For instance, Fennel's `int` type 
 matches with INT8 or UINT32 from Postgres.
* `List[T]` matches a list of data of type T.
* `Dict[T]` matches any dictionary from strings to values of type T.
* `Option[T]` matches if either the value is `null` or if its non-null value 
  matches type T. Note that `null` is an invalid value for any non-Option types.
* `datetime` matching is a bit more flexible to support multiple common
  data formats to avoid bugs due to incompatible formats. Fennel is able to
  safely parse datetime from all the following formats.

### Datetime Formats

* Integers that describe timestamp as interval from Unix epoch e.g. `1682099757`
 Fennel is smart enough to automatically deduce if an integer is describing 
 timestamp as seconds, miliseconds, microseconds or nanoseconds
* Strings that are decimal representation of an interval from Unix epoch e.g.`"1682099757"`
* Strings describing timestamp in [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) 
  format e.g. `'2002-10-02T10:00:00-05:00'` or `'2002-10-02T15:00:00Z'` or `'2002-10-02T15:00:00.05Z'`
* Strings describing timestamp in [RFC2822](https://www.ietf.org/rfc/rfc2822.txt) 
 format e.g. `'Sun, 23 Jan 2000 01:23:45 JST'`

### Safety of Credentials

In the above example, the credentials are defined in the code itself, which 
usually is not a good practice from a security point of view. Instead, Fennel 
recommends two ways of using Sources securely:

1. Using environment variables on your local machines
2. Defining credentials in Fennel's web console and referring to sources by 
   their names in the Python definitions.

In either approach, once the credentials reach the Fennel servers, they are 
securely stored in a Secret Manager and/or encrypted disks.

### Load Impact of Sources

Fennel sources have negligible load impact on the external data sources. For instance,
in the above example, as long as indices are put on the cursor field, Fennel will 
make a single SELECT query on Postgres every minute. And once data reaches Fennel 
datasets, all subsequent operations are done using the copy of the data stored on 
Fennel servers, not the underlying data sources. This ensures that external data
sources never need to be over-provisioned (or changed in any way) just for Fennel
to be able to read the data.


### Change Data Capture (CDC)

Fennel can also do CDC ingestion for Postgres and MySQL. However, that requires
setting some permissions on your Postgres/MySQL instances. Please talk to Fennel
team if you want this enabled.
