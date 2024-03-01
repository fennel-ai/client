---
title: 'Source'
order: 0
status: 'published'
---

# Source

Data gets into Fennel datasets via Sources - in fact, sources are the only 
mechanism for data to reach a Fennel dataset.

Fennel ships with data connectors to all [common datastores](/api-reference/sources) 
so that you can 'source' your Fennel datasets from your external datasets. In 
addition to the pull based sources that read from external data sources, Fennel
also ships with a push based source called `Webhook` for you to manually push
data into Fennel datasets. 

Let's see an example with postgres connector:

<pre snippet="concepts/introduction#source"></pre>

In this example, line 3 creates an object that knows how to connect with your
Postgres database. Line 13-19 describe a dataset that needs to be sourced from
the Postgres. And line 13 declares that this dataset should be sourced from a
table named `user` within the Postgres database. And that's it - once this
is written, `UserLocation` dataset will start mirroring your postgres table
`user`and will update as the underlying Postgres table updates.


## Schema Matching

Fennel has a strong typing system and all ingested data is evaluated against
the dataset schema. See [Fennel types](/api-reference/data-types/core-types) to 
see the full list of types supported by Fennel. 

### Schema As Subset of Source
All the fields defined in Fennel dataset must be present in your source dataset
though it's okay if your external data source has additional fields - they are 
imply ignored.  In this example, it is expected that the `user` table in 
Postgres will have at least four columns -`uid`, `city`, `country`, and 
`update_time` with appropriate types. 

Not only those fields must be present, they must be of the right types (or be 
convertible into right types via some parsing/casting). If ingested data 
doesn't match with the schema of the Fennel dataset, the data is discarded and 
not admitted to the dataset. Fennel maintains logs of how often it happens and 
it's possible to set alerts on that.



### Type Matching/Casting Rules

Here is how various types are matched from the sourced data (note that type 
matching/casting only happens during sourcing - once data has been parsed in 
the given type, from then onwards, there is no type casting/coercion anywhere 
else in Fennel)

* `int`, `float`, `str`, `bool` respectively match with any integer types, float
 types, string types and boolean types. For instance, Fennel's `int` type
 matches with INT8 or UINT32 from Postgres.
* `List[T]` matches a list of data of type `T`.
* `Dict[T]` matches any dictionary from strings to values of type `T`.
* `Optional[T]` matches if either the value is `null` or if its non-null value
  matches type `T`. Note that `null` is an invalid value for any non-Optional types.
* `struct` is similar to dataclass in Python or struct in compiled languages
  and matches JSON data with appropriate types recursively.
* `datetime` matching is a bit more flexible to support multiple common
  data formats to avoid bugs due to incompatible formats. Fennel is able to
  safely parse datetime from all the following formats.

### Datetime Formats

* Integers that describe timestamp as interval from Unix epoch e.g. `1682099757`
 Fennel is smart enough to automatically deduce if an integer is describing
 timestamp as seconds, milliseconds, microseconds or nanoseconds
* Strings that are decimal representation of an interval from Unix epoch e.g.`"1682099757"`
* Strings describing timestamp in [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt)
  format e.g. `'2002-10-02T10:00:00-05:00'` or `'2002-10-02T15:00:00Z'` or `'2002-10-02T15:00:00.05Z'`
* Strings describing timestamp in [RFC2822](https://www.ietf.org/rfc/rfc2822.txt)
 format e.g. `'Sun, 23 Jan 2000 01:23:45 JST'`
* When data is being read from arrow representations (say parquet files), `date32`
  and `date64` types are also converted to datetime (with time being UTC midnight)


## Configuring Sources

Many sources (but not all) take some of the following arguments:

### Every
The frequency with which you want Fennel to check the external data source for 
new data. Fennel is built with continuous ingestion in mind so you could set it 
to small values and still be fine.

### Disorder
Fennel, like many other streaming systems, is designed to robustly handle out
of order data. If there are no bounds on how out of order data can get, internal 
state maintained by Fennel can become very large. Unlike many other streaming 
systems (like Flink or Spark streaming), Fennel keeps this state on disk, not RAM, 
which eliminates OOM issues. But even then, it's desirable to garbage collect 
this state when all data before a timestamp has been seen.

This is usually handled by a technique called [Watermarking](https://www.oreilly.com/radar/the-world-beyond-batch-streaming-102/)
where max out of order delay is specified. This max out of order delay of a source
is called `disorder` in Fennel, and once specified at source level, is respected
automatically by each downstream pipeline. 

### Cursor
For some (but not all) sources, Fennel uses a cursor to do incremental 
ingestion of data. It does so by remembering the last value of the cursor 
column (in this case `update_time`) and issuing a query of the 
form `SELECT * FROM user WHERE update_time > {last_seen_update_time - disorder}`.

Clearly, this works only when the cursor field is monotonically increasing with
row updates - which Fennel expects you to ensure. It is also advised to have
an index of the cursor column so that this query is efficient. 

Many data sources don't need an explicit cursor and instead use other implicit 
mechanisms to track and save ingestion progress.


### Preproc
The `preproc` field in the source provides a way to ingest a column that 
doesn't exist. Instead, it is either given a default value or to base the value 
of that column on another column. 

### CDC
Fennel natively supports change data capture (aka CDC) as well as other modes
of ingestion (e.g. `append`). You can configure the `cdc` parameter to specify
how should your data be interpreted and converted to valid change log data.

### Since
The `since` field in the source provides a way to ingest data from a specific time onwards from the source. 

Typically, the data sources could contain data from a long time ago, but based on the use case, we may only want to 
ingest data from a specific time onwards. The `since` field allows us to do that. 

The `since` field is a `datetime` instance.

### Until
The `until` field provides a way to limit ingestion of a particular source to end at a particular timestamp.

When deriving datasets that are unions of batch and realtime sources, `until` and `since` can be used to control when
to switch between sources. For example, read from s3 `until` a timestamp, and from that timestamp onwards, read from kafka
using `since`.  


## Load Impact of Sources

Fennel sources have negligible load impact on the external data sources. 

For instance, in the above example, as long as indices are put on the cursor 
field, Fennel will make a single SELECT query on Postgres every minute. And 
once data reaches Fennel datasets, all subsequent operations are done using 
the copy of the data stored on Fennel servers, not the underlying data sources. 
This ensures that external data sources don't need to be over-provisioned 
(or changed in any way) just for Fennel to be able to read the data.
