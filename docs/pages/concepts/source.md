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

In this example, first an object is created that knows how to connect with your
Postgres database. Then we specify the table within this database (i.e. `user`) 
that we intend to source from. And finally, a decorator is added on top of the 
dataset which should be sourcing from this table.

And that's it - once this is written, `UserLocation` dataset will start 
mirroring your postgres table `user`and will update as the underlying Postgres 
table updates.

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

### CDC
Fennel natively supports change data capture (aka CDC) as well as other modes
of ingestion (e.g. `append`). You can configure the `cdc` parameter to specify
how should your data be interpreted and converted to valid change log data. More
often than not, you'd choose `append` to signify that all incoming rows should
be appended to the dataset such that there are no deletes/updates.

### Preproc
The `preproc` field in the source provides a way to ingest a column that 
doesn't exist. Instead, it is either given a default value or to base the value 
of that column on another column. 


### Since
The `since` param (of type `datetime`) in the source provides a way to ingest 
data from a specific time onwards from the source. When set, only the rows 
having the value of their timestamp field >= the `since` value are ingested. 

### Until
Analogous to `since`, `until`, when set restricts the ingestion to only those
rows where the value of the timestamp field is < the `until` value.

`since` and `until` are very useful when building datasets that are union of 
batch & realtime sources. For example, read from s3 `until` a timestamp, and 
from that same timestamp onwards, read from kafka using `since`.  

### Bounded
By default, Fennel assumes all data sources are unbounded and will keep getting 
new data. However, a source can be marked as finite by setting `bounded` to True.
In such cases, if Fennel is unable to obtain any new data from source despite 
trying for at least `idleness` period, Fennel assumes that the source has been
exhausted. This allows Fennel to free up any state that isn't needed any more, 
thereby saving costs and improving performance.

### Idleness
The `idleness` field, when non-None, signifies that a bounded source is 
expected to be marked closed after a specified duration of idleness. 

## Load Impact of Sources

Fennel sources have negligible load impact on the external data sources. 

For instance, in the above example, as long as indices are put on the cursor 
field, Fennel will make a single SELECT query on Postgres every minute. And 
once data reaches Fennel datasets, all subsequent operations are done using 
the copy of the data stored on Fennel servers, not the underlying data sources. 
This ensures that external data sources don't need to be over-provisioned 
(or changed in any way) just for Fennel to be able to read the data.
