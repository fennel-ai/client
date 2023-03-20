---
title: 'Sources'
order: 1
status: 'published'
---

# Sources

One of the central challenges of feature engineering is that the data is spread over data systems of different modalities - everything from Postgres, Kafka, S3, to even analytical systems like Redshift, Bigquery, Snowflake etc. Each of these have different perf characteristics, different states of data freshness, and their own custom tooling. All of this makes it hard to write features across all of these. &#x20;

Fennel datasets allow you to bring relevant data from all of these and more sources into the same "plane of abstraction" to be able to write various features. It does so by providing pre-built sources that know how to read data from each of these systems. Let's look at an example to see how it works:

### **Example**

```python
from fennel.sources import source, Postgres

postgres = Postgres(host=...<credentials>..)

@source(postgres.table('user'), cursor='update_time', every='1m')
@meta(owner='xyz@example.com')
@dataset
class UserLocation:
    uid: int
    city: str
    country: str
    update_time: datetime    
```

In this example, line 3 creates an object that describes 'connection' to your Postgres database. Line 6-12 describe a dataset that needs to be sourced from the Postgres. And line 5 declares that this dataset should be sourced from a table named `user` within the Postgres database. And that's it - once this is written, `UserLocation` dataset will start mirroring your postgres table `user`and will update as the underlying Postgres table updates.&#x20;

Each source has its own configuration options. In case of Postgres, there are two more options besides the table name as shown in this example- `cursor`  and `every`. Let's look at both one by one:

**Cursor**

Once this dataset is declared, Fennel will periodically ask Postgres if there have been any new rows since the last time it checked. The way it does this is by remembering the last value of cursor column (in this case `update_time`) and issuing a query of the form `SELECT * FROM user WHERE update_time > last_update_time.` As you might have noticed, for this to work the cursor field needs to be monotonically increasing with row updates. As long as that is the case, Fennel will source every new row into the dataset. Several data sources have a cursor parameter except for some (e.g. Kafka/S3) which have a cursor like functionality built into the data source itself (e.g. offsets in Kafka act as natural cursor)

**Every**

`every` denotes the period after which Fennel asks Postgres for new rows. Almost all sources expose this parameter using which you can control the frequency at which data is synced.

### Schema Matching

It is expected that the names of fields in the dataset match the schema of the external data source. In this example, for instance, it is expected that the `user` table in Postgres will have at leat four columns -`uid`, `city`, `country`, and `update_time` each with appropriate types. The postgres table could totally have many more columns - they are simply ignored.&#x20;

Here is how various types are matched from the sourced data:

* `int`, `float`, `str`, `bool` match with any integer types, float types, string types and boolean types respectively. For instance `int` type checks with INT8 or UINT32 from Postgres.&#x20;
* `List[T]` matches a list of data of type T and `Dict[T]` matches any data that is dictionary from strings to columns of type T.&#x20;
* `Option[T]` on dataset matches with column if either the cell is `null` or if its non-null value matches type T.
* `datetime` matching is a bit more flexible to support common date formats and is defined below.

Mismatch of sourced data schema and the schema of Fennel datasets is a runtime error though we are working towards moving some of that error checking to "compile" time when the datasets are first declared.&#x20;

### Datetime Parsing

If a dataset field is declared to be of type `datetime` (as is `update_time` in the example above), it can be safely parsed from any of the following data:

* Integers that describe timestamp as seconds/milliseconds/microseconds/nanoseconds from Unix epoch. Fennel is smart enough to automatically deduce if an integer is describing timestamp as seconds, miliseconds, microseconds or nanoseconds.
* Strings that describe timestamp in [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt) format e.g. `'2002-10-02T10:00:00-05:00'` or `'2002-10-02T15:00:00Z'` or `'2002-10-02T15:00:00.05Z'`
* Strings that describe timestamp in [RFC2822](https://www.ietf.org/rfc/rfc2822.txt) format e.g. `'Sun, 23 Jan 2000 01:23:45 JST'`&#x20;

### Safety of Credentials

In the above example, the credentials are defined in the code itself, which usually is not a good practice from a security point of view. Instead, Fennel recommends two ways of using Sources securely:

1. Using environment variables
2. Defining credentials in Fennel's web console

**Using Environment Variables**

Here is an example of how that may look like (this assumes you have injected appropriate secrets as env variables on your servers)

<pre snippet="datasets/sources#postgres_source" />

**Defining credentials in Fennel's Web Console**

Fennel ships with a web console that can be used for defining source credentials along with many other diagnostic and monitoring purposes. Please ask your contact at Fennel to give you access to the console if you don't yet have it.&#x20;

Note that in either method, once the credentials reach the Fennel servers, they are securely stored in a cloud native Secret Manager.&#x20;

### Load Impact of Sources

All sources are built in a way that they have minimal load impact on the external data sources. For instance in the above example, as long as indices are put on the cursor field, Fennel will make a single SELECT query on Postgres every minute. Besides that, there will be no other communication with it. In other words, once data is sourced into Fennel datasets, all subsequent operations are done using the copy of the data stored on Fennel servers, not the underlying data sources.

### Data Copies

Feature engineering systems often generate heavy read/write traffic, which often results in teams having to proactively manage capacity and/or manage caching of their data sources in lockstep with feature engineering needs. This creates operational burden and is error-prone.&#x20;

Fennel's design goal is to be extremely easy to install and use. To that end, Fennel has made an intentional choice to make a copy of the data from external data sources into storage inside Fennel. The benefit of this is that Fennel has negligible load impact on your data sources and hence they don't need to be scaled up / or kept in sync with load generated by feature engineering workloads. The obvious downside of this is that there are two copies of data in the system which incurs some incremental cost. But it is not as bad as it sounds -- disk storage is by far the cheapest hardware resource so it ends up being relatively cheap compared to other system components like CPU or RAM.

If this is a major deal breaker for you for some reason, please get in touch with the Fennel team - we'd love to understand your situation and try to find a workaround.&#x20;

### Change Data Capture (CDC)

Fennel currently does not support CDC though adding CDC for at least Postgres and MySQL is on the roadmap. If you have use cases where you urgently need CDC to work, please get in touch with the Fennel team and we'd be happy to prioritize this on our roadmap.

### Full List of Sources

Look at [Sources](/api-reference/sources) for a full list of sources supported by Fennel or let us know (over email or slack) if you want to use a data source that is not yet supported.&#x20;
