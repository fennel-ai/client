---
title: Deltalake
order: 0
status: published
---
### Deltalake
Data connector to read data from tables in [deltalake](https://delta.io/) living 
in S3. 

Deltalake connector is implemented via s3 connector - just the format parameter 
needs to be setup as 'delta' and source CDC must be passed as 'native'.

To enable Change Data Feed (CDF) on your table, follow the instructions in this
[documentation](https://docs.delta.io/latest/delta-change-data-feed.html#enable-change-data-feed)

:::warning
Fennel doesn't support reading delta tables from HDFS or any other non-S3 storage.
:::

:::info
Fennel supports only native CDC with data in Deltalake format. If your data is of the
append type, enable CDF on the table and use native CDC mode.
:::

<pre snippet="api-reference/sources/s3#s3_delta"
    status="success" message="Sourcing delta tables into Fennel datasets" 
 >
</pre>

