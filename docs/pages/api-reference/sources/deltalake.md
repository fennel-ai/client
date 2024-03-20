---
title: Deltalake
order: 0
status: published
---
### Deltalake
Data connector to read data from tables in [deltalake](https://delta.io/) living 
in S3. 

Deltalake connector is implemented via s3 connector - just the format parameter 
needs to be setup as 'delta'.

:::warning
Fennel doesn't support reading delta tables from HDFS or any other non-S3 storage.
:::

<pre snippet="api-reference/sources/s3#s3_delta"
    status="success" message="Sourcing delta tables into Fennel datasets" 
    highlight="10">
</pre>
