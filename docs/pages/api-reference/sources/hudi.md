---
title: Hudi
order: 0
status: published
---
### Hudi

<Divider>
<LeftSection>
Data connector to read data from [Apache Hudi](https://hudi.apache.org/) tables in S3. 

Hudi connector is implemented via s3 connector - just the format parameter needs to 
be setup as 'hudi'

:::warning
Fennel doesn't support reading hudi tables from HDFS or any other non-S3 storage.
:::
</LeftSection>

<RightSection>
<pre snippet="api-reference/sources/s3#s3_hudi"
    status="success" message="Sourcing hudi tables into Fennel datasets" 
    highlight="10">
</pre>
</RightSection>
</Divider>
