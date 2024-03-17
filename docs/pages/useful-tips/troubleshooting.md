---
title: 'Troubleshooting'
order: 0
status: 'published'
---

# Troubleshooting
## Local Development
Fennel ships with a standalone mock client for easier & faster local mode
development. Here are a few tips to debug issues encountered during local 
development:

### Debugging Dataset Schema Issues
To debug dataset schema issues while writing pipelines, you can call 
the `.schema()` method on the dataset which returns a regular dictionary from 
the field name to the type and then just printing it or inspecting it using a 
debugger.

<pre snippet="useful-tips/debugging#basic" status="success"
   message="Calling .schema() on datasets in pipelines">
</pre>


### Printing Full Datasets
You can also print the full contents of Fennel datasets at any time by calling
`client.get_dataset_df(dataset_name)` using the mock client and printing 
the resulting dataframe.

===
<pre snippet="useful-tips/debugging#print_dataset"
  status="success" message="Obtaining full dataset from mock client"
></pre>
<pre snippet="useful-tips/debugging#print_dataset"
  status="success" message="Obtaining full dataset from mock client"
></pre>
===

:::warning
This debug functionality is only available in the mock client. You can instead 
use inspect APIs to debug data flow issues in prod.
:::

### Explicitly Setting Pandas Types
Fennel backend uses a strong typing system built in Rust. However, the mock client
keeps the data in Pandas format which is notorious for doing arbitrary type 
conversions (at least before Pandas 2.0). For instance, an integer column with 
missing values is automatically typecast to be a float column by Pandas. 

Sometimes this shows up as issues that will be present only with the mock client
but not with the real server. And sometimes it shows up as real issues with real
server getting masked during local development with the mock client.

Hence it's recommended to explicitly set data types when working with pandas and
mock client by using the [astype](https://pandas.pydata.org/docs/reference/api/pandas.Series.astype.html)
method.

<pre snippet="useful-tips/debugging#astype"
    status="success" message="Explicit type cast in pandas using astype">
</pre>

## Data Integration
### MySQL connection issues
Some users have reported that they could not connect to Amazon RDS MySQL or 
MariaDB. This can be diagnosed with the error message: `Cannot create a PoolableConnectionFactory`. 
To solve this issue please set `jdbc_params` to `enabledTLSProtocols=TLSv1.2`