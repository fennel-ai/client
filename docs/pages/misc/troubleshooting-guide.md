---
title: Troubleshooting Guide
order: 1
status: 'published'
---

# Troubleshooting Guide

### MySQL connection issues

Some users have reported that they could not connect to Amazon RDS MySQL or 
MariaDB. This can be diagnosed with the error message: `Cannot create a PoolableConnectionFactory`. 
To solve this issue please set `jdbc_params` to `enabledTLSProtocols=TLSv1.2`


### Pipeline schema issues

To debug schema issues you can print the schema of the intermediate datasets 
in your pipeline. You can do this by calling the `.schema()` method on 
the dataset.

```python
filtered_ds = activity.filter(lambda df: df["action_type"] == "report")
print(filtered_ds.schema())
```


### Null feature values during extraction

One common possibility is that the timestamps for the datasets are equal to or 
ahead of the current time. Since fennel ensures that everything is point in 
time correct, this can result in null values for the features.

Either way, it might be helpful to print the current data that is stored in the datasets 
that you are using to extract features.  You can do this by calling :
`client.get_dataset_df(dataset_name) ` against the mock client and printing 
the resultant dataframe.

Please note that this debug functionality is only available in the mock client. 
To debug issues in prod, you need to use the metadata APIs exposed by Fennel.
