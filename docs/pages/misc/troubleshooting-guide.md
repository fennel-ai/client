---
title: Troubleshooting Guide
order: 1
status: 'published'
---

# Troubleshooting Guide

<details style={{fontSize: '1.25rem'}}>

<summary style={{fontSize: '1.5rem'}}>I am having issues in connecting my MySQL DB to Fennel</summary>

Some users have reported that they could not connect to Amazon RDS MySQL or MariaDB. This can be diagnosed with the error message: `Cannot create a PoolableConnectionFactory`. To solve this issue please set **`jdbc_params` ** to **** `enabledTLSProtocols=TLSv1.2`&#x20;
</details>

<br/> 

<details style={{fontSize: '1.25rem'}}>

<summary style={{fontSize: '1.5rem'}}>I am running into schema issues while creating my pipelines</summary>

To debug this issue you can print the schema of the intermediate datasets in your pipeline. You can do this by adding calling the `.schema()` method on the dataset.&#x20;

```python
filtered_ds = activity.filter(lambda df: df["action_type"] == "report")
print(filtered_ds.schema())
```

</details>

<br/> 

<details style={{fontSize: '1.25rem'}}>

<summary style={{fontSize: '1.5rem'}}>I am getting null results for my features during extract_features call </summary>

It might be helpful to print the current data that is stored in the datasets that you are using to extract features.
You can do this by calling :
`client.get_dataset_df(dataset_name) ` and printing the resultant dataframe.&#x20; 

Please note that this debug functionality is only available in the mock client. To debug issues in prod, you need to use
the metadata api's exposed by Fennel.&#x20;

Another possibility is that the timestamps for the datasets are equal to or ahead of the current time. Since fennel
ensures that everything is point in time correct, this can result in null values for the features.&#x20;

</details>