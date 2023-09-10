---
title: Client
order: 0
status: WIP
---

# Client

Fennel Client has the following methods on it:

### extract\_features

Given some input and output features, extracts the current value of all the output features given the values of the input features.

**Arguments:**

* `output_feature_list: List[Union[Feature, Featureset]]`: list of features (written as fully qualified name of a feature along with the featureset) that should be extracted. Can also take featureset objects as input, in which case all features in the featureset are extracted.
* `input_feature_list: List[Union[Feature, Featureset]]` : list of features/featuresets for which values are known
* `input_df: Dataframe`: a pandas dataframe object that contains the values of all features in the input feature list. Each row of the dataframe can be thought of as one entity for which features are desired.
* `log: bool` - boolean which indicates if the extracted features should also be logged (for log-and-wait approach to training data generation). Default is False
* `workflow: str` - the name of the workflow associated with the feature extraction. Only relevant when `log` is set to True
* `sampling_rate: float` - the rate at which feature data should be sampled before logging. Only relevant when log is set to True. The default value is 1.0

**Example:**

```python
client = Client(<URL>)

@featureset
class UserFeatures:
userid: int = feature(id=1)
... 6 more features
```

<pre snippet="api-reference/client#extract_features_api"></pre>


****

### **sync**

Synchronizes the local dataset and featureset definitions with the server. This method should be called after all the datasets and featuresets definitions have been defined using the client SDK.
This method will create the resources required for the datasets and featuresets on the server. It will also update the resources / throw errors if the schema of the datasets and featuresets have changed.

**Arguments:**

* `datasets: List[Dataset]` - a list of dataset definitions that need to be synced with the server
* `featuresets: List[Featureset]` - a list of featureset definitions that need to be synced with the server

**Example**

<pre snippet="api-reference/client#sync_api"></pre>

****

### **log**

While Fennel supports inbuilt connectors to external datasets, it's also possible to "manually" log data to Fennel datasets using `log`.

**Arguments:**

* `dataset_name: str` - the name of the dataset to which data needs to be logged
* `dataframe: Dataframe` - the data that needs to be logged, expressed as a Pandas dataframe.&#x20;
* `batch_size: int` - the size of batches in which dataframe is chunked before sending to the server. Useful when attempting to send very large batches. The default value is 1000.

This method throws an error if the schema of the dataframe (i.e. column names and types) are not compatible with the schema of the dataset.&#x20;

**Example**

<pre snippet="api-reference/client#log_api"></pre>

****

### **extract_historical_features**

For offline training of models, users often need to extract features for a large number of entities.
This method allows users to extract features for a large number of entities in a single call while ensuring
point-in-time correctness of the extracted features.

This api is an asynchronous api that returns a request id and the path to the output folder in S3 containing the extracted features.&#x20;
&#x20;

**Arguments:**


* `input_feature_list: List[Union[Feature, Featureset]]` - List of features or featuresets to use as input.
* `output_feature_list: List[Union[Feature, Featureset]]` - List of features or featuresets to compute.
* `timestamp_column: str` - The name of the column containing the timestamps.
* `format: str` - The format of the input data. Can be either "pandas", "csv", "json" or "parquet". Default is "pandas".
* `input_dataframe: Optional[pd.DataFrame]` - Dataframe containing the input features. Only relevant when format is "pandas".
* `input_bucket: Optional[str]` - The name of the S3 bucket containing the input data. Only relevant when format is "csv", "json" or "parquet".
* `input_prefix: Optional[str]` - The prefix of the S3 key containing the input data. Only relevant when format is "csv", "json" or "parquet".

**Returns:**

* `Dict[str, Any]` - A dictionary containing the following information:
  * request_id
  * output s3 bucket
  * output s3 path prefix
  * completion rate.
  * failure rate.
  * status

A completion rate of 1.0 indicates that all processing has been completed.
A completion rate of 1.0 and a failure rate of 0.0 indicates that all processing has been completed successfully.


**Example**

<pre snippet="api-reference/client#extract_historical_features_api"></pre>

****

### **extract_historical_features_status**

This method allows users to monitor the progress of the extract_historical_features asynchronous operation.
It accepts the request ID that was returned by the `extract_historical_features` method and returns the current status of that operation.

The response format of this function and the `extract_historical_features` function are identical.&#x20;

**Arguments:**


* `request_id: str` - The request ID returned by the `extract_historical_features` method. This ID uniquely identifies the feature extraction operation
* `cancel: bool` - If set to True, the feature extraction operation will be cancelled. Default is ofcourse, False.

**Returns:**

* `Dict[str, Any]` - A dictionary containing the following information:
  * request_id
  * output s3 bucket
  * output s3 path prefix
  * completion rate.
  * failure rate.
  * status

A completion rate of 1.0 indicates that all processing has been completed.
A completion rate of 1.0 and a failure rate of 0.0 indicates that all processing has been completed successfully.

**Example**

```
client.extract_historical_features_progress(request_id='bf5dfe5d-0040-4405-a224-b82c7a5bf085')
>>> {'request_id': 'bf5dfe5d-0040-4405-a224-b82c7a5bf085', 'output_bucket': <bucket_name>, 'output_prefix': <output_prefix>, 'completion_rate': 0.76, 'failure_rate': 0.0}
```

