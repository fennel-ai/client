---
title: Client
order: 0
status: wip
---

# Client

Fennel Client has the following methods on it:

### extract\_features

Given some input and output features, extracts the current value of all the output features given the values of the input features.

**Arguments:**

* `output_feature_list`: list of features (written as fully qualified name of a feature along with the featureset) that should be extracted. Can also take featurset objects as input, in which case all features in the featureset are extracted.
* `input_feature_list` : list of features/featuresets for which values are known
* `input_df`: a pandas dataframe object that contains the values of all features in the input feature list. Each row of the dataframe can be thought of as one entity for which features are desired.
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

