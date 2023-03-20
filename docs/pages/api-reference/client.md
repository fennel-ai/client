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

* `output_feature_list`: list of features (written as fully qualified name of a feature along with the featureset) that should be extracted
* `input_feature_list` : list of features for which values are known
* `input_df`: a pandas dataframe object that contains the values of all features in the input feature list. Each row of the dataframe can be thought of as one entity for which features are desired.
* `log: bool` - boolean which indicates if the extracted features should also be logged (for log-and-wait approach to training data generation). Default is False
* `workflow: str` - the name of the workflow associated with the feature extraction. Only relevant when `log` is set to True
* `sampling_rate: float` - the rate at which feature data should be sampled before logging. Only relevant when log is set to True. The default value is 1.0

**Example:**

```python
client = Client(<URL>)

@featureset
class UserInfo:
    userid: int = feature(id=1)
    ...

feature_df = client.extract_features(
    output_feature_list=[
        UserInfo.userid,
        UserInfo.name,
        UserInfo.geoid,
        UserInfo.age,
        UserInfo.age_squared,
        UserInfo.age_cubed,
        UserInfo.is_name_common,
    ],
    input_feature_list=[UserInfo.userid],
    input_df=pd.DataFrame(
        {"UserInfo.userid": [18232, 18234]}
    ),
    log=True,
    workflow='home_page_ads',
)
assert feature_df.shape == (2, 7)
```

### **extract\_historical\_features**

****

### **sync**

****

### **log**

While Fennel supports inbuilt connectors to external datasets, it's also possible to "manually" log data to Fennel datasets using `log`.

**Arguments:**

* `dataset_name: str` - the name of the dataset to which data needs to be logged
* `dataframe: Dataframe` - the data that needs to be logged, expressed as a Pandas dataframe.&#x20;
* `batch_size: int` - the size of batches in which dataframe is chunked before sending to the server. Useful when attempting to send very large batches. The default value is 1000.

This method throws an error if the schema of the dataframe (i.e. column names and types) are not compatible with the schema of the dataset.&#x20;

**Example**

```python
@meta(owner='abc@xyz.ai')
@dataset
class User:
    id: int
    gender: str
    signup: datetime

# create a client somehow
# ...
client.sync(datasets=[User])
df = pd.Dataframe.from_dict({
    'id': [1, 2, 3],
    'gender': ['M', 'F', 'M'],
    'signup': [1674793720, 1674793720, 1674793720],
})
client.log('User', df)
```
