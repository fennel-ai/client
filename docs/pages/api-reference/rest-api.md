---
title: REST API
order: 1
status: 'published'
---

# REST API

All DDL operations (i.e. definitions of datasets/featuresets) can only be done via Python client. However, other operations that don't alter the definitions but just exchange data can also be done via a REST API in addition to the Python client.

### /api/v1/log

Used to log data to a dataset. It's post call with the following properties:

* `webhook`: the name of the webhook to which the data should be logged
* `endpoint`: the endpoint of the webhook to which the data should be logged
* `rows`: a list of rows (as json) that must be logged to the datasets

**Example**

<pre snippet="api-reference/rest-api#rest_log_api" />

### /api/v1/extract\_features


Used to extract a set of output features given known values of some input features. It's a POST call with the following parameters:

* `input_features`: list of fully qualified names of input features
* `output_features`: list of fully qualified names of desired output features
* `data`: list of json strings, each representing a particular row of the dataframe of input feature values
* `log`: boolean, true if the extracted features should also be logged to serve as future training data
* `workflow`: string describing the name of the workflow to which extract features should be logged (only relevant when `log` is set to true)
* `sampling_rate`: float between 0-1 describing the sampling to be done while logging the extracted features (only relevant when `log` is true)

**Example**

<pre snippet="api-reference/rest-api#rest_extract_features_api" />

