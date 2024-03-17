---
title: Extract Historical
order: 0
status: published
---
### Extract Historical
Method to query the historical values of features. Typically used for training 
data generation or batch inference.

#### Parameters

<Expandable title="inputs" type="List[Union[Feature, str]]">
List of features to be used as inputs to extract. Features should be provided 
either as Feature objects or strings representing fully qualified feature names.
</Expandable>

<Expandable title="outputs" type="List[Union[Featureset, Feature, str]]">
List of features that need to be extracted. Features should be provided 
either as Feature objects, or Featureset objects (in which case all features under
that featureset are extracted) or strings representing fully qualified feature names.
</Expandable>

<Expandable title="format" type='"pandas" | "csv" | "json" | "parquet"' defaultVal="pandas">
The format of the input data
</Expandable>

<Expandable title="input_dataframe" type="pd.Dataframe">
A pandas dataframe object that contains the values of all features in the inputs
list. Each row of the dataframe can be thought of as one entity for which 
features need to be extracted.

Only relevant when `format` is "pandas".
</Expandable>

<Expandable title="input_s3" type="Optional[sources.S3]">
Sending large volumes of the input data over the wire is often infeasible.
In such cases, input data can be written to S3 and the location of the file is
sent as `input_s3` via `S3.bucket()` function of [S3](/api-reference/sources/s3) 
connector. 

This parameter makes sense only when `format` isn't "pandas".

When using this option, please ensure that Fennel's data connector 
IAM role has the ability to execute read & list operations on this bucket - 
talk to Fennel support if you need help.

</Expandable>

<Expandable title="timestamp_column" type="str">
The name of the column containing the timestamps as of which the feature values
must be computed.
</Expandable>

<Expandable title="output_s3" type="Optional[sources.S3]">
Specifies the location & other details about the s3 path where the values of
all the output features should be written. Similar to `input_s3`, this is 
provided via `S3.bucket()` function of [S3](/api-reference/sources/s3) connector.

If this isn't provided, Fennel writes the results of all requests to a fixed
default bucket - you can see its details from the return value of `extract_historical`
or via Fennel Console.

When using this option, please ensure that Fennel's data connector 
IAM role has write permissions on this bucket - talk to Fennel support if you 
need help.
</Expandable>

<Expandable title="feature_to_column_map" type="Optional[Dict[Feature, str]]" defaultVal="None">
When reading input data from s3, sometimes the column names in s3 don't match
one-to-one with the names of the input features. In such cases, a dictionary
mapping features to column names can be provided. 

This should be setup only when `input_s3` is provided.
</Expandable>

===
<pre name="Request" snippet="api-reference/client/extract#extract_historical_api"
  status="success" message="Example with `format='pandas'` & default s3 output"
></pre>
<pre name="Response" snippet="api-reference/client/extract#extract_historical_response"
  status="success" message="Response of extract historical"
></pre>
===

<pre snippet="api-reference/client/extract#extract_historical_s3"
  status="success" message="Example specifying input and output s3 buckets"
></pre>

#### Returns
<Expandable title="type" type="Dict[str, Any]">
Immediately returns a dictionary containing the following information:
* request_id - a random uuid assigned to this request. Fennel can be polled
  about the status of this request using the `request_id`
* output s3 bucket - the s3 bucket where results will be written
* output s3 path prefix - the prefix of the output s3 bucket
* completion rate - progress of the request as a fraction between 0 and 1
* failure rate - fraction of the input rows (between 0-1) where an error was 
  encountered and output features couldn't be computed
* status - the overall status of this request

A completion rate of 1.0 indicates that all processing has been completed.
A completion rate of 1.0 and failure rate of 0 means that all processing has 
been completed successfully.
</Expandable>

#### Errors
<Expandable title="Unknown features">
Fennel will throw an error (equivalent to 404) if any of the input or output
features doesn't exist.
</Expandable>

<Expandable title="Resolution error">
An error is raised when there is absolutely no way to go from the input features
to the output features via any sequence of intermediate extractors.
</Expandable>

<Expandable title="Schema mismatch errors">
Fennel raises a run-time error and may register failure on a subset of rows if 
any extractor returns a value of the feature that doesn't match its stated type.
</Expandable>

<Expandable title="Authorization error">
Fennel checks that the passed token has sufficient permissions for each of the
features/extractors - including any intermediate ones that need to be computed
in order to resolve the path from the input features to the output features.
</Expandable>

