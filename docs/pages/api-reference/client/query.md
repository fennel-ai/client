---
title: Query
order: 0
status: published
---
### Query

<Divider>
<LeftSection>
Method to query the latest value of features (typically for online inference).

#### Parameters

<Expandable title="inputs" type="List[Union[Feature, str]]">
List of features to be used as inputs to query. Features should be provided 
either as Feature objects or strings representing fully qualified feature names.
</Expandable>

<Expandable title="outputs" type="List[Union[Featureset, Feature, str]]">
List of features that need to be queries. Features should be provided 
either as Feature objects, or Featureset objects (in which case all features under
that featureset are queries) or strings representing fully qualified feature names.
</Expandable>

<Expandable title="input_dataframe" type="pd.Dataframe">
A pandas dataframe object that contains the values of all features in the inputs
list. Each row of the dataframe can be thought of as one entity for which 
features need to be queried.
</Expandable>

<Expandable title="log" type="bool" defaultVal="False">
Boolean which indicates if the queried features should also be logged (for 
log-and-wait approach to training data generation).
</Expandable>

<Expandable title="workflow" type="str" defaultVal="'default'">
The name of the workflow associated with the feature query. Only relevant
when `log` is set to True, in which case, features associated with the same workflow
are collected together. Useful if you want to separate logged features between, say,
login fraud and transaction fraud.
</Expandable>

<Expandable title="sampling_rate" type="float" defaultVal="1.0">
The rate at which feature data should be sampled before logging. Only relevant
when `log` is set to True.
</Expandable>


#### Returns
<Expandable title="type" type="Union[pd.Dataframe, pd.Series]">
Returns the queried features as dataframe with one column for each feature 
in `outputs`. If a single output feature is requested, features are returned
as a single pd.Series. Note that input features aren't returned back unless
they are also present in the `outputs`
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
Fennel raises a run-time error if any extractor returns a value of the feature 
that doesn't match its stated type.
</Expandable>

<Expandable title="Authorization error">
Fennel checks that the passed token has sufficient permissions for each of the
features/extractors - including any intermediate ones that need to be computed
in order to resolve the path from the input features to the output features.
</Expandable>

</LeftSection>
<RightSection>
<pre snippet="api-reference/client/query#basic" status="success"
    message="Querying two features"></pre>
</RightSection>
</Divider>