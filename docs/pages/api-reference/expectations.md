---
title: Expectations
order: 7
status: 'published'
---

# Expectations

Fennel's type system lets one maintain data integrity by rejecting data that does not conform to its
types. However, there are cases where one may want to accept data that does not conform to the
types, but still monitor how often the data does not conform to the types. For this, Fennel
provides the ability to specify expectations on the data.

Fennel internally relies on [Great Expectations](https://greatexpectations.io/) to help
users easily specify data expectations. Fennel's expectations are a subset of `Great Expectations`
expectations and are documented below, but the api to specify expectations is the same. 

## Expectation Types

<br/>

### Single Column Expectations

The following expectations operate on a single column at a time.

<ol>
<li>

<details>
   <summary><b>[expect_column_values_to_not_be_null](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null)</b></summary>

   Expect the column values to not be null. To be counted as an exception, values must be explicitly null or missing, such as np.nan.
   Empty strings don't count as null unless they have been coerced to a null type. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_be_null](https://greatexpectations.io/expectations/expect_column_values_to_be_null)</b></summary>

   Expect the column values to be null. It is the inverse of `expect_column_values_to_not_be_null`.  <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_be_of_type](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type)</b></summary>

   Expect a column to contain values of a specified data type.  <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `type_ (str)` – The expected data type of the column values.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_be_in_type_list](https://greatexpectations.io/expectations/expect_column_values_to_be_in_type_list)</b></summary>

   Expect a column to contain values of one of several specified data types. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `type_list (list)` – A list of expected data types of the column values.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_be_in_set)</b></summary>

   Expect each column value to be in a given set.<br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `value_set (list)` – A set of objects used for comparison.
   </details>
</li>

<li><details>
   <summary><b>[expect_column_values_to_not_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_not_be_in_set)</b></summary>

   Expect each column value to not be in a given set. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `value_set (list)` – A set of objects used for comparison.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_be_between](https://greatexpectations.io/expectations/expect_column_values_to_be_between)</b></summary>

   Expect column values to be between a minimum value and a maximum value. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `min_value (int)` – The minimum value for a column entry.
   - `max_value (int)` – The maximum value for a column entry.
   - `strict_min (bool)` – If True, the column values must be strictly larger than min_value.
   - `strict_max (bool)` – If True, the column values must be strictly smaller than max_value.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_value_lengths_to_be_between](https://greatexpectations.io/expectations/expect_column_value_lengths_to_be_between)</b></summary>

   Expect the lengths of column values to be between a minimum value and a maximum value. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `min_value (int)` – The minimum value for a column entry length.
   - `max_value (int)` – The maximum value for a column entry length.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_value_lengths_to_equal](https://greatexpectations.io/expectations/expect_column_value_lengths_to_equal)</b></summary>

   Expect the lengths of column values to equal a given value. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `value (int)` – The expected length of column values.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex)</b></summary>

   Expect column entries to be strings that match a given regular expression. . <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `value (int)` – The expected length of column values.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_not_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex)</b></summary>

   Expect the lengths of column values to equal a given value. <br/><br/>
   *Parameters*:
   - `column (str)` – The column name.
   - `value (int)` – The expected length of column values.
   </details>
</li>


<li> <details>
   <summary><b>[expect_column_values_to_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list)</b></summary>

   Expect column entries to be strings that match at least one of a list of regular expressions.<br/><br/>

   *Parameters*:
   - `column (str)` – The column name.
   - `regex_list (list)` – The list of regular expressions that each column entry should match at least one of.
   </details>
</li>

<li>
<details>
   <summary><b>[expect_column_values_to_not_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)</b></summary>
   
   Expect column entries to be strings that do not match any of a list of regular expressions.<br/><br/>
   
   *Parameters*:
   - `column (str)` – The column name.
   - `regex_list (list)` – The list of regular expressions that each column entry should not match any of.
   </details>
</li>

<li> <details>
   <summary><b>[expect_column_values_to_match_strftime_format](https://greatexpectations.io/expectations/expect_column_values_to_match_strftime_format)</b></summary>

   Expect column entries to be strings representing a date or time with a given format.<br/><br/>

   *Parameters*:
   - `column (str)` – The column name.
   - `strftime_format (str)` – The strftime format that each column entry should match.
   </details>
</li>
<li>
<details>
   <summary><b>[expect_column_values_to_be_dateutil_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_dateutil_parseable)</b></summary>

   Expect column entries to be parseable using dateutil.<br/><br/>

   *Parameters*:
   - `column (str)` – The column name.
   </details>
</li>

<li>
<details>
   <summary><b>[expect_column_values_to_be_json_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_json_parseable)</b></summary>

   Expect column entries to be parseable as JSON.<br/><br/>
   
   *Parameters*:
   - `column (str)` – The column name.
   </details>
</li>
<li>
<details>
   <summary><b>[expect_column_values_to_match_json_schema](https://greatexpectations.io/expectations/expect_column_values_to_match_json_schema)</b></summary>

   Expect column entries to match a given JSON schema.<br/><br/>
   
   *Parameters*:
   - `column (str)` – The column name.
   - `json_schema (dict)` – The JSON schema that each column entry should match.
   </details>
</li>
</ol>

### Multi Column Expectations

The following expectations require two or more columns. 

<ol>
<li>
<details>
   <summary><b>[expect_column_pair_values_to_be_equal](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_equal)</b></summary>

   Expect the values in a column to be the exact same as the values in another column.<br/><br/>

   *Parameters*:
   - `column_A (str)` – The first column name.
   - `column_B (str)` – The second column name.
   - `ignore_row_if (str)` – Control how null values are handled. See ignore_row_if for details.
   </details>
</li>

<li>
<details>
   <summary><b>[expect_column_pair_values_A_to_be_greater_than_B](https://greatexpectations.io/expectations/expect_column_pair_values_A_to_be_greater_than_B)</b></summary>

   Expect the values in column A to be greater than the values in column B.<br/><br/>
   *Parameters*:

   - `column_A (str)` – The first column name.
   - `column_B (str)` – The second column name.
   - `or_equal (bool)` – If True, then values can be equal, not strictly greater than.
   </details>
</li>
<li>
<details>
   <summary><b>[expect_column_pair_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_in_set)</b></summary>

   Expect the values in a column to belong to a given set.<br/><br/>

   *Parameters*:
   - `column_A (str)` – The first column name.
   - `column_B (str)` – The second column name.
   - `value_pairs_set (set)` – A set of tuples describing acceptable pairs of values. Each tuple should have two elements, the first from column A and the second from column B.
   </details>
</li>
<li>
<details>
   <summary><b>[expect_multicolumn_sum_to_equal](https://greatexpectations.io/expectations/expect_multicolumn_sum_to_equal)</b></summary>

   Expect the sum of multiple columns to equal a specified value.<br/><br/>
   *Parameters*:
   - `column_list (list)` – The list of column names to be summed.
   - `sum_total (int)` – The expected sum of the columns.
   </details>
</li>


</ol>


<br/><br/>

### Example

<pre snippet="data-quality/data-expectations#expectations" />
