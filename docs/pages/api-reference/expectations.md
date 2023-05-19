---
title: Expectations
order: 7
status: 'published'
---

# Expectations

Fennel internally relies on [Great Expectations](https://greatexpectations.io/) to help
users easily specify data expectations. Fennel's expectations are a subset of `Great Expectations`
expectations and are documented below, but the api to specify expectations is the same. 

## Expectation Types

1. ### [expect_column_values_to_not_be_null](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null)

#### Description
Expect the column values to not be null.

#### Parameters
* `column` (str) – The column name.


2. ### [expect_column_values_to_be_null](https://greatexpectations.io/expectations/expect_column_values_to_be_null)

#### Description
Expect the column values to be null.

#### Parameters
* `column` (str) – The column name.

3. ### [expect_column_values_to_be_of_type](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type)

#### Description

Expect a column to contain values of a specified data type.

#### Parameters
* `column` (str) – The column name.
* `type_` (str) – The expected data type of the column values.

4. ### [expect_column_values_to_be_in_type_list](https://greatexpectations.io/expectations/expect_column_values_to_be_in_type_list)

#### Description

Expect a column to contain values of one of several specified data types.

#### Parameters
* `column` (str) – The column name.
* `type_list` (list) – A list of expected data types of the column values.

5. ### [expect_column_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_be_in_set)

#### Description

Expect each column value to be in a given set.

#### Parameters

* `column` (str) – The column name.
* `value_set` (list) – A set of objects used for comparison.

6. ### [expect_column_values_to_not_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_not_be_in_set)

#### Description

Expect each column value to not be in a given set.

#### Parameters

* `column` (str) – The column name.
* `value_set` (list) – A set of objects used for comparison.

7. ### [expect_column_values_to_be_between](https://greatexpectations.io/expectations/expect_column_values_to_be_between)

#### Description

Expect column values to be between a minimum value and a maximum value.

#### Parameters

* `column` (str) – The column name.
* `min_value` (int) – The minimum value for a column entry.
* `max_value` (int) – The maximum value for a column entry.
* `strict_min` (bool) – If True, the column values must be strictly larger than min_value.
* `strict_max` (bool) – If True, the column values must be strictly smaller than max_value.

8. ### [expect_column_value_lengths_to_be_between](https://greatexpectations.io/expectations/expect_column_value_lengths_to_be_between)

#### Description

Expect column entries to be strings with length between a minimum value and a maximum value.

#### Parameters

* `column` (str) – The column name.
* `min_value` (int) – The minimum value for a column entry.
* `max_value` (int) – The maximum value for a column entry.


9. ### [expect_column_value_lengths_to_equal](https://greatexpectations.io/expectations/expect_column_value_lengths_to_equal)

#### Description

Expect column entries to be strings with length equal to a specified value.

#### Parameters

* `column` (str) – The column name.
* `value` (int) – The expected length of a column entry.

10. ### [expect_column_values_to_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex)

#### Description

Expect column entries to be strings that match a given regular expression.

#### Parameters

* `column` (str) – The column name.
* `regex` (str) – The regular expression that each column entry should match.

11. ### [expect_column_values_to_not_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex)

#### Description

Expect column entries to be strings that do not match a given regular expression.

#### Parameters

* `column` (str) – The column name.
* `regex` (str) – The regular expression that each column entry should not match.

12. ### [expect_column_values_to_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list)

#### Description

Expect column entries to be strings that match at least one of a list of regular expressions.

#### Parameters

* `column` (str) – The column name.
* `regex_list` (list) – The list of regular expressions that each column entry should match at least one of.

13. ### [expect_column_values_to_not_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)

#### Description

Expect column entries to be strings that do not match any of a list of regular expressions.

#### Parameters

* `column` (str) – The column name.
* `regex_list` (list) – The list of regular expressions that each column entry should not match any of.

14. ### [expect_column_values_to_match_strftime_format](https://greatexpectations.io/expectations/expect_column_values_to_match_strftime_format)

#### Description

Expect column entries to be strings representing a date or time with a given format.

#### Parameters

* `column` (str) – The column name.
* `strftime_format` (str) – The strftime format that each column entry should match.

15. ### [expect_column_values_to_be_dateutil_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_dateutil_parseable)

#### Description

Expect column entries to be parseable using dateutil.

#### Parameters

* `column` (str) – The column name.

16. ### [expect_column_values_to_be_json_parseable](https://greatexpectations.io/expectations/expect_column_values_to_be_json_parseable)

#### Description

Expect column entries to be parseable as JSON.

#### Parameters

* `column` (str) – The column name.

17. ### [expect_column_values_to_match_json_schema](https://greatexpectations.io/expectations/expect_column_values_to_match_json_schema)

#### Description

Expect column entries to match a given JSON schema.

#### Parameters

* `column` (str) – The column name.
* `json_schema` (dict) – The JSON schema that each column entry should match.

18. ### [expect_column_pair_values_to_be_equal](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_equal)

#### Description

Expect the values in a column to be the exact same as the values in another column.

#### Parameters

* `column_A` (str) – The first column name.
* `column_B` (str) – The second column name.
* `ignore_row_if` (str) – Control how null values are handled. See ignore_row_if for details.

19. ### [expect_column_pair_values_A_to_be_greater_than_B](https://greatexpectations.io/expectations/expect_column_pair_values_A_to_be_greater_than_B)

#### Description

Expect the values in column A to be greater than the values in column B.

#### Parameters

* `column_A` (str) – The first column name.
* `column_B` (str) – The second column name.
* `or_equal` (bool) – If True, then values can be equal, not strictly greater than.

20. ### [expect_column_pair_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_in_set)

#### Description

Expect the values in a column to belong to a given set.

#### Parameters

* `column_A` (str) – The first column name.
* `column_B` (str) – The second column name.
* `value_pairs_set` (set) – A set of tuples describing acceptable pairs of values. Each tuple should have two elements, the first from column A and the second from column B.

21. ### [expect_multicolumn_sum_to_equal](https://greatexpectations.io/expectations/expect_multicolumn_sum_to_equal)

#### Description

Expect the sum of multiple columns to equal a specified value.

#### Parameters

* `column_list` (list) – The list of column names to be summed.
* `sum_total` (int) – The expected sum of the columns.