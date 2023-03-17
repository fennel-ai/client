import inspect

from typing import Tuple


def expect_column_values_to_not_be_null(
    column,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column values to not be null.

    To be counted as an exception, values must be explicitly null or missing, such as a NULL in PostgreSQL or an
    np.NaN in pandas. Empty strings don't count as null unless they have been coerced to a null type.

    expect_column_values_to_not_be_null is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_be_null \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_null>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_be_null(
    column,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column values to be null.

    expect_column_values_to_be_null is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_not_be_null \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_be_null>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_be_of_type(
    column,
    type_,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect a column to contain values of a specified data type.

    expect_column_values_to_be_of_type is a :func:`column_aggregate_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>` for typed-column backends,
    and also for PandasDataset where the column dtype and provided type_ are unambiguous constraints (any dtype
    except 'object' or dtype of 'object' with type_ specified as 'object').

    For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type is a
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will
    independently check each row's type.

    Args:
        column (str): \
            The column name.
        type\\_ (str): \
            A string representing the data type that each column should have as entries. Valid types are defined
            by the current backend implementation and are dynamically loaded. For example, valid types for
            PandasDataset include any numpy dtype values (such as 'int64') or native python types (such as 'int'),
            whereas valid types for a SqlAlchemyDataset include types named by the current driver such as 'INTEGER'
            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for SparkDFDataset include
            'StringType', 'BooleanType' and other pyspark-defined type names.


    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See also:
        :func:`expect_column_values_to_be_in_type_list \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_type_list>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_be_in_type_list(
    column,
    type_list,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect a column to contain values from a specified type list.

    expect_column_values_to_be_in_type_list is a :func:`column_aggregate_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_aggregate_expectation>` for typed-column backends,
    and also for PandasDataset where the column dtype provides an unambiguous constraints (any dtype except
    'object'). For PandasDataset columns with dtype of 'object' expect_column_values_to_be_of_type is a
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>` and will
    independently check each row's type.

    Args:
        column (str): \
            The column name.
        type_list (str): \
            A list of strings representing the data type that each column should have as entries. Valid types are
            defined by the current backend implementation and are dynamically loaded. For example, valid types for
            PandasDataset include any numpy dtype values (such as 'int64') or native python types (such as 'int'),
            whereas valid types for a SqlAlchemyDataset include types named by the current driver such as 'INTEGER'
            in most SQL dialects and 'TEXT' in dialects such as postgresql. Valid types for SparkDFDataset include
            'StringType', 'BooleanType' and other pyspark-defined type names.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See also:
        :func:`expect_column_values_to_be_of_type \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_of_type>`

    """
    return inspect.stack()[0].function.lower(), locals()


###
#
# Sets and ranges
#
####


def expect_column_values_to_be_in_set(
    column,
    value_set,
    mostly=None,
    parse_strings_as_datetimes=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    # noinspection PyUnresolvedReferences
    """Expect each column value to be in a given set.

    For example:
    ::

        # my_df.my_col = [1,2,2,3,3,3]
        >>> my_df.expect_column_values_to_be_in_set(
            "my_col",
            [2,3]
        )
        {
          "success": false
          "result": {
            "unexpected_count": 1
            "unexpected_percent": 16.66666666666666666,
            "unexpected_percent_nonmissing": 16.66666666666666666,
            "partial_unexpected_list": [
              1
            ],
          },
        }

    expect_column_values_to_be_in_set is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        value_set (set-like): \
            A set of objects used for comparison.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.
        parse_strings_as_datetimes (boolean or None) : If True values provided in value_set will be parsed as \
            datetimes before making comparisons.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_not_be_in_set \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_be_in_set>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_not_be_in_set(
    column,
    value_set,
    mostly=None,
    parse_strings_as_datetimes=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    # noinspection PyUnresolvedReferences
    """Expect column entries to not be in the set.

    For example:
    ::

        # my_df.my_col = [1,2,2,3,3,3]
        >>> my_df.expect_column_values_to_not_be_in_set(
            "my_col",
            [1,2]
        )
        {
          "success": false
          "result": {
            "unexpected_count": 3
            "unexpected_percent": 50.0,
            "unexpected_percent_nonmissing": 50.0,
            "partial_unexpected_list": [
              1, 2, 2
            ],
          },
        }

    expect_column_values_to_not_be_in_set is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        value_set (set-like): \
            A set of objects used for comparison.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_be_in_set \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_in_set>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_be_between(
    column,
    min_value=None,
    max_value=None,
    strict_min=False,
    strict_max=False,
    # tolerance=1e-9,
    allow_cross_type_comparisons=None,
    parse_strings_as_datetimes=False,
    output_strftime_format=None,
    mostly=None,
    row_condition=None,
    condition_parser=None,
    result_format=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be between a minimum value and a maximum value (inclusive).

    expect_column_values_to_be_between is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        min_value (comparable type or None): The minimum value for a column entry.
        max_value (comparable type or None): The maximum value for a column entry.

    Keyword Args:
        strict_min (boolean):
            If True, values must be strictly larger than min_value, default=False
        strict_max (boolean):
            If True, values must be strictly smaller than max_value, default=False
         allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and\
            string). Otherwise, attempting such comparisons will raise an exception.
        parse_strings_as_datetimes (boolean or None) : If True, parse min_value, max_value, and all non-null column\
            values to datetimes before making comparisons.
        output_strftime_format (str or None): \
            A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    Notes:
        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound, and there is no minimum value checked.
        * If max_value is None, then min_value is treated as a lower bound, and there is no maximum value checked.

    See Also:
        :func:`expect_column_value_lengths_to_be_between \
        <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_be_between>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_value_lengths_to_be_between(
    column,
    min_value=None,
    max_value=None,
    mostly=None,
    row_condition=None,
    condition_parser=None,
    result_format=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be strings with length between a minimum value and a maximum value (inclusive).

    This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.

    expect_column_value_lengths_to_be_between is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        min_value (int or None): \
            The minimum value for a column entry length.
        max_value (int or None): \
            The maximum value for a column entry length.
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    Notes:
        * min_value and max_value are both inclusive.
        * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has \
          no minimum.
        * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has \
          no maximum.

    See Also:
        :func:`expect_column_value_lengths_to_equal \
        <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_equal>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_value_lengths_to_equal(
    column,
    value,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be strings with length equal to the provided value.

    This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.

    expect_column_values_to_be_between is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        value (int or None): \
            The expected value for a column entry length.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_value_lengths_to_be_between \
        <great_expectations.dataset.dataset.Dataset.expect_column_value_lengths_to_be_between>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_match_regex(
    column,
    regex,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be strings that match a given regular expression. Valid matches can be found \
    anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat", \
    "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".

    expect_column_values_to_match_regex is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        regex (str): \
            The regular expression the column entries should match.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_not_match_regex \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_regex>`

        :func:`expect_column_values_to_match_regex_list \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_not_match_regex(
    column,
    regex,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be strings that do NOT match a given regular expression. The regex must not match \
    any portion of the provided string. For example, "[at]+" would identify the following strings as expected: \
    "fish", "dog", and the following as unexpected: "cat", "hat".

    expect_column_values_to_not_match_regex is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        regex (str): \
            The regular expression the column entries should NOT match.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_match_regex \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex>`

        :func:`expect_column_values_to_match_regex_list \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_match_regex_list(
    column,
    regex_list,
    match_on="any",
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect the column entries to be strings that can be matched to either any of or all of a list of regular
    expressions. Matches can be anywhere in the string.

    expect_column_values_to_match_regex_list is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        regex_list (list): \
            The list of regular expressions which the column entries should match

    Keyword Args:
        match_on= (string): \
            "any" or "all".
            Use "any" if the value should match at least one regular expression in the list.
            Use "all" if it should match each regular expression in the list.
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_match_regex \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex>`

        :func:`expect_column_values_to_not_match_regex \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_not_match_regex>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_not_match_regex_list(
    column,
    regex_list,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect the column entries to be strings that do not match any of a list of regular expressions. Matches can
    be anywhere in the string.

    expect_column_values_to_not_match_regex_list is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        regex_list (list): \
            The list of regular expressions which the column entries should not match

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`. \
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_match_regex_list \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_regex_list>`

    """
    return inspect.stack()[0].function.lower(), locals()


###
#
# Datetime and JSON parsing
#
###


def expect_column_values_to_match_strftime_format(
    column,
    strftime_format,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be strings representing a date or time with a given format.

    expect_column_values_to_match_strftime_format is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        strftime_format (str): \
            A strftime format string to use for matching

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_be_dateutil_parseable(
    column,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be parsable using dateutil.

    expect_column_values_to_be_dateutil_parseable is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_be_json_parseable(
    column,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be data written in JavaScript Object Notation.

    expect_column_values_to_be_json_parseable is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_match_json_schema \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_match_json_schema>`

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_values_to_match_json_schema(
    column,
    json_schema,
    mostly=None,
    result_format=None,
    row_condition=None,
    condition_parser=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """Expect column entries to be JSON objects matching a given JSON schema.

    expect_column_values_to_match_json_schema is a \
    :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    See Also:
        :func:`expect_column_values_to_be_json_parseable \
        <great_expectations.dataset.dataset.Dataset.expect_column_values_to_be_json_parseable>`


        The `JSON-schema docs <http://json-schema.org/>`_.
    """
    return inspect.stack()[0].function.lower(), locals()


###
#
# Column pairs
#
###


def expect_column_pair_values_to_be_equal(
    column_A,
    column_B,
    ignore_row_if="both_values_are_missing",
    result_format=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """
    Expect the values in column A to be the same as column B.

    Args:
        column_A (str): The first column name
        column_B (str): The second column name

    Keyword Args:
        ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither"

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_pair_values_A_to_be_greater_than_B(
    column_A,
    column_B,
    or_equal=None,
    parse_strings_as_datetimes=False,
    allow_cross_type_comparisons=None,
    ignore_row_if="both_values_are_missing",
    result_format=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """
    Expect values in column A to be greater than column B.

    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        or_equal (boolean or None): If True, then values can be equal, not strictly greater

    Keyword Args:
        allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and\
            string). Otherwise, attempting such comparisons will raise an exception.

    Keyword Args:
        ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    """
    return inspect.stack()[0].function.lower(), locals()


def expect_column_pair_values_to_be_in_set(
    column_A,
    column_B,
    value_pairs_set,
    ignore_row_if="both_values_are_missing",
    result_format=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """
    Expect paired values from columns A and B to belong to a set of valid pairs.

    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        value_pairs_set (list of tuples): All the valid pairs to be matched

    Keyword Args:
        ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither"

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    """
    return inspect.stack()[0].function.lower(), locals()


#
# Multicolumn
#
###


def expect_multicolumn_sum_to_equal(
    column_list,
    sum_total,
    result_format=None,
    include_config=True,
    catch_exceptions=None,
    meta=None,
) -> Tuple[str, dict]:
    """ Multi-Column Map Expectation

    Expects that the sum of row values is the same for each row, summing only values in columns specified in
    column_list, and equal to the specific value, sum_total.

    For example (with column_list=["B", "C"] and sum_total=5)::

        A B C
        1 3 2
        1 5 0
        1 1 4

        Pass

        A B C
        1 3 2
        1 5 1
        1 1 4

        Fail on row 2

    Args:
        column_list (List[str]): \
            Set of columns to be checked
        sum_total (int): \
            expected sum of columns
    """
    return inspect.stack()[0].function.lower(), locals()
