from collections import defaultdict
from datetime import datetime
from functools import partial
from typing import Dict, List, Union, Optional, Any, Tuple

import numpy as np
import pandas as pd
from frozendict import frozendict

import fennel.datasets.datasets
import fennel.gen.schema_pb2 as schema_proto
from fennel.featuresets import Extractor, Feature, Featureset, is_valid_feature
from fennel.gen.featureset_pb2 import (
    ExtractorType as ProtoExtractorType,
)
from fennel.gen.schema_pb2 import Field, DSSchema, Schema
from fennel.internal_lib.schema import data_schema_check, get_datatype
from fennel.testing.branch import Entities
from fennel.testing.data_engine import DataEngine
from fennel.testing.test_utils import cast_col_to_dtype


class QueryEngine:
    """
    The Query Engine is a stateless class responsible for managing all aspects of lookups, including the execution of specified extractors and interaction with the data engine.
    """

    def lookup(
        self,
        data_engine: DataEngine,
        dataset_name: str,
        keys: pd.DataFrame,
        fields: Optional[List[str]] = None,
        timestamps: Optional[pd.Series] = None,
    ) -> Tuple[Union[pd.DataFrame, pd.Series], pd.Series]:
        """
        This function does a lookup on a dataset given data_engine containing the dataset.
        Args:
            data_engine: (DataEngine) - Data Engine containing the datasets and corresponding pandas dataframes
            dataset_name (str): The name of the dataset.
            keys (pd.DataFrame): All the keys to lookup.
            fields: (Optional[List[str]]): The fields to lookup. If None, all fields are returned.
            timestamps (Optional[pd.Series]): The timestamps to lookup. If None, the current time is used.
        Returns:

        """
        if isinstance(keys, list):
            raise ValueError(
                "keys should be a pandas DataFrame, not a dictionary"
            )

        if dataset_name not in data_engine.get_dataset_names():
            raise KeyError(f"Dataset: {dataset_name} not found")

        dataset_fields = data_engine.get_dataset_fields(dataset_name)
        dataset = data_engine.get_dataset(dataset_name)

        if fields is not None:
            for field_name in fields:
                if field_name not in dataset_fields:
                    raise ValueError(f"Field: {field_name} not in dataset")
        else:
            fields = dataset_fields

        fennel.datasets.datasets.dataset_lookup = partial(
            data_engine.get_dataset_lookup_impl(None, [dataset_name]),
        )

        timestamps = (
            cast_col_to_dtype(
                pd.Series(timestamps),
                schema_proto.DataType(
                    timestamp_type=schema_proto.TimestampType()
                ),
            )
            if timestamps is not None
            else pd.Series([datetime.utcnow() for _ in range(len(keys))])
        )
        keys = keys.to_dict(orient="records")

        keys_dict = defaultdict(list)
        for key in keys:
            for key_name in key.keys():
                keys_dict[key_name].append(key[key_name])

        data, found = dataset.lookup(
            timestamps,
            **{name: pd.Series(value) for name, value in keys_dict.items()},
        )

        fennel.datasets.datasets.dataset_lookup = partial(
            data_engine.get_dataset_lookup_impl(None, None),
        )
        if len(fields) == 1:
            return pd.Series(name=fields[0], data=data[fields[0]]), found
        return data[fields], found

    def run_extractors(
        self,
        extractors_to_run: List[Extractor],
        data_engine: DataEngine,
        entities: Entities,
        input_dataframe: pd.DataFrame,
        outputs: List[Union[Feature, Featureset, str]],
        timestamps: pd.Series,
    ):
        """
        Runs list of extractors on data engine.
        Args:
            extractors_to_run: (List[Extractor]) - List of extractors to run.
            data_engine: (DataEngine) - DataEngine containing datasets and corresponding pandas dataframes.
            entities: (Entities) - Containing features, featuresets and extractors.
            input_dataframe: (pd.DataFrame) - Keys against extractors will extract data from data engine.
            outputs: (List[Union[Feature, Featureset, str]]) - Output features.
            timestamps: (pd.Series) - Timestamp as of which extractors will extract data from data engine.
        Returns:
            pandas dataframe
        """
        # Map of input name to the pandas series
        intermediate_data = self._get_input_dataframe_map(input_dataframe)
        for extractor in extractors_to_run:
            prepare_args = self._prepare_extractor_args(
                extractor, intermediate_data
            )
            features = entities.features_for_fs[extractor.featureset]
            feature_schema = {}
            for feature in features:
                feature_schema[f"{extractor.featureset}.{feature.name}"] = (
                    feature.dtype
                )
            fields = []
            for feature_str in extractor.output_features:
                feature_str = f"{extractor.featureset}.{feature_str}"
                if feature_str not in feature_schema:
                    raise ValueError(f"Feature `{feature_str}` not found")
                dtype = feature_schema[feature_str]
                fields.append(Field(name=feature_str, dtype=dtype))
            dsschema = DSSchema(
                values=Schema(fields=fields)
            )  # stuff every field as value

            if extractor.extractor_type == ProtoExtractorType.ALIAS:
                feature_name = extractor.fqn_output_features()[0]
                intermediate_data[feature_name] = intermediate_data[
                    extractor.inputs[0].fqn()
                ]
                intermediate_data[feature_name].name = feature_name
                self._check_schema_exceptions(
                    intermediate_data[feature_name], dsschema, extractor.name
                )
                continue

            if extractor.extractor_type == ProtoExtractorType.LOOKUP:
                output = self._compute_lookup_extractor(
                    data_engine, extractor, timestamps.copy(), intermediate_data
                )
                self._check_schema_exceptions(output, dsschema, extractor.name)
                continue

            allowed_datasets = self._get_allowed_datasets(extractor)
            fennel.datasets.datasets.dataset_lookup = (
                data_engine.get_dataset_lookup_impl(
                    extractor.name, allowed_datasets
                )
            )
            extractor_fqn = f"{extractor.featureset}.{extractor.name}"
            func = entities.extractor_funcs[extractor_fqn]
            try:
                ts_clone = timestamps.copy()
                output = func(ts_clone, *prepare_args)
            except Exception as e:
                raise Exception(
                    f"Extractor `{extractor.name}` in `{extractor.featureset}` "
                    f"failed to run with error: {e}. "
                )
            fennel.datasets.datasets.dataset_lookup = partial(
                data_engine.get_dataset_lookup_impl(None, None)
            )
            if not isinstance(output, (pd.Series, pd.DataFrame)):
                raise Exception(
                    f"Extractor `{extractor.name}` returned "
                    f"invalid type `{type(output)}`, expected a pandas series or dataframe"
                )
            self._check_schema_exceptions(output, dsschema, extractor.name)
            if isinstance(output, pd.Series):
                if output.name in intermediate_data:
                    continue
                # If output is a dict, convert it to frozendict
                if output.apply(lambda x: isinstance(x, dict)).any():
                    output = frozendict(output)
                intermediate_data[output.name] = output
            elif isinstance(output, pd.DataFrame):
                for col in output.columns:
                    if col in intermediate_data:
                        continue
                    if output[col].apply(lambda x: isinstance(x, dict)).any():
                        output[col] = output[col].apply(frozendict)
                    intermediate_data[col] = output[col]
            else:
                raise Exception(
                    f"Extractor {extractor.name} returned "
                    f"invalid type {type(output)}"
                )

        self._validate_extractor_output(intermediate_data)
        return self._prepare_output_df(intermediate_data, outputs)

    def _prepare_output_df(
        self,
        data: Dict[str, pd.Series],
        outputs: List[Union[Feature, Featureset, str]],
    ) -> pd.DataFrame:
        """
        Prepares the output dataframe using outputs and extractor outputs.
        Args:
            data: (Dict[str, pd.Series]) - Dict with input names and extractor output name mapped to corresponding pd.Series.
            outputs: (List[Union[Feature, Featureset, str]]) - List of output from user
        Returns:
            Pandas DataFrame
        """
        output_df = pd.DataFrame()
        for out_feature in outputs:
            if isinstance(out_feature, Feature):
                output_df[out_feature.fqn_] = data[out_feature.fqn_]
            elif isinstance(out_feature, str) and is_valid_feature(out_feature):
                output_df[out_feature] = data[out_feature]
            elif isinstance(out_feature, Featureset):
                for f in out_feature.features:
                    output_df[f.fqn_] = data[f.fqn_]
            elif type(out_feature) is tuple:
                for f in out_feature:
                    output_df[f.fqn_] = data[f.fqn_]
            else:
                raise Exception(
                    f"Unknown feature {out_feature} of type {type(out_feature)} found "
                    f"during feature extraction."
                )
        return output_df

    def _validate_extractor_output(self, data: Dict[str, pd.Series]):
        """
        Ensure the  number of rows in each column is the same
        Args:
            data: (Dict[str, pd.Series]) - Dict with input names and extractor output name mapped to corresponding pd.Series.
        """
        # Ensure the  number of rows in each column is the same
        num_rows_per_col = {col: len(data[col]) for col in data}
        first_col = list(num_rows_per_col.keys())[0]
        for col, num_rows in num_rows_per_col.items():
            if num_rows != num_rows_per_col[first_col]:
                raise Exception(
                    f"Number of values in feature {col} is {num_rows}, "
                    f"but {num_rows_per_col[first_col]} in feature {first_col}. "
                )

    def _get_input_dataframe_map(
        self, input_dataframe: pd.DataFrame
    ) -> Dict[str, pd.Series]:
        """
        Returns map of input name to pandas series
        Args:
            input_dataframe: (pd.DataFrame) - Input DataFrame
        Returns:
            Dict[str, pd.Series]
        """
        # Map of feature name to the pandas series
        output: Dict[str, pd.Series] = {}
        for col in input_dataframe.columns:
            if input_dataframe[col].apply(lambda x: isinstance(x, dict)).any():
                input_dataframe[col] = input_dataframe[col].apply(
                    lambda x: frozendict(x)
                )
            output[col] = input_dataframe[col].reset_index(drop=True)
        return output

    def _prepare_extractor_args(
        self, extractor: Extractor, intermediate_data: Dict[str, pd.Series]
    ):
        args = []
        for input in extractor.inputs:
            if isinstance(input, Feature):
                if input.fqn_ in intermediate_data:
                    if (
                        intermediate_data[input.fqn_]
                        .apply(lambda x: isinstance(x, dict))
                        .any()
                    ):
                        intermediate_data[input.fqn_] = intermediate_data[
                            input.fqn_
                        ].apply(lambda x: frozendict(x))
                    args.append(intermediate_data[input.fqn_])
                else:
                    raise Exception(
                        f"Feature `{input}` could not be "
                        f"calculated by any extractor."
                    )
            elif isinstance(input, Featureset):
                raise Exception(
                    "Featureset is not supported as input to an "
                    "extractor since they are mutable."
                )
            elif type(input) is tuple:
                series = []
                for feature in input:
                    if feature.fqn_ in intermediate_data:
                        series.append(intermediate_data[feature.fqn_])
                    else:
                        raise Exception(
                            f"Feature {feature.fqn_} couldn't be "
                            f"calculated by any extractor."
                        )
                if series.apply(lambda x: isinstance(x, dict)).any():
                    series = series.apply(lambda x: frozendict(x))
                args.append(pd.concat(series, axis=1))
            else:
                raise Exception(
                    f"Unknown input type {type(input)} found "
                    f"during feature extraction."
                )
        return args

    def _check_schema_exceptions(
        self, output, dsschema: DSSchema, extractor_name: str
    ):
        if output is None or output.shape[0] == 0:
            return
        output_df = pd.DataFrame(output)
        output_df.reset_index(inplace=True)
        exceptions = data_schema_check(dsschema, output_df, extractor_name)
        if len(exceptions) > 0:
            raise Exception(
                f"Extractor `{extractor_name}` returned "
                f"invalid schema for data: {exceptions}"
            )

    def _compute_lookup_extractor(
        self,
        data_engine: DataEngine,
        extractor: Extractor,
        timestamps: pd.Series,
        intermediate_data: Dict[str, pd.Series],
    ) -> pd.Series:
        if len(extractor.output_features) != 1:
            raise ValueError(
                f"Lookup extractor {extractor.name} must have exactly one output feature, found {len(extractor.output_features)}"
            )
        if len(extractor.depends_on) != 1:
            raise ValueError(
                f"Lookup extractor {extractor.name} must have exactly one dependent dataset, found {len(extractor.depends_on)}"
            )

        input_features = {
            k.name: intermediate_data[k] for k in extractor.inputs  # type: ignore
        }
        allowed_datasets = self._get_allowed_datasets(extractor)
        fennel.datasets.datasets.dataset_lookup = (
            data_engine.get_dataset_lookup_impl(
                extractor.name, allowed_datasets
            )
        )
        results, _ = extractor.depends_on[0].lookup(
            timestamps, **input_features
        )
        if (
            not extractor.derived_extractor_info
            or not extractor.derived_extractor_info.field
            or not extractor.derived_extractor_info.field.name
        ):
            raise TypeError(
                f"Field for lookup extractor {extractor.name} must have a named field"
            )
        results = results[extractor.derived_extractor_info.field.name]
        if extractor.derived_extractor_info.default is not None:
            if results.dtype != object:
                results = results.fillna(
                    extractor.derived_extractor_info.default
                )
            else:
                # fillna doesn't work for list type or dict type :cols
                for row in results.loc[results.isnull()].index:
                    results[row] = extractor.derived_extractor_info.default
            results = cast_col_to_dtype(
                results,
                get_datatype(extractor.derived_extractor_info.field.dtype),
            )
        else:
            results = cast_col_to_dtype(
                results,
                get_datatype(
                    Optional[extractor.derived_extractor_info.field.dtype]
                ),
            )
            results.replace({np.nan: None}, inplace=True)
        fennel.datasets.datasets.dataset_lookup = (
            data_engine.get_dataset_lookup_impl(None, None)
        )
        results.name = extractor.fqn_output_features()[0]
        intermediate_data[extractor.fqn_output_features()[0]] = results
        return results

    def _get_allowed_datasets(self, extractor: Extractor) -> List[str]:
        return [x._name for x in extractor.get_dataset_dependencies()]
