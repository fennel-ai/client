"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.duration_pb2
import google.protobuf.internal.containers
import google.protobuf.message
import metadata_pb2
import pycode_pb2
import schema_pb2
import spec_pb2
import sys
import typing

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class CoreDataset(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class FieldMetadataEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> metadata_pb2.Metadata: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: metadata_pb2.Metadata | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    NAME_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    DSSCHEMA_FIELD_NUMBER: builtins.int
    HISTORY_FIELD_NUMBER: builtins.int
    RETENTION_FIELD_NUMBER: builtins.int
    FIELD_METADATA_FIELD_NUMBER: builtins.int
    PYCODE_FIELD_NUMBER: builtins.int
    IS_SOURCE_DATASET_FIELD_NUMBER: builtins.int
    LINEAGES_FIELD_NUMBER: builtins.int
    ACTIVE_DATAFLOW_FIELD_NUMBER: builtins.int
    name: builtins.str
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    @property
    def dsschema(self) -> schema_pb2.DSSchema: ...
    @property
    def history(self) -> google.protobuf.duration_pb2.Duration: ...
    @property
    def retention(self) -> google.protobuf.duration_pb2.Duration: ...
    @property
    def field_metadata(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, metadata_pb2.Metadata]: ...
    @property
    def pycode(self) -> pycode_pb2.PyCode: ...
    is_source_dataset: builtins.bool
    @property
    def lineages(self) -> global___DatasetLineages:
        """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
        THE CLIENT
        """
    @property
    def active_dataflow(self) -> global___Dataflow: ...
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        metadata: metadata_pb2.Metadata | None = ...,
        dsschema: schema_pb2.DSSchema | None = ...,
        history: google.protobuf.duration_pb2.Duration | None = ...,
        retention: google.protobuf.duration_pb2.Duration | None = ...,
        field_metadata: collections.abc.Mapping[builtins.str, metadata_pb2.Metadata] | None = ...,
        pycode: pycode_pb2.PyCode | None = ...,
        is_source_dataset: builtins.bool = ...,
        lineages: global___DatasetLineages | None = ...,
        active_dataflow: global___Dataflow | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["active_dataflow", b"active_dataflow", "dsschema", b"dsschema", "history", b"history", "lineages", b"lineages", "metadata", b"metadata", "pycode", b"pycode", "retention", b"retention"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["active_dataflow", b"active_dataflow", "dsschema", b"dsschema", "field_metadata", b"field_metadata", "history", b"history", "is_source_dataset", b"is_source_dataset", "lineages", b"lineages", "metadata", b"metadata", "name", b"name", "pycode", b"pycode", "retention", b"retention"]) -> None: ...

global___CoreDataset = CoreDataset

@typing_extensions.final
class OnDemand(google.protobuf.message.Message):
    """All integers representing time are in microseconds and hence should be int64."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FUNCTION_SOURCE_CODE_FIELD_NUMBER: builtins.int
    FUNCTION_FIELD_NUMBER: builtins.int
    EXPIRES_AFTER_FIELD_NUMBER: builtins.int
    function_source_code: builtins.str
    function: builtins.bytes
    expires_after: builtins.int
    """TODO(mohit): Make this duration"""
    def __init__(
        self,
        *,
        function_source_code: builtins.str = ...,
        function: builtins.bytes = ...,
        expires_after: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["expires_after", b"expires_after", "function", b"function", "function_source_code", b"function_source_code"]) -> None: ...

global___OnDemand = OnDemand

@typing_extensions.final
class Pipeline(google.protobuf.message.Message):
    """----------------------------------------------------------------------------------------------
    Pipeline
    ----------------------------------------------------------------------------------------------
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NAME_FIELD_NUMBER: builtins.int
    DATASET_NAME_FIELD_NUMBER: builtins.int
    SIGNATURE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    INPUT_DATASET_NAMES_FIELD_NUMBER: builtins.int
    VERSION_FIELD_NUMBER: builtins.int
    ACTIVE_FIELD_NUMBER: builtins.int
    LINEAGES_FIELD_NUMBER: builtins.int
    name: builtins.str
    dataset_name: builtins.str
    signature: builtins.str
    @property
    def metadata(self) -> metadata_pb2.Metadata: ...
    @property
    def input_dataset_names(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    version: builtins.int
    active: builtins.bool
    @property
    def lineages(self) -> global___PipelineLineages:
        """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
        THE CLIENT
        """
    def __init__(
        self,
        *,
        name: builtins.str = ...,
        dataset_name: builtins.str = ...,
        signature: builtins.str = ...,
        metadata: metadata_pb2.Metadata | None = ...,
        input_dataset_names: collections.abc.Iterable[builtins.str] | None = ...,
        version: builtins.int = ...,
        active: builtins.bool = ...,
        lineages: global___PipelineLineages | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["lineages", b"lineages", "metadata", b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["active", b"active", "dataset_name", b"dataset_name", "input_dataset_names", b"input_dataset_names", "lineages", b"lineages", "metadata", b"metadata", "name", b"name", "signature", b"signature", "version", b"version"]) -> None: ...

global___Pipeline = Pipeline

@typing_extensions.final
class Operator(google.protobuf.message.Message):
    """Each operator corresponds to a valid operation as part of a pipeline"""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    IS_ROOT_FIELD_NUMBER: builtins.int
    PIPELINE_NAME_FIELD_NUMBER: builtins.int
    DATASET_NAME_FIELD_NUMBER: builtins.int
    AGGREGATE_FIELD_NUMBER: builtins.int
    JOIN_FIELD_NUMBER: builtins.int
    TRANSFORM_FIELD_NUMBER: builtins.int
    UNION_FIELD_NUMBER: builtins.int
    FILTER_FIELD_NUMBER: builtins.int
    DATASET_REF_FIELD_NUMBER: builtins.int
    NAME_FIELD_NUMBER: builtins.int
    id: builtins.str
    """Every operator has an ID assigned by the client"""
    is_root: builtins.bool
    """If the operator is the "root" operator in the given `pipeline` of the given
    `dataset`.
    """
    pipeline_name: builtins.str
    """Name of the pipeline in which this operator is defined in"""
    dataset_name: builtins.str
    """Name of the dataset in which the pipeline is defined in"""
    @property
    def aggregate(self) -> global___Aggregate: ...
    @property
    def join(self) -> global___Join: ...
    @property
    def transform(self) -> global___Transform: ...
    @property
    def union(self) -> global___Union: ...
    @property
    def filter(self) -> global___Filter: ...
    @property
    def dataset_ref(self) -> global___DatasetRef: ...
    name: builtins.str
    """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
    THE CLIENT

    Name of the operator assigned by the server
    """
    def __init__(
        self,
        *,
        id: builtins.str = ...,
        is_root: builtins.bool = ...,
        pipeline_name: builtins.str = ...,
        dataset_name: builtins.str = ...,
        aggregate: global___Aggregate | None = ...,
        join: global___Join | None = ...,
        transform: global___Transform | None = ...,
        union: global___Union | None = ...,
        filter: global___Filter | None = ...,
        dataset_ref: global___DatasetRef | None = ...,
        name: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["aggregate", b"aggregate", "dataset_ref", b"dataset_ref", "filter", b"filter", "join", b"join", "kind", b"kind", "transform", b"transform", "union", b"union"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["aggregate", b"aggregate", "dataset_name", b"dataset_name", "dataset_ref", b"dataset_ref", "filter", b"filter", "id", b"id", "is_root", b"is_root", "join", b"join", "kind", b"kind", "name", b"name", "pipeline_name", b"pipeline_name", "transform", b"transform", "union", b"union"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["kind", b"kind"]) -> typing_extensions.Literal["aggregate", "join", "transform", "union", "filter", "dataset_ref"] | None: ...

global___Operator = Operator

@typing_extensions.final
class Aggregate(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERAND_ID_FIELD_NUMBER: builtins.int
    KEYS_FIELD_NUMBER: builtins.int
    SPECS_FIELD_NUMBER: builtins.int
    OPERAND_NAME_FIELD_NUMBER: builtins.int
    operand_id: builtins.str
    @property
    def keys(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def specs(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[spec_pb2.PreSpec]: ...
    operand_name: builtins.str
    """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
    THE CLIENT
    """
    def __init__(
        self,
        *,
        operand_id: builtins.str = ...,
        keys: collections.abc.Iterable[builtins.str] | None = ...,
        specs: collections.abc.Iterable[spec_pb2.PreSpec] | None = ...,
        operand_name: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["keys", b"keys", "operand_id", b"operand_id", "operand_name", b"operand_name", "specs", b"specs"]) -> None: ...

global___Aggregate = Aggregate

@typing_extensions.final
class Join(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class OnEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        value: builtins.str
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: builtins.str = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    LHS_OPERAND_ID_FIELD_NUMBER: builtins.int
    RHS_DSREF_OPERAND_ID_FIELD_NUMBER: builtins.int
    ON_FIELD_NUMBER: builtins.int
    WITHIN_LOW_FIELD_NUMBER: builtins.int
    WITHIN_HIGH_FIELD_NUMBER: builtins.int
    LHS_OPERAND_NAME_FIELD_NUMBER: builtins.int
    RHS_DSREF_OPERAND_NAME_FIELD_NUMBER: builtins.int
    lhs_operand_id: builtins.str
    rhs_dsref_operand_id: builtins.str
    """RHS of a JOIN can only be a dataset, here it refers to the DSRef operator"""
    @property
    def on(self) -> google.protobuf.internal.containers.ScalarMap[builtins.str, builtins.str]:
        """Map of left field name to right field name to join on."""
    @property
    def within_low(self) -> google.protobuf.duration_pb2.Duration: ...
    @property
    def within_high(self) -> google.protobuf.duration_pb2.Duration: ...
    lhs_operand_name: builtins.str
    """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
    THE CLIENT
    """
    rhs_dsref_operand_name: builtins.str
    def __init__(
        self,
        *,
        lhs_operand_id: builtins.str = ...,
        rhs_dsref_operand_id: builtins.str = ...,
        on: collections.abc.Mapping[builtins.str, builtins.str] | None = ...,
        within_low: google.protobuf.duration_pb2.Duration | None = ...,
        within_high: google.protobuf.duration_pb2.Duration | None = ...,
        lhs_operand_name: builtins.str = ...,
        rhs_dsref_operand_name: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_within_high", b"_within_high", "_within_low", b"_within_low", "within_high", b"within_high", "within_low", b"within_low"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_within_high", b"_within_high", "_within_low", b"_within_low", "lhs_operand_id", b"lhs_operand_id", "lhs_operand_name", b"lhs_operand_name", "on", b"on", "rhs_dsref_operand_id", b"rhs_dsref_operand_id", "rhs_dsref_operand_name", b"rhs_dsref_operand_name", "within_high", b"within_high", "within_low", b"within_low"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_within_high", b"_within_high"]) -> typing_extensions.Literal["within_high"] | None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_within_low", b"_within_low"]) -> typing_extensions.Literal["within_low"] | None: ...

global___Join = Join

@typing_extensions.final
class Transform(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class SchemaEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> schema_pb2.DataType: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: schema_pb2.DataType | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    OPERAND_ID_FIELD_NUMBER: builtins.int
    SCHEMA_FIELD_NUMBER: builtins.int
    PYCODE_FIELD_NUMBER: builtins.int
    OPERAND_NAME_FIELD_NUMBER: builtins.int
    operand_id: builtins.str
    @property
    def schema(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, schema_pb2.DataType]: ...
    @property
    def pycode(self) -> pycode_pb2.PyCode: ...
    operand_name: builtins.str
    """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
    THE CLIENT
    """
    def __init__(
        self,
        *,
        operand_id: builtins.str = ...,
        schema: collections.abc.Mapping[builtins.str, schema_pb2.DataType] | None = ...,
        pycode: pycode_pb2.PyCode | None = ...,
        operand_name: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["pycode", b"pycode"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["operand_id", b"operand_id", "operand_name", b"operand_name", "pycode", b"pycode", "schema", b"schema"]) -> None: ...

global___Transform = Transform

@typing_extensions.final
class Filter(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERAND_ID_FIELD_NUMBER: builtins.int
    PYCODE_FIELD_NUMBER: builtins.int
    OPERAND_NAME_FIELD_NUMBER: builtins.int
    operand_id: builtins.str
    @property
    def pycode(self) -> pycode_pb2.PyCode: ...
    operand_name: builtins.str
    """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
    THE CLIENT
    """
    def __init__(
        self,
        *,
        operand_id: builtins.str = ...,
        pycode: pycode_pb2.PyCode | None = ...,
        operand_name: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["pycode", b"pycode"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["operand_id", b"operand_id", "operand_name", b"operand_name", "pycode", b"pycode"]) -> None: ...

global___Filter = Filter

@typing_extensions.final
class Union(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    OPERAND_IDS_FIELD_NUMBER: builtins.int
    OPERAND_NAMES_FIELD_NUMBER: builtins.int
    @property
    def operand_ids(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def operand_names(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]:
        """NOTE: FOLLOWING PROPERTIES ARE SET BY THE SERVER AND WILL BE IGNORED BY
        THE CLIENT
        """
    def __init__(
        self,
        *,
        operand_ids: collections.abc.Iterable[builtins.str] | None = ...,
        operand_names: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["operand_ids", b"operand_ids", "operand_names", b"operand_names"]) -> None: ...

global___Union = Union

@typing_extensions.final
class DatasetRef(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    REFERRING_DATASET_NAME_FIELD_NUMBER: builtins.int
    referring_dataset_name: builtins.str
    def __init__(
        self,
        *,
        referring_dataset_name: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["referring_dataset_name", b"referring_dataset_name"]) -> None: ...

global___DatasetRef = DatasetRef

@typing_extensions.final
class Dataflow(google.protobuf.message.Message):
    """----------------------------------------------------------------------------------------------
    Lineage
    ----------------------------------------------------------------------------------------------
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class PipelineDataflow(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        DATASET_NAME_FIELD_NUMBER: builtins.int
        PIPELINE_NAME_FIELD_NUMBER: builtins.int
        INPUT_DATAFLOWS_FIELD_NUMBER: builtins.int
        dataset_name: builtins.str
        pipeline_name: builtins.str
        @property
        def input_dataflows(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Dataflow]: ...
        def __init__(
            self,
            *,
            dataset_name: builtins.str = ...,
            pipeline_name: builtins.str = ...,
            input_dataflows: collections.abc.Iterable[global___Dataflow] | None = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["dataset_name", b"dataset_name", "input_dataflows", b"input_dataflows", "pipeline_name", b"pipeline_name"]) -> None: ...

    DATASET_NAME_FIELD_NUMBER: builtins.int
    PIPELINE_DATAFLOW_FIELD_NUMBER: builtins.int
    dataset_name: builtins.str
    @property
    def pipeline_dataflow(self) -> global___Dataflow.PipelineDataflow: ...
    def __init__(
        self,
        *,
        dataset_name: builtins.str = ...,
        pipeline_dataflow: global___Dataflow.PipelineDataflow | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["dataset_name", b"dataset_name", "kind", b"kind", "pipeline_dataflow", b"pipeline_dataflow"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["dataset_name", b"dataset_name", "kind", b"kind", "pipeline_dataflow", b"pipeline_dataflow"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["kind", b"kind"]) -> typing_extensions.Literal["dataset_name", "pipeline_dataflow"] | None: ...

global___Dataflow = Dataflow

@typing_extensions.final
class PipelineLineages(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATASET_NAME_FIELD_NUMBER: builtins.int
    PIPELINE_NAME_FIELD_NUMBER: builtins.int
    INPUT_DATASETS_FIELD_NUMBER: builtins.int
    ACTIVE_FIELD_NUMBER: builtins.int
    dataset_name: builtins.str
    pipeline_name: builtins.str
    @property
    def input_datasets(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___DatasetLineages]: ...
    active: builtins.bool
    def __init__(
        self,
        *,
        dataset_name: builtins.str = ...,
        pipeline_name: builtins.str = ...,
        input_datasets: collections.abc.Iterable[global___DatasetLineages] | None = ...,
        active: builtins.bool = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["active", b"active", "dataset_name", b"dataset_name", "input_datasets", b"input_datasets", "pipeline_name", b"pipeline_name"]) -> None: ...

global___PipelineLineages = PipelineLineages

@typing_extensions.final
class DatasetPipelineLineages(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PIPELINE_LINEAGES_FIELD_NUMBER: builtins.int
    @property
    def pipeline_lineages(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___PipelineLineages]: ...
    def __init__(
        self,
        *,
        pipeline_lineages: collections.abc.Iterable[global___PipelineLineages] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["pipeline_lineages", b"pipeline_lineages"]) -> None: ...

global___DatasetPipelineLineages = DatasetPipelineLineages

@typing_extensions.final
class DatasetLineages(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SOURCE_DATASET_FIELD_NUMBER: builtins.int
    DERIVED_DATASET_FIELD_NUMBER: builtins.int
    source_dataset: builtins.str
    """If it is a source dataset, it will have a source dataset name."""
    @property
    def derived_dataset(self) -> global___DatasetPipelineLineages:
        """If it is a derived dataset, it will have pipeline lineages, one for each
        pipeline in the dataset.
        """
    def __init__(
        self,
        *,
        source_dataset: builtins.str = ...,
        derived_dataset: global___DatasetPipelineLineages | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["derived_dataset", b"derived_dataset", "kind", b"kind", "source_dataset", b"source_dataset"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["derived_dataset", b"derived_dataset", "kind", b"kind", "source_dataset", b"source_dataset"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["kind", b"kind"]) -> typing_extensions.Literal["source_dataset", "derived_dataset"] | None: ...

global___DatasetLineages = DatasetLineages
