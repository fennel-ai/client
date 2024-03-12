# Changelog

## [1.0.5] - 2024-03-012
- Introduce `spread` for S3 sources

## [1.0.4] - 2024-03-10
- Introduce `bounded` and `idleness` parameters to all sources

## [1.0.3] - 2024-03-08
- Add support for period in branch names. 

## [1.0.2] - 2024-03-08
- Allow only valid branch names.
- Display sync error correctly.

## [1.0.1] - 2024-03-05
- Make sasl info a required field for kafka sources

## [1.0.0] - 2024-03-01
- Add support for branches in Fennel.
- Change/Rename all client methods.
- Introduce versioning for datasets.

## [0.20.20] - 2024-02-14
- Make `cdc` and `disorder` non-optional fields on a data source with reasonable default values.
- remove `with_source` on the `@sources` decorator. This has been deprecated in favor using `tier=` on the data source
  itself.

## [0.20.19] - 2024-01-30
- Accept datetime as a valid value for fields in struct type

## [0.20.17] - 2024-01-30
- Allow `=` in the `path` parameter to the S3 source

## [0.20.16] - 2024-01-30
- Introduce `path` parameter to the S3 source

## [0.20.15] - 2024-01-30
- Improved casting to timestamp in case of epoch

## [0.20.14] - 2024-01-29
- Support `since` for all source types in the client
- Allow directly specifying a timestamp kinesis init_position instead of forcing the
  user to type `at_timestamp`

## [0.20.13] - 2024-01-26
- Deprecate JSONL as a standalone format. format=json should be used for
  newline-delimitted json

## [0.20.11] - 2024-01-24
- Enable JSONL (newline-delimitted json) as an allowed format for S3 

## [0.20.10] - 2024-01-22
- Added capability in lookup method in client to support as-of lookups on keyed datasets.

## [0.20.9] - 2024-01-22
- Add support for diff summaries and printing detailed diffs on error.

## [0.20.8] - 2024-01-22
- Do data casting in log only and do schema validation in assign

## [0.20.7] - 2024-01-22
- Fix bug in explode operator in the mock client to ignore index of the dataframe. This mimics the behavior of our
    backend engine.

## [0.20.6] - 2024-01-22
- Improved error reporting in case of invalid sources.

## [0.20.5] - 2024-01-19
- Support `starting_from` on Snowflake data source

## [0.20.3] - 2024-01-18
- Mock client raises an exception rather than returning 400 response in case of a failure.

## [0.20.3] - 2024-01-17

- Upddated the Kinesis source interface for specifying the initial ShardIterator type

## [0.20.2] - 2024-01-17

- Added window operator

## [0.20.1] - 2024-01-16

- Add validations for the explode operator

## [0.20.0] - 2024-01-12

- Adding functions in the client -> `extract`, `extract_historical` and `extract_historical_progress`.
- Deprecating functions in the client -> `extract_features`, `extract_historical_features`
  and `extract_historical_progress_features`.

## [0.19.8] - 2024-01-11

- Improved error reporting in case of joint operator failure in Mock Client.

## [0.19.7] - 2024-01-09

- Add jsonl file format support in pb2 file, introduce disorder for Sources

## [0.19.6] - 2024-01-08

- Add default format json for Kinesis Source

## [0.19.5] - 2024-01-03

- Updates the contract between client and the server for `extract_historical_features` s3 buckets

## [0.19.4] - 2023-12-06

- Bug fix for `preproc` on `@sources` for string and bool data types.

## [0.19.3] - 2023-12-06

- Rename `pre_proc` to `preproc` on `@sources`.

## [0.19.2] - 2023-12-05

- Add support for `pre_proc` on `@sources` to specify default values for columns which may not exist in the data
  sources.

## [0.19.0] - 2023-11-28

- Allow for AWS access key credentials for extract_historical_features buckets

## [0.18.21] - 2023-11-18

- Fix error response being logged twice in the client for sync and extract_features.

## [0.18.15] - 2023-11-12

- Allow owner to be specified at the file level itself.

## [0.18.14] - 2023-11-11

- Use pd types rather than python types

## [0.18.12] - 2023-11-08

- Add support for strings in extract_features and extract_historical_features

## [0.18.11] - 2023-11-08

- Add support for tier selectors.

## [0.18.10] - 2023-10-30

- Added `preview` parameter to sync.
- Show entity diffs on sync.

## [0.18.10] - 2023-10-30

- Add support for `since` in S3 source.

## [0.18.9]- 2023-10-27

- Added support for datetime fields in struct types

## [0.18.6] - 2023-09-19

- Added assign operator

## [0.18.5] - 2023-09-19

- Added select operator

## [0.18.4] - 2023-09-22

- Minor proto update for derived lookup extractors

## [0.18.2] - 2023-09-13

- Add derived extractors for aliasing and lookups

## [0.18.1] - 2023-09-08

- Add support to specify output bucket and prefix for extract historical, and support to map output columns to different
  features.

## [0.18.0] - 2023-08-30

- Added support for Debezium data in Avro format via Kafka connector

## [0.17.8] - 2023-08-17

- Added support for distinct aggregate in the backend

## [0.17.7] - 2023-08-17

- Distinct type for aggregations

## [0.17.3] - 2023-08-10

- Bug fixes for columnar serialization

## [0.17.2] - 2023-08-10

- Bug fixes for columnar serialization

## [0.17.1] - 2023-08-04

- Dataframes are serialized in columnar format for extractors

## [0.17.0] - 2023-08-03

- Remove jaas config from kafka source
- Fix kafka validation for security protocol
- Add delta lake for S3 format
- Fix validation bug

## [0.16.19] - 2023-08-02

- Support for 'stddev' aggregate

## [0.16.18] - 2023-07-21

- Fix for 'first' operator

## [0.16.17] - 2023-07-21

- Support for 'first' operator

## [0.16.16] - 2023-07-20

- Better errors in mock client

## [0.16.15] - 2023-07-20

- Support struct type

## [0.16.12] - 2023-07-20

- Support verify_cert on kafka source

## [0.16.11] - 2023-07-20

- Support role-based access to s3 data

## [0.16.10] - 2023-07-18

- Bug fixes for the client.

## [0.16.6] - 2023-07-08

- Support count unique for aggregations

## [0.16.5] - 2023-07-07

- Fix get_dataset_df in mock client to correctly handle empty datasets

## [0.16.4] - 2023-07-06

- Mock client fix to handle empty data in aggegations

## [0.16.3] - 2023-07-06

- Client API's for extract historical

## [0.16.2] - 2023-06-27

- Enable versioning for expectations.

## [0.16.1] - 2023-06-27

- Add inspect APIs to the client.

## [0.16.0] - 2023-06-19

- New `explode` and `dedup` operators
- Support for inner joins

## [0.15.21] - 2023-06-16

- Add definition APIs to the client.

## [0.15.20] - 2023-06-13

- Pass timestamp sorting column to the source.

## [0.15.19] - 2023-06-04

- Send pipeline source code during sync.

## [0.15.18] - 2023-06-01

- Support chaining of operators for lambda functions.

## [0.15.17] - 2023-06-01

- Create schema copy for every node.

## [0.15.16] - 2023-06-01

- Add support for longer complex lambda functions.

## [0.15.15] - 2023-05-26

- Improve error handling of drop/rename operators
- Require `default` for min/max aggregates

## [0.15.14] - 2023-05-30

- Add support for kinesis source.

## [0.15.13] - 2023-05-30

- Change ownership of expectations based on pipelines for derived datasets.

## [0.15.11] - 2023-05-26

- Fix join semantics for left and right joins.

## [0.15.10] - 2023-05-23

- Add debug api to the client

## [0.15.7] - 2023-05-19

- Increase client timeout for sync to 300s.

## [0.15.5] - 2023-05-18

- Provide an option to override the source for a dataset.

## [0.15.3] - 2023-05-15

- Provide schema for every node

## [0.15.2] - 2023-05-13

- Fixes for Webhook + Integration tests

## [0.15.1] - 2023-05-12

- Support backfilling of pipelines + Webhook support

## [0.15] - 2023-05-11

- Vendor in dependencies of the client

## [0.12] - 2023-04-17

- Add support for intervals in Join Operator

## [0.11] - 2023-04-05

- Use new lambda based execution.

## [0.9.1] - 2023-04-03

- Use source code based execution.

## [0.8.7] - 2023-03-29

- Change from type to decorator based input and output specification for pipelines and extractors

## [0.8.6] - 2023-03-21

- Add rename and drop operators to the client.

## [0.8.5] - 2023-03-21

- Improve client error reporting.

## [0.8.4] - 2023-03-20

- Port docs to client.

## [0.8.3] - 2023-03-20

- Some minor bug fixes

## [0.8.2] - 2023-03-17

- Kafka source support.

## [0.8.1] - 2023-03-15

- Add back support for great expectations

## [0.8.0] - 2023-03-12

- Large rewrite of protobufs and the interfaces and a bunch of backward incompatible changes.

## [0.7.1] - 2023-02-22

- Disallow featuresets as inputs to extractors

## [0.7.0] - 2023-02-02

- Introduce great expectations

## [0.6.1] - 2023-02-10

- Improve error propagation for the user

## [0.6.0] - 2023-02-10

- Update in generated protobuf files and translators

## [0.5.1] - 2023-02-02

- Client side schema check on feature extraction.

## [0.5.0] - 2023-02-01

- Make Sync call a REST call instead of gRPC

## [0.4.2] - 2023-01-18

- Fix timestamps returned during a lookup.

## [0.4.1] - 2023-01-18

- Log features to Kafka.

## [0.4.0] - 2023-01-18

- Pipelines have ids and extractors have versions.

## [0.3.7] - 2023-01-17

- Several updates to the api's and keeping them in sync with the documentation.

## [0.3.4] - 2023-01-15

- log splits the input dataframe into smaller batches to avoid potential payload size limit or timeouts.

## [0.3.3] - 2023-01-15

- revert log call splitting the input json. to_json returns a JSON string, can't batch that.

## [0.3.2] - 2023-01-15

- log splits the input dataframe into smaller batches to avoid potential payload size limit or timeouts.

## [0.3.1] - 2023-01-15

- Pickle function module by value.

## [0.3.0] - 2023-01-14

- Fix aggregate execution.

## [0.2.9] - 2023-01-12

- Pipeline schema validation.

## [0.2.8] - 2023-01-11

- Dynamic import to support integration client

## [0.2.6] - 2023-01-10

- Pickle by reference and make extractors bounded functions.

## [0.2.5] - 2023-01-10

- Provide string representation of fields and features.

## [0.2.3] - 2023-01-02

- Use dataset field names rather than strings in pipelines.

## [0.2.2] - 2023-01-02

- Added error checks covering cases when extractor depends on incorrect dataset.

## [0.2.1] - 2022-12-29

- Enable featuresets as inputs to an extractor

## [0.2.0] - 2022-12-27

- Preserve ordering of key lookup

## [0.1.9] - 2022-12-23

- Schema must be specified if transform changes it.

## [0.1.7] - 2022-12-12

- Migrate from py arrow schemas to inhouse schemas
