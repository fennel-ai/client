# Changelog
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
