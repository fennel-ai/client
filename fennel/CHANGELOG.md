# Changelog

## [0.5.0] - 2023-02-01
- Make Sync call a REST call instead of gRPC 

## [0.4.2] - 2023-01-18
- Fix timestamps returned during a lookup. 

## [0.4.1] - 2023-01-18
- Log feautures to Kafka. 

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