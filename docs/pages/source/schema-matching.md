---
title: 'Schema Matching'
order: 0
status: 'published'
---

# Schema Matching

Fennel has a strong typing system and all ingested data is matched against
the dataset schema. See [Fennel types](/api-reference/data-types) to see the 
full list of types supported by Fennel. 

### Fennel Schema As Subset of Source Data

All the fields defined in Fennel dataset must be present in your source dataset
though it's okay if your external data source has additional fields - they are 
simply ignored.  

Not only all fields must be present, they must be of the right types (or be 
convertible into right types via some parsing/casting). If ingested data 
doesn't match with the schema of the Fennel dataset, the data is discarded and 
not admitted to the dataset. Fennel maintains logs of how often it happens and 
it's possible to set alerts on that.


Here is how various types are matched from the sourced data (note that type 
matching/casting only happens during sourcing - once data has been parsed in 
the given type, from then onwards, there is no type casting/coercion anywhere 
else in Fennel)

* `int`, `float`, `str`, `bool` respectively match with any integer types, float
 types, string types and boolean types. For instance, Fennel's `int` type
 matches with INT8 or UINT32 from Postgres.
* `List[T]` matches a list of data of type T.
* `Dict[T]` matches any dictionary from strings to values of type T.
* `Option[T]` matches if either the value is `null` or if its non-null value
  matches type T. Note that `null` is an invalid value for any non-Option types.
* `struct` is similar to dataclass in Python or struct in compiled languages
  and matches JSON data with appropriate types recursively.
* `datetime` matching is a bit more flexible to support multiple common
  data formats to avoid bugs due to incompatible formats. Fennel is able to
  safely parse datetime from all the following formats.


### Datetime Formats

* Integers that describe timestamp as interval from Unix epoch e.g. `1682099757`
 Fennel is smart enough to automatically deduce if an integer is describing
 timestamp as seconds, milliseconds, microseconds or nanoseconds
* Strings that are decimal representation of an interval from Unix epoch e.g.`"1682099757"`
* Strings describing timestamp in [RFC 3339](https://www.ietf.org/rfc/rfc3339.txt)
  format e.g. `'2002-10-02T10:00:00-05:00'` or `'2002-10-02T15:00:00Z'` or `'2002-10-02T15:00:00.05Z'`
* Strings describing timestamp in [RFC2822](https://www.ietf.org/rfc/rfc2822.txt)
 format e.g. `'Sun, 23 Jan 2000 01:23:45 JST'`
* When data is being read from arrow representations (say parquet files), `date32`
  and `date64` types are also converted to datetime (with time being UTC midnight)
* For any of these methods, if time zone isn't provided explicitly, time is 
  assumed be in UTC timezone.


### Preproc

Sometimes, the external data sources doesn't have the 'column' corresponding to
the field in your Fennel dataset. Fennel allows you to handle these scenarios
by specying `preproc` property on your sources (short for pre-processing). Let's
look at an example: 