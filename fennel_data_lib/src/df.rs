use std::io::{Read, Write};
use std::sync::Arc;

use anyhow::{anyhow, Ok, Result};
use arrow::array::ArrayRef;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use csv::StringRecord;
use itertools::Itertools;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use pl::Expr as PolarsExpr;
use polars::frame::DataFrame as PLDF;
use polars::lazy::dsl as pl;
use polars::prelude::IntoLazy;
use polars::series::Series;
use serde_json::Map;
use serde_json::Value as JsonValue;

use crate::arrow_lib::{from_arrow_array, from_arrow_recordbatch, to_arrow_array};

use crate::expr::EvalContext;
use crate::{
    expr::CompiledExpr,
    rowcol::{Col, Row, RowCodec},
    schema::{Field, Schema},
    types::Type,
    value::Value,
};

/// Dataframe is a collection of rows and columns.
/// It currently stores data in column-major format but
/// can be extended to support row-major format as well.
#[derive(Debug, Clone)]
pub struct Dataframe {
    schema: Arc<Schema>,
    cols: Vec<Col>,
}

impl Dataframe {
    /// Create a new dataframe from the given columns
    /// Returns error if some columns have the same name
    pub fn new(cols: Vec<Col>) -> Result<Self> {
        let schema = Arc::new(Schema::new(
            cols.iter()
                .map(|c| c.field().clone())
                .collect::<Vec<Field>>(),
        )?);
        Ok(Dataframe { schema, cols })
    }

    /// Given a schema and a list of rows, create a dataframe.
    /// Throws an error if any of the rows does not match the schema.
    /// NOTE: this is a very expensive operation so use it sparingly.
    pub fn from_rows(schema: Arc<Schema>, rows: &[Row]) -> Result<Self> {
        for row in rows {
            if row.schema().ne(&schema) {
                return Err(anyhow!(
                    "cannot convert rows to columns: row schema {:?} does not match dataframe schema {:?}",
                    row.schema(),
                            schema,
                ));
            }
        }
        let mut cols = vec![None; schema.len()];
        let mut iters = rows
            .iter()
            .map(|r| r.schema().sorted_fields())
            .collect::<Vec<_>>();
        for (target, field) in schema.sorted_fields() {
            let mut data = Vec::with_capacity(rows.len());
            for (iter, row) in iters.iter_mut().zip(rows.iter()) {
                let (source, _) = iter.next().unwrap();
                data.push(row.at(source).unwrap().clone());
            }
            cols[target] = Some(Col::new(Arc::new(field.clone()), Arc::new(data))?);
        }
        // all columns should be present, so unwrap
        let cols = cols.into_iter().map(|c| c.unwrap()).collect();
        Ok(Dataframe::new(cols)?)
    }

    /// Given a schema and a json array ot object, parse the json and create a dataframe.
    /// Json arrays are parsed row wise (list of dicts) and json objects are parsed column wise (dict of lists)
    /// Returns error if the json is not a list of dictionaries or if the json does not match the schema.
    pub fn from_json(schema: Arc<Schema>, json: &JsonValue) -> Result<Self> {
        match json {
            JsonValue::Array(json_rows) => {
                Self::from_json_rows(schema, json_rows)
            }
            JsonValue::Object(json_cols) => {
                let cols = json_cols
                    .iter()
                    .filter_map(|(name, json_col)| {
                        match schema.find(name) {
                            Some(field) => Some(Col::from_json_parsed(Arc::new(field.clone()), json_col)),
                            None => None,
                        }
                    })
                    .collect::<Result<Vec<Col>>>()?;
                let df = Dataframe::new(cols)?;
                for field in schema.fields() {
                    if df.col(field.name()).is_none() {
                        return Err(anyhow!("can not convert json to dataframe: json does not contain column {}", field.name()));
                    }
                }
                Ok(df)
            }
            _ => Err(anyhow!("can not convert json to dataframe: json must be a list of dictionaries or a dictionary of columns")),
        }
    }

    // Schema and records should have same number of fields and in the same order.
    pub fn from_csv(records: Vec<StringRecord>, schema: Arc<Schema>) -> Result<Self> {
        let mut rows: Vec<Row> = Vec::with_capacity(records.len());
        for record in records {
            if schema.fields().len() != record.len() {
                return Err(anyhow!(
                    "number of columns in csv record {} does not match schema {}",
                    record.len(),
                    schema.fields().len()
                ));
            }
            let mut row: Vec<Value> = Vec::with_capacity(schema.fields().len());
            for (field, csv_value) in schema.fields().iter().zip(record.iter()) {
                if *field.dtype() == Type::String {
                    row.push(Value::String(Arc::new(csv_value.to_string())));
                    continue;
                }
                let json_val = Value::from_json_parsed(
                    field.dtype(),
                    &JsonValue::String(csv_value.to_string()),
                )
                .map_err(|e| {
                    anyhow!(
                        "failed to convert csv value {} to json value for type {:?}: {}",
                        csv_value,
                        field.dtype(),
                        e
                    )
                })?;
                row.push(json_val);
            }
            rows.push(Row::new(schema.clone(), Arc::new(row))?);
        }
        Dataframe::from_rows(schema, &rows)
    }

    pub fn from_rb_reader(
        schema: Arc<Schema>,
        mut reader: ParquetRecordBatchReader,
    ) -> Result<Self> {
        let mut dataframes = Vec::new();
        while let Some(batch) = reader.next().transpose()? {
            let df = from_arrow_recordbatch(schema.clone(), &batch)?;
            dataframes.push(df);
        }
        return Dataframe::concat(schema, &dataframes);
    }

    pub fn from_parquet_bytes(schema: Arc<Schema>, bytes: Bytes) -> Result<Self> {
        let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .map_err(|err| anyhow!("failed to create parquet reader: {:?}", err))?
            .build()
            .map_err(|err| anyhow!("failed to create parquet reader: {:?}", err))?;
        Dataframe::from_rb_reader(schema, arrow_reader)
    }

    // Schema can be a subset of data in JsonValue
    pub fn from_json_rows(schema: Arc<Schema>, json_rows: &[JsonValue]) -> Result<Self> {
        let rows = json_rows
            .iter()
            .map(|json_row| Row::from_json_parsed(schema.clone(), &json_row))
            .collect::<Result<Vec<Row>>>()?;
        Dataframe::from_rows(schema, &rows)
    }

    /// to_json converts the dataframe to a JSON Value constructing a JSON object with the column names as keys.
    pub fn to_json(&self) -> Result<JsonValue> {
        let mut json_cols = Map::with_capacity(self.num_cols());
        for col in self.cols() {
            json_cols.insert(String::from(col.name()), col.to_json()?);
        }
        Ok(JsonValue::Object(json_cols))
    }

    pub fn to_csv_string(&self) -> String {
        let headers = self.cols.iter().map(|col| col.name()).join(",");
        let values = self
            .rows()
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(|value| value.to_string().replace(',', "; "))
                    .join(",")
            })
            .join("\n");
        format!("{headers}\n{values}")
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    pub fn cols(&self) -> &[Col] {
        &self.cols
    }

    /// Return the first column with the given name.
    pub fn col(&self, name: &str) -> Option<&Col> {
        self.cols.iter().find(|c| c.name() == name)
    }

    /// Given a schema and a list of columns, clone the data into rows oriented format.
    /// NOTE: this is a very expensive operation so use it sparingly.
    // TODO: make this return an iterator
    pub fn rows(&self) -> Vec<Row> {
        if self.num_rows() == 0 {
            return vec![];
        }
        let indices = (0..self.num_rows()).collect::<Vec<usize>>();
        self.index(&indices).unwrap()
    }

    /// Returns a new dataframe with the given columns removed
    /// Error is returned if any of the columns is not found.
    pub fn strip(&self, names: &[&str]) -> Result<Self> {
        let colnames = self.colnames().collect::<Vec<&str>>();
        for n in names.iter() {
            if !colnames.contains(n) {
                return Err(anyhow!("can not drop column: column {} is not found", n));
            }
        }
        let mut new_cols = Vec::with_capacity(self.cols.len());
        for col in self.cols.iter() {
            if !names.contains(&col.name()) {
                new_cols.push(col.clone());
            }
        }
        Ok(Dataframe::new(new_cols)?)
    }

    /// Returns only the rows at the given indices.
    /// Returns error if any of the indices is out of bounds.
    /// Note: this is a very expensive operation so use it sparingly.
    // TODO: make this return an iterator
    pub fn index(&self, indices: &[usize]) -> Result<Vec<Row>> {
        let mut rows = Vec::with_capacity(indices.len());
        let cols = self.cols();
        for &i in indices {
            if i >= self.num_rows() {
                return Err(anyhow!(
                    "can not index dataframe: index {} is out of bounds",
                    i
                ));
            }
            let mut values = Vec::with_capacity(cols.len());
            for col in cols.iter() {
                values.push(col.index(i).unwrap().clone());
            }
            rows.push(Row::new(self.schema.clone(), Arc::new(values))?);
        }
        Ok(rows)
    }

    /// Return an iterator over the column names.
    pub fn colnames(&self) -> impl Iterator<Item = &str> {
        self.schema().fields().iter().map(|f| f.name())
    }

    /// Returns a new dataframe where columns are renamed.
    /// If some columns in old are not found, they are ignored.
    /// Returns error if after renaming there are columns with the same name.
    pub fn rename(&self, old: &[&str], new: &[&str]) -> Result<Self> {
        if old.len() != new.len() {
            return Err(anyhow!(
                "can not rename columns: old and new names have different lengths"
            ));
        }
        let mut new_cols = Vec::with_capacity(self.cols.len());
        for col in self.cols.iter().cloned() {
            let col = match old.iter().position(|&o| o == col.name()) {
                Some(idx) => col.rename(new[idx].to_string())?,
                None => col,
            };
            new_cols.push(col);
        }
        Ok(Dataframe::new(new_cols)?)
    }

    /// Append a column to the dataframe, returning a new dataframe.
    pub fn append(&self, col: Col) -> Result<Self> {
        let schema = self.schema();
        if schema.find(col.name()).is_some() {
            return Err(anyhow!(
                "can not append to dataframe: column name {:?} already exists in dataframe schema {:?}",
                col.name(),
                self.schema()
            ));
        }
        let mut cols = self.cols.clone();
        cols.push(col);
        Ok(Self::new(cols)?)
    }

    /// Stitch two dataframes together. Returns an error if the number of
    /// rows in each dataframe is not equal or if both dataframes contains
    /// columns with the same name.
    pub fn stitch(&self, other: &Self) -> Result<Self> {
        // If one of the dataframes is empty, return the other.
        if other.num_cols() == 0 {
            return Ok(self.clone());
        } else if self.num_cols() == 0 {
            return Ok(other.clone());
        }
        // If both the dataframes have some columns, they should have the same number of rows.
        if self.num_rows() != other.num_rows() {
            return Err(anyhow!(
                "can not stitch dataframes: number of rows in each dataframe must be equal"
            ));
        }
        // Verify that the schemas can be stitched.
        let _ = self.schema().as_ref().stitch(other.schema().as_ref())?;
        // Construct a new dataframe with the columns from both dataframes.
        let cols = self
            .cols()
            .iter()
            .chain(other.cols().iter())
            .cloned()
            .collect();
        Ok(Dataframe::new(cols)?)
    }

    /// Returns a dataframe with only the specified fields.
    /// Returns an error if any of the fields are not found in the dataframe.
    ///
    /// NOTE: ordering of names is preserved
    pub fn project(&self, names: &[impl AsRef<str>]) -> Result<Dataframe> {
        for name in names {
            if self.schema.find(name.as_ref()).is_none() {
                return Err(anyhow!(
                    "can not project dataframe: field name {:?} not found in dataframe schema {:?}",
                    name.as_ref(),
                    self.schema
                ));
            }
        }

        let cols = names
            .iter()
            .map(|n| self.select(n.as_ref()).unwrap().clone())
            .collect::<Vec<Col>>();

        Ok(Dataframe::new(cols)?)
    }

    /// Returns the column with the specified name or none if not found.
    pub fn select(&self, name: &str) -> Option<&Col> {
        self.cols().iter().find(|c| c.name() == name)
    }

    /// Returns the number of rows in the dataframe.
    pub fn num_rows(&self) -> usize {
        if self.cols.is_empty() {
            return 0;
        }
        self.cols[0].len()
    }

    /// Returns the number of columns in the dataframe.
    pub fn num_cols(&self) -> usize {
        self.cols.len()
    }

    /// Returns a new dataframe with the given dataframe stacked on top of this dataframe.
    /// Returns an error if the schemas are not equal.
    /// This requires a full copy of the data and is an expensive operation.
    pub fn stack(&self, other: &Self) -> Result<Self> {
        if self.schema() != other.schema() {
            return Err(anyhow!("can not stack dataframes: schemas must be equal"));
        }
        let mut cols = Vec::with_capacity(self.num_cols());
        for c1 in self.cols() {
            // can unwrap because we already checked the schema equality
            let c2 = other.select(c1.name()).unwrap();
            let mut col = c1.clone();
            col.extend(c2)?;
            cols.push(col);
        }
        Ok(Dataframe::new(cols)?)
    }

    pub fn empty(schema: Arc<Schema>) -> Self {
        Dataframe::from_rows(schema, &[]).unwrap()
    }

    /// Returns a new dataframe with the given dataframes concatenated horizontally
    /// This function is an extension to stack.
    pub fn concat(schema: Arc<Schema>, dataframes: &[Dataframe]) -> Result<Dataframe> {
        if dataframes.is_empty() {
            return Ok(Dataframe::empty(schema));
        }
        let mut cols = Vec::with_capacity(schema.len());
        for f in schema.fields() {
            let mut values = vec![];
            for df in dataframes.iter() {
                let c = df.select(f.name()).ok_or_else(|| {
                    anyhow!(
                        "can not concat dataframes: column name {:?} not found in dataframe schema {:?}",
                        f.name(),
                        df.schema()
                    )
                })?;
                values.extend(c.values().to_owned());
            }
            let col = Col::new(Arc::new(f.clone()), Arc::new(values))?;
            cols.push(col);
        }
        Ok(Dataframe::new(cols)?)
    }

    /// Returns a hash of each row in the dataframe.
    pub fn hash(&self) -> Vec<u64> {
        self.rows().iter().map(|r| r.hash()).collect()
    }

    /// Returns an ordered value hash (not depending on type and name) of each row in the dataframe.
    pub fn value_hash(&self) -> anyhow::Result<Vec<u64>> {
        self.rows().iter().map(|r| r.value_hash()).collect()
    }

    /// Returns a new dataframe replacing the column with given name with the given column.
    pub fn replace(&self, name: &str, col: Col) -> Result<Self> {
        let mut cols = self.cols.clone();
        let idx = cols.iter().position(|c| c.name() == name).ok_or_else(|| {
            anyhow!(
                "can not replace column: column name {:?} not found in dataframe schema {:?}",
                name,
                self.schema()
            )
        })?;
        cols[idx] = col;
        Ok(Self::new(cols)?)
    }

    /// Writes the dataframe to the given writer in an efficient binary format.
    /// This does not write the schema, only the data and hence the schema
    /// must be known when reading the dataframe.
    pub fn encode(&self, mut writer: &mut impl Write) -> Result<()> {
        // write schema hash and codec version in 2 bytes
        // followed by writing each of the columns one by one
        let h = self.schema.hash();
        let lower = (h & 0x1fff) as u16; // lower 13 bits
        let header = lower << 3 | RowCodec::V1_13bithash as u16;
        writer.write_u16::<BigEndian>(header)?;
        // sort columns by name and write them
        let mut cols = self.cols.clone();
        cols.sort_by(|a, b| a.name().cmp(b.name()));
        for col in cols {
            col.encode(&mut writer)?;
        }
        Ok(())
    }

    /// Reads a dataframe from the given reader that was previously written
    /// using the `encode` method.
    pub fn decode(schema: Arc<Schema>, mut reader: &mut impl Read) -> Result<Self> {
        let header: u16 = reader.read_u16::<BigEndian>()?;
        if header & 0b111 != (RowCodec::V1_13bithash as u16) {
            panic!(
                "unable to decode dataframe: codec mismatch: expected {:?}, got {}",
                RowCodec::V1_13bithash,
                header & 0b111
            );
        }
        let schema_hash: u16 = header >> 3;
        let expected = (schema.hash() & 0x1fff) as u16;
        if schema_hash != expected {
            panic!(
                "unable to decode dataframe: schema hash mismatch: expected {}, got {}",
                expected, schema_hash
            );
        }

        // sort fields in schema by name and read them one by one
        let mut fields = schema.fields().to_vec();
        fields.sort_by(|a, b| a.name().cmp(b.name()));
        let mut cols = Vec::with_capacity(fields.len());
        for field in fields {
            let col = Col::decode(field.name(), field.dtype().clone(), &mut reader)?;
            cols.push(col);
        }
        Ok(Self::new(cols)?)
    }

    pub fn filter(&self, cexpr: &CompiledExpr, ctx: Option<EvalContext>) -> Result<Self> {
        let col = self.eval(cexpr, Type::Bool, "__@@_filter_", ctx)?;
        let mut indices = vec![];
        for (i, v) in col.values().iter().enumerate() {
            if v.as_bool().unwrap() {
                indices.push(i);
            }
        }
        let rows = self.index(&indices)?;
        Ok(Dataframe::from_rows(self.schema.clone(), &rows)?)
    }

    pub fn assign(
        &self,
        cexpr: &CompiledExpr,
        name: &str,
        dtype: Type,
        ctx: Option<EvalContext>,
    ) -> Result<Self> {
        let col = self.eval(cexpr, dtype, name, ctx)?;
        Ok(self.append(col)?)
    }

    /// Evaluate the given expression and return the result as a new column.
    /// The output type of the expression must match the given type.
    pub fn eval(
        &self,
        cexpr: &CompiledExpr,
        outtype: Type,
        name: impl Into<String>,
        ctx: Option<EvalContext>,
    ) -> Result<Col> {
        if !cexpr.matches(&outtype) {
            return Err(anyhow!(
                "expression {} does not evaluate to expected type '{:?}'",
                cexpr,
                outtype
            ));
        }
        let index_name = "__@@_index_";
        // convert expr to polars expression and df to polars dataframe
        // also convert schema/dataframe to arrow schema/recordbatch
        let ctx = ctx
            .unwrap_or_else(|| EvalContext::new(None))
            .with_index_col_name(index_name.to_string());
        let plexpr: PolarsExpr = cexpr.into_polar_expr(Some(ctx)).map_err(|err| {
            anyhow!(
                "failed to convert compiled expression to polars expression: {}",
                err
            )
        })?;
        let plexpr = plexpr.alias("__@@_eval_");
        let pl_array = {
            let mut pl_series = vec![];
            for col in self.cols() {
                // go from our col -> arrow array -> polars series (last change is zero copy)
                let array = to_arrow_array(col.values(), col.dtype(), false)?;
                let array = Box::<dyn polars_arrow::array::Array>::from(&*array);
                let series = Series::from_arrow(col.name(), array)?;
                pl_series.push(series);
            }
            let pl_df = PLDF::new(pl_series)?.with_row_index(index_name, None)?;
            let result = pl_df.lazy().with_column(plexpr).collect();
            let pl_df = match result {
                Result::Ok(df) => df,
                Err(e) => {
                    let errstr = format!("{}", e);
                    // strip off all text after the first newline
                    // we do this because polars sometimes gives suggestions of
                    // alternative APIs to use which are not relevant to us
                    let errstr = errstr.split('\n').next().unwrap_or(&errstr);
                    anyhow::bail!("failed to eval expression: {}, error: {}", cexpr, errstr);
                }
            };
            // setting pl_flavor tells polars to not rely on its 'View' types
            pl_df.column("__@@_eval_")?.to_arrow(0, false)
        };
        // pl_array -> arrow array -> our col (first change is zero copy)
        let array = ArrayRef::from(pl_array);
        let values = from_arrow_array(&array, &outtype, false);
        if values.is_err() {
            anyhow::bail!(
                "failed to convert polars array of type {:?} to fennel array for type '{:?}' due to error: {:?}\nfull array: {:?}",
                array.data_type(),
                outtype,
                values.err().unwrap(),
                array
            )
        }
        Ok(Col::new(
            Arc::new(Field::new(name.into(), outtype)),
            Arc::new(values.unwrap()),
        )?)
    }
}

impl PartialEq for Dataframe {
    fn eq(&self, other: &Self) -> bool {
        if self.schema.ne(other.schema()) {
            return false;
        }
        for c1 in self.cols() {
            // can unwrap because we already checked the schema equality
            let c2 = other.select(c1.name()).unwrap();
            if c1.ne(c2) {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        types::Type,
        value::{UTCTimestamp, Value},
    };

    use super::*;

    #[test]
    fn test_dataframe_basic() {
        let cols = vec![
            Col::new(
                Arc::new(Field::new("a".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b".to_string(), Type::Bool)),
                Arc::new(vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                ]),
            )
            .unwrap(),
        ];
        let df = Dataframe::new(cols.clone()).unwrap();
        assert_eq!(df.num_rows(), 3);
        assert_eq!(df.num_cols(), 2);
        assert_eq!(vec!["a", "b"], df.colnames().collect::<Vec<_>>());
        assert_eq!(
            &Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ])
            .unwrap(),
            df.schema().as_ref(),
        );
        assert_eq!(
            &vec![Value::Int(1), Value::Int(2), Value::Int(3)],
            df.select("a").unwrap().values(),
        );
        assert_eq!(
            &vec![Value::Bool(true), Value::Bool(false), Value::Bool(true)],
            df.select("b").unwrap().values(),
        );
        assert_eq!(None, df.select("c"));
        assert_eq!(&cols[0], df.select("a").unwrap());
    }

    #[test]
    fn test_rename() {
        let df1 = Dataframe::new(
            vec![
                Col::new(
                    Arc::new(Field::new("a".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                )
                .unwrap(),
                Col::new(
                    Arc::new(Field::new("b".to_string(), Type::Bool)),
                    Arc::new(vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                    ]),
                )
                .unwrap(),
            ]
            .clone(),
        )
        .unwrap();
        assert_eq!(2, df1.num_cols());
        // df1 should not be changed after rename
        let df2 = df1.rename(&["a"], &["c"]).unwrap();
        assert_eq!(vec!["a", "b"], df1.colnames().collect::<Vec<_>>());
        assert_eq!(vec!["c", "b"], df2.colnames().collect::<Vec<_>>());
        assert_eq!(
            df1.select("a").unwrap().values(),
            df2.select("c").unwrap().values()
        );
        assert_eq!(
            df1.select("a").unwrap().dtype(),
            df2.select("c").unwrap().dtype()
        );
        assert_eq!(df1.select("b").unwrap(), df2.select("b").unwrap());

        // it's ok to give names that are not in the dataframe, even if they are being
        // created by the rename operation
        let df3 = df1.rename(&["a", "c"], &["c", "d"]).unwrap();
        assert_eq!(df2, df3);

        // rename fails if the number of old and new names are not the same
        df1.rename(&["a", "b"], &["c", "d", "e"]).unwrap_err();
        df1.rename(&["a", "b", "c"], &["d", "e"]).unwrap_err();

        // rename fails if names are duplicated after the rename
        df1.rename(&["a", "x"], &["b", "y"]).unwrap_err();
    }

    #[test]
    fn test_hash() {
        let df = Dataframe::new(vec![
            Col::new(
                Arc::new(Field::new("a".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b".to_string(), Type::Bool)),
                Arc::new(vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                ]),
            )
            .unwrap(),
        ])
        .unwrap();
        assert_eq!(
            df.hash(),
            vec![
                12978725723221278546,
                11843051563920734634,
                2373189351219591826,
            ]
        );
    }

    #[test]
    fn test_replace() {
        let df1 = Dataframe::new(
            vec![
                Col::new(
                    Arc::new(Field::new("a".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                )
                .unwrap(),
                Col::new(
                    Arc::new(Field::new("b".to_string(), Type::Bool)),
                    Arc::new(vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                    ]),
                )
                .unwrap(),
            ]
            .clone(),
        )
        .unwrap();
        assert_eq!(2, df1.num_cols());
        let new_df = df1
            .replace(
                "b",
                Col::new(
                    Arc::new(Field::new("bb".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(4), Value::Int(5), Value::Int(6)]),
                )
                .unwrap(),
            )
            .unwrap();
        // df1 is not affected by replace
        assert_eq!(2, df1.num_cols());
        assert_eq!(3, df1.num_rows());
        assert_eq!(vec!["a", "b"], df1.colnames().collect::<Vec<_>>());

        // df2 has the same number of rows as df1
        assert_eq!(3, new_df.num_rows());
        // df2 has the same number of columns as df1
        assert_eq!(2, new_df.num_cols());
        assert_eq!(vec!["a", "bb"], new_df.colnames().collect::<Vec<_>>());

        // bad replace attempts fail
        {
            // length is off
            df1.replace(
                "c",
                Col::new(
                    Arc::new(Field::new("bb".to_string(), Type::Int)),
                    Arc::new(vec![
                        Value::Int(4),
                        Value::Int(5),
                        Value::Int(6),
                        Value::Int(7),
                    ]),
                )
                .unwrap(),
            )
            .unwrap_err();
        }
        {
            // column name not found
            df1.replace(
                "c",
                Col::new(
                    Arc::new(Field::new("bb".to_string(), Type::Int)),
                    Arc::new(vec![
                        Value::Int(4),
                        Value::Int(5),
                        Value::Int(6),
                        Value::Int(7),
                    ]),
                )
                .unwrap(),
            )
            .unwrap_err();
        }
    }

    #[test]
    fn test_project_append_drop() {
        let df1 = Dataframe::new(
            vec![
                Col::new(
                    Arc::new(Field::new("a".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                )
                .unwrap(),
                Col::new(
                    Arc::new(Field::new("b".to_string(), Type::Bool)),
                    Arc::new(vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                    ]),
                )
                .unwrap(),
            ]
            .clone(),
        )
        .unwrap();
        assert_eq!(2, df1.num_cols());
        let df2 = df1
            .append(
                Col::new(
                    Arc::new(Field::new("c".to_string(), Type::Float)),
                    Arc::new(vec![
                        Value::Float(1.0),
                        Value::Float(2.0),
                        Value::Float(3.0),
                    ]),
                )
                .unwrap(),
            )
            .unwrap();
        assert_eq!(3, df2.num_cols());
        assert_eq!(2, df1.num_cols());
        assert_eq!(3, df2.num_rows());
        assert_eq!(vec!["a", "b", "c"], df2.colnames().collect::<Vec<_>>());
        assert_eq!(df1.select("a").unwrap(), df2.select("a").unwrap());
        assert_eq!(df1.select("b").unwrap(), df2.select("b").unwrap());

        // assert that with ordering different, the dataframes are still equal
        assert_eq!(df1, df2.project(&["b", "a"]).unwrap());
        assert_eq!(df1, df2.project(&["a", "b"]).unwrap());

        // assert that the ordering is the same as requested
        assert_eq!(
            df2.project(&["b", "a"]).unwrap().schema.fields(),
            vec![
                Field::new("b".to_string(), Type::Bool),
                Field::new("a".to_string(), Type::Int),
            ]
        );
        assert_eq!(
            df2.project(&["a", "b"]).unwrap().schema.fields(),
            vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ]
        );

        let df3 = df2.project(&["a", "c"]).unwrap();
        assert_eq!(2, df3.num_cols());
        assert_eq!(vec!["a", "c"], df3.colnames().collect::<Vec<_>>());
        assert_eq!(df1.select("a").unwrap(), df3.select("a").unwrap());
        assert_eq!(df2.select("c").unwrap(), df3.select("c").unwrap());

        let df4 = df2.strip(&["a", "b"]).unwrap();
        assert_eq!(1, df4.num_cols());
        assert_eq!(vec!["c"], df4.colnames().collect::<Vec<_>>());
        assert_eq!(df3.select("c").unwrap(), df4.select("c").unwrap());

        let df5 = df2.strip(&["b"]).unwrap();
        assert_eq!(df5, df3.project(&["a", "c"]).unwrap());

        // drop fails if the column name is not in the dataframe
        df2.strip(&["a", "b", "d"]).unwrap_err();
    }

    #[test]
    fn test_to_from_rows() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ])
            .unwrap(),
        );
        let rows = vec![
            Row::new(
                schema.clone(),
                Arc::new(vec![Value::Int(1), Value::Bool(true)]),
            )
            .unwrap(),
            Row::new(
                schema.clone(),
                Arc::new(vec![Value::Int(2), Value::Bool(false)]),
            )
            .unwrap(),
            Row::new(
                schema.clone(),
                Arc::new(vec![Value::Int(3), Value::Bool(true)]),
            )
            .unwrap(),
        ];
        let cols = vec![
            Col::new(
                Arc::new(Field::new("a".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b".to_string(), Type::Bool)),
                Arc::new(vec![
                    Value::Bool(true),
                    Value::Bool(false),
                    Value::Bool(true),
                ]),
            )
            .unwrap(),
        ];
        let df1 = Dataframe::from_rows(schema.clone(), &rows).unwrap();
        let df2 = Dataframe::new(cols.clone()).unwrap();
        assert_eq!(df1, df2);
        assert_eq!(rows, df1.rows());

        assert_eq!(
            vec![rows[0].clone(), rows[2].clone()],
            df1.index(&[0, 2]).unwrap()
        );
    }

    #[test]
    fn test_stack() {
        let df1 = Dataframe::new(
            vec![
                Col::new(
                    Arc::new(Field::new("a".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                )
                .unwrap(),
                Col::new(
                    Arc::new(Field::new("b".to_string(), Type::Bool)),
                    Arc::new(vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                    ]),
                )
                .unwrap(),
            ]
            .clone(),
        )
        .unwrap();

        // can not stack dataframe with differnet schema
        let df2 = Dataframe::new(
            vec![
                Col::new(
                    Arc::new(Field::new("a".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                )
                .unwrap(),
                Col::new(
                    Arc::new(Field::new("c".to_string(), Type::Float)),
                    Arc::new(vec![
                        Value::Float(1.0),
                        Value::Float(2.0),
                        Value::Float(3.0),
                    ]),
                )
                .unwrap(),
            ]
            .clone(),
        )
        .unwrap();
        df1.stack(&df2).unwrap_err();

        // but can stack dataframe with same schema
        let df3 = Dataframe::new(
            vec![
                Col::new(
                    Arc::new(Field::new("a".to_string(), Type::Int)),
                    Arc::new(vec![Value::Int(1), Value::Int(2), Value::Int(3)]),
                )
                .unwrap(),
                Col::new(
                    Arc::new(Field::new("b".to_string(), Type::Bool)),
                    Arc::new(vec![
                        Value::Bool(true),
                        Value::Bool(false),
                        Value::Bool(true),
                    ]),
                )
                .unwrap(),
            ]
            .clone(),
        )
        .unwrap();
        let df4 = df1.stack(&df3).unwrap();
        assert_eq!(6, df4.num_rows());
        assert_eq!(2, df4.num_cols());
        let mut expected = df1.rows();
        expected.extend(df3.rows());
        assert_eq!(expected, df4.rows());
    }

    #[test]
    fn test_from_json() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::List(Box::new(Type::Bool))),
                Field::new("c".to_string(), Type::Optional(Box::new(Type::Float))),
                Field::new("d".to_string(), Type::Timestamp),
                Field::new("e".to_string(), Type::Embedding(3)),
            ])
            .unwrap(),
        );
        // order of fields in json is not important
        // optional fields can be omitted or set to null
        // spacing is not important
        let row_json = r#"
        [
            {"a": 1, "b": [true, false],      "c": 1.0, "d": "2020-01-01T00:00:00Z", "e": [1.0, 2.0, 3.0]},
            {  "b": [false, true], "a": 2,"d": "2020-01-01T00:00:00Z", "e": [4.0, 5.0, 6.0]},
            {"a": 3, "b": [true, true], "c": null, "d": "2020-01-01T00:00:00Z", "e": [7.0, 8.0, 9.0]}
        ]"#;

        // json can be row wise or columnar
        let col_json = r#"
        {
            "a": [1, 2, 3], 
            "b": [[true, false], [false, true], [true, true]], 
            "c": [1.0, null, null],
            "d": ["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"], 
            "e": [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]]
        }"#;
        let row_wise_data = serde_json::from_str::<serde_json::Value>(&*row_json).unwrap();
        let columnar_data = serde_json::from_str::<serde_json::Value>(&*col_json).unwrap();
        use crate::value::List;
        let expected_rows = vec![
            Row::new(
                schema.clone(),
                Arc::new(vec![
                    Value::Int(1),
                    Value::List(Arc::new(
                        List::new(Type::Bool, &[Value::Bool(true), Value::Bool(false)]).unwrap(),
                    )),
                    Value::Float(1.0),
                    Value::Timestamp(UTCTimestamp::from(1577836800000000)),
                    Value::Embedding(Arc::new(vec![1.0, 2.0, 3.0])),
                ]),
            )
            .unwrap(),
            Row::new(
                schema.clone(),
                Arc::new(vec![
                    Value::Int(2),
                    Value::List(Arc::new(
                        List::new(Type::Bool, &[Value::Bool(false), Value::Bool(true)]).unwrap(),
                    )),
                    Value::None,
                    Value::Timestamp(UTCTimestamp::from(1577836800000000)),
                    Value::Embedding(Arc::new(vec![4.0, 5.0, 6.0])),
                ]),
            )
            .unwrap(),
            Row::new(
                schema.clone(),
                Arc::new(vec![
                    Value::Int(3),
                    Value::List(Arc::new(
                        List::new(Type::Bool, &[Value::Bool(true), Value::Bool(true)]).unwrap(),
                    )),
                    Value::None,
                    Value::Timestamp(UTCTimestamp::from(1577836800000000)),
                    Value::Embedding(Arc::new(vec![7.0, 8.0, 9.0])),
                ]),
            )
            .unwrap(),
        ];
        for df in [
            Dataframe::from_json(schema.clone(), &row_wise_data).unwrap(),
            Dataframe::from_json(schema.clone(), &columnar_data).unwrap(),
        ] {
            let gotrows = df.rows();
            assert_eq!(expected_rows.len(), gotrows.len());
            for (i, row) in expected_rows.iter().enumerate() {
                assert_eq!(row, &gotrows[i]);
            }

            // try converting back to json and assert
            assert_eq!(df.to_json().unwrap().to_string(),
                       "{\"a\":[1,2,3],\"b\":[[true,false],[false,true],[true,true]],\"c\":[1.0,null,null],\"d\":[\"2020-01-01T00:00:00.000000Z\",\"2020-01-01T00:00:00.000000Z\",\"2020-01-01T00:00:00.000000Z\"],\"e\":[[1.0,2.0,3.0],[4.0,5.0,6.0],[7.0,8.0,9.0]]}");
        }

        // Testing the error case: field is missing in the schema
        let subset = Arc::new(schema.project(&["a", "b"]).unwrap());
        for df in [
            Dataframe::from_json(subset.clone(), &row_wise_data).unwrap(),
            Dataframe::from_json(subset.clone(), &columnar_data).unwrap(),
        ] {
            assert_eq!(df.num_cols(), 2);
        }
    }

    #[test]
    fn test_to_json() {
        let df = Dataframe::new(vec![
            Col::new(
                Arc::new(Field::new("a".to_string(), Type::Int)),
                Arc::new(vec![Value::Int(10), Value::Int(20)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("b".to_string(), Type::Float)),
                Arc::new(vec![Value::Float(20.0), Value::Float(30.0)]),
            )
            .unwrap(),
            Col::new(
                Arc::new(Field::new("c".to_string(), Type::Embedding(3))),
                Arc::new(vec![
                    Value::Embedding(Arc::new(vec![300.0, 400.0, 500.0])),
                    Value::Embedding(Arc::new(vec![3000.0, 4000.5, 5000.1])),
                ]),
            )
            .unwrap(),
        ])
        .unwrap();
        assert_eq!(
            df.to_json().unwrap().to_string(),
            "{\"a\":[10,20],\"b\":[20.0,30.0],\"c\":[[300.0,400.0,500.0],[3000.0,4000.5,5000.1]]}"
        );
    }

    #[test]
    fn test_dataframe_to_from_bytes() {
        let cases = [
            Dataframe::new(vec![
                Col::from("c1", Type::Int, vec![1, 2, 3]).unwrap(),
                Col::from("c2", Type::Float, vec![1., 2., 3.]).unwrap(),
            ])
            .unwrap(),
            Dataframe::new(vec![
                Col::from("c1", Type::Int, Vec::<i64>::new()).unwrap(),
                Col::from("c2", Type::Float, Vec::<f64>::new()).unwrap(),
            ])
            .unwrap(),
            // columns not in the sorted order
            Dataframe::new(vec![
                Col::from("c2", Type::Float, vec![1., 2., 3.]).unwrap(),
                Col::from("c1", Type::Int, vec![1, 2, 3]).unwrap(),
            ])
            .unwrap(),
        ];
        for case in cases {
            let mut buf = Vec::new();
            case.encode(&mut buf).unwrap();
            let got = Dataframe::decode(case.schema().clone(), &mut buf.as_slice()).unwrap();
            assert_eq!(case, got);
        }
    }

    #[test]
    fn test_concat_empty() {
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("a".to_string(), Type::Int),
                Field::new("b".to_string(), Type::Bool),
            ])
            .unwrap(),
        );
        let df = Dataframe::concat(schema.clone(), &[]).unwrap();
        assert_eq!(df, Dataframe::empty(schema));
    }

    #[test]
    fn test_stitch() {
        let df1 = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3]).unwrap(),
            Col::from("b", Type::Bool, vec![true, false, true]).unwrap(),
        ])
        .unwrap();

        let df2 = Dataframe::new(vec![]).unwrap();

        let df3 = df1.stitch(&df2).unwrap();
        assert_eq!(df3, df1);

        let df4 = df2.stitch(&df1).unwrap();
        assert_eq!(df4, df1);

        let df5 = Dataframe::new(vec![
            Col::from("c", Type::Float, vec![4.0, 5.0, 6.0]).unwrap()
        ])
        .unwrap();

        let df6 = df1.stitch(&df5).unwrap();
        assert_eq!(df6.col("a").unwrap(), df1.col("a").unwrap());
        assert_eq!(df6.col("b").unwrap(), df1.col("b").unwrap());
        assert_eq!(df6.col("c").unwrap(), df5.col("c").unwrap());

        // Stitching is commutative.
        assert_eq!(df1.stitch(&df5).unwrap(), df5.stitch(&df1).unwrap());

        // Stitching with another dataframe with a common column should faile.
        let df9 = Dataframe::new(vec![Col::from("a", Type::Int, vec![4, 5, 6]).unwrap()]).unwrap();
        assert!(df1.stitch(&df9).is_err());
    }

    #[test]
    fn test_df_eval_basic() {
        use crate::expr::{builders::*, BinOp, UnOp};
        let df = Dataframe::new(vec![
            Col::from("a", Type::Int, vec![1, 2, 3, 4, 5]).unwrap(),
            Col::from("b", Type::Int, vec![11, 12, 13, 14, 15]).unwrap(),
            Col::from("c", Type::Float, vec![1.1, 2.2, 3.3, 4.4, 5.5]).unwrap(),
            Col::from("d", Type::Bool, vec![true, false, true, false, true]).unwrap(),
            Col::from("e", Type::Timestamp, vec![1001, 1002, 1003, 1004, 1005]).unwrap(),
            Col::from("f", Type::String, vec!["a", "b", "c", "d", "e"]).unwrap(),
        ])
        .unwrap();
        let cases = [
            (lit(1), Col::from("r", Type::Int, [1; 5])),
            (lit(1.1), Col::from("r", Type::Float, [1.1; 5])),
            (lit(true), Col::from("r", Type::Bool, [true; 5])),
            (lit("a"), Col::from("r", Type::String, ["a"; 5])),
            (col("a"), Col::from("r", Type::Int, [1, 2, 3, 4, 5])),
            (
                col("c"),
                Col::from("r", Type::Float, [1.1, 2.2, 3.3, 4.4, 5.5]),
            ),
            (
                col("d"),
                Col::from("r", Type::Bool, [true, false, true, false, true]),
            ),
            (
                col("f"),
                Col::from("r", Type::String, ["a", "b", "c", "d", "e"]),
            ),
            (
                unary(UnOp::Not, col("d")),
                Col::from("r", Type::Bool, vec![false, true, false, true, false]),
            ),
            (
                unary(UnOp::Neg, col("b")),
                Col::from("r", Type::Int, vec![-11, -12, -13, -14, -15]),
            ),
            (
                unary(UnOp::Neg, col("c")),
                Col::from("r", Type::Float, vec![-1.1, -2.2, -3.3, -4.4, -5.5]),
            ),
            (
                binary(lit(1), BinOp::Add, lit(1)),
                Col::from("r", Type::Int, vec![2; 5]),
            ),
            (
                binary(lit(1), BinOp::Add, lit(1.1)),
                Col::from("r", Type::Float, vec![2.1; 5]),
            ),
            (
                binary(col("a"), BinOp::Add, lit(1)),
                Col::from("r", Type::Int, vec![2, 3, 4, 5, 6]),
            ),
            (
                binary(lit(1), BinOp::Add, lit(1)),
                Col::from("r", Type::Int, vec![2; 5]),
            ),
            (
                binary(lit(1), BinOp::Add, lit(1.1)),
                Col::from("r", Type::Float, vec![2.1; 5]),
            ),
            (
                binary(col("a"), BinOp::Add, lit(1)),
                Col::from("r", Type::Int, vec![2, 3, 4, 5, 6]),
            ),
        ];
        for case in cases {
            let col = case.1.unwrap();
            let compiled = case.0.compile(df.schema().clone()).unwrap();
            let got = df.eval(&compiled, col.dtype().clone(), "r", None).unwrap();
            assert_eq!(got, col, "case: {:?}", case.0);
        }
    }
}
