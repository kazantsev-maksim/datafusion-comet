// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, ArrayData, MapArray, StructArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::{internal_datafusion_err, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
use datafusion::common::utils::list_to_arrays;

#[derive(Debug)]
pub struct SparkMapFromEntries {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkMapFromEntries {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapFromEntries {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkMapFromEntries {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_from_entries"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    arg_types[0].clone(),
                    false,
                )),
                false,
            )
        )
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args: [ColumnarValue; 1] = args
            .args
            .try_into()
            .map_err(|_| internal_datafusion_err!("map_from_entries expects exactly one argument"))?;
        spark_map_from_entries(&args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }
}

pub fn spark_map_from_entries(args: &[ColumnarValue; 1]) -> Result<ColumnarValue> {
    match args {
        [ColumnarValue::Array(array)] => {
            match array.data_type() {
                DataType::List(field) => {
                    let map_data_type = DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            field.data_type().clone(),
                            false,
                        )),
                        false,
                    );
                    let mut map_data = ArrayData::builder(map_data_type);
                    let entries = list_to_arrays::<i32>(&array);
                    for entry in entries.into_iter() {
                        let _entry = entry
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .expect("A struct is expected");
                        map_data.add_child_data(_entry.clone());
                    }
                }
                _ => unreachable!()
            }
        },
        other => Err(DataFusionError::Execution(format!(
            "Expected a struct array, got: {other:?}"
        ))),
    }
}

fn structs_to_map(entry_struct: &StructArray) -> Result<ColumnarValue> {
    let map_data_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            entry_struct.data_type().clone(),
            false,
        )),
        false,
    );
    let map_data = ArrayData::builder(map_data_type)
        .add_child_data(entry_struct.to_data())
        .build()?;
    let map_array = Arc::new(MapArray::from(map_data));
    Ok(ColumnarValue::Array(map_array))
}
