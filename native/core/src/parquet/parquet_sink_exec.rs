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

use std::collections::HashMap;
use std::fmt::format;
use crate::execution::operators::ExecutionError;
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::config::{ParquetOptions, TableParquetOptions};
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileSinkConfig};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

const PARQUET_EXTENSION: &'static str = "parquet";

pub fn init_parquet_sink_exec(
    input: Arc<dyn ExecutionPlan>,
    output_base_path: String,
    table_partition_cols: Vec<(String, DataType)>,
    static_partitions: HashMap<String, String>,
    output_schema: SchemaRef,
) -> Result<Arc<DataSinkExec>, ExecutionError> {
    let file_sink_config = FileSinkConfig {
        original_url: output_base_path.clone(),
        object_store_url: ObjectStoreUrl::parse(&output_base_path)?,
        file_group: FileGroup::new(vec![]),
        table_paths: vec![],
        output_schema: output_schema.clone(),
        table_partition_cols,
        insert_op: InsertOp::Overwrite,
        keep_partition_by_columns: false,
        file_extension: PARQUET_EXTENSION.into(),
    };
    let parquet_sink = Arc::new(ParquetSink::new(
        file_sink_config,
        TableParquetOptions::default(),
    ));
    Ok(Arc::new(DataSinkExec::new(input, parquet_sink, None)))
}

fn path_with_partitions(output_base_path: String, static_partitions: HashMap<String, String>) -> ObjectStoreUrl {
    let partitions_path = static_partitions
        .iter()
        .map(|kv| format!("{}={}", kv.0, kv.1))
        .reduce(|a, b| format!("{}/{}", a, b));
    ObjectStoreUrl::parse(format!("{:?}/{:?}", output_base_path, partitions_path)).unwrap()
}

#[cfg(test)]
mod tests {

}