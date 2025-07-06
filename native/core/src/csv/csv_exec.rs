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

use std::sync::Arc;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{CsvSource, FileScanConfigBuilder};
use crate::execution::operators::ExecutionError;

pub(crate) fn init_csv_datasource_exec(
    object_store_url: ObjectStoreUrl,
    data_schema: Option<SchemaRef>
) -> Result<Arc<DataSourceExec>, ExecutionError> {
     let source = Arc::new(CsvSource::new(
             true,
             b',',
             b'"',
         )
         .with_terminator(Some(b'#')
     ));

    let file_scan_config = FileScanConfigBuilder::new(object_store_url, Arc::clone(&data_schema.unwrap()), source)
        .build();

    Ok(Arc::new(DataSourceExec::new(Arc::new(file_scan_config))))
}