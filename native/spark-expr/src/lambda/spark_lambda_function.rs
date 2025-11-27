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
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion::common::Result;

pub type SparkLambdaFunction =
    Arc<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>;

#[derive(Debug, Clone)]
pub struct LambdaArgument {
    name: String,
    data_type: DataType,
    nullable: bool,
}

#[derive(Debug, Clone)]
pub struct LambdaFunction {
    function: SparkLambdaFunction,
    arguments: Vec<LambdaArgument>,
    return_type: DataType,
}