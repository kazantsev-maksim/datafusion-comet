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
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};

#[derive(Debug)]
pub struct CollectMetricsExec {
    metric_name: String,
    metric_expressions: Vec<Arc<dyn PhysicalExpr>>,
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CollectMetricsExec {
    pub fn new(
        metric_name: String,
        metric_expressions: Vec<Arc<dyn PhysicalExpr>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = input.properties().clone().as_ref().clone();
        Self {
            metric_name,
            metric_expressions,
            input,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}
