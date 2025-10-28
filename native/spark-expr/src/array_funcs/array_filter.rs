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

use std::any::Any;
use std::fmt::{Display, Formatter};
use datafusion::physical_expr::PhysicalExpr;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use arrow::array::RecordBatch;
use datafusion::common::internal_err;
use datafusion::logical_expr::ColumnarValue;

#[derive(Debug, Eq)]
pub struct ArrayFilter {
    array_expr: Arc<dyn PhysicalExpr>,
    condition_expr: Arc<dyn PhysicalExpr>,
}

impl Hash for ArrayFilter {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.array_expr.hash(state);
        self.condition_expr.hash(state);
    }
}

impl PartialEq for ArrayFilter {
    fn eq(&self, other: &Self) -> bool {
        self.array_expr.eq(&other.array_expr) && self.condition_expr.eq(&other.condition_expr)
    }
}

impl ArrayFilter {
    pub fn new(array_expr: Arc<dyn PhysicalExpr>, condition_expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            array_expr,
            condition_expr,
        }
    }
}

impl Display for ArrayFilter {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

impl PhysicalExpr for ArrayFilter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, _: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        unimplemented!()
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.array_expr, &self.condition_expr]
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalExpr>>) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(ArrayFilter::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
            ))),
            _ => internal_err!("ArrayFilter should have exactly two childrens"),
        }
    }

    fn fmt_sql(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {

}
