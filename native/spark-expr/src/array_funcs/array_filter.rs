use std::sync::Arc;
use datafusion::physical_expr::PhysicalExpr;

struct ArrayFilter {
    array_expr: Arc<dyn PhysicalExpr>,
}