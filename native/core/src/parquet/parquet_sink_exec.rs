use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::common::Result;
use datafusion::datasource::physical_plan::parquet::plan_to_parquet;

#[derive(Clone, Debug)]
pub struct ParquetSinkExec {
    input: Arc<dyn ExecutionPlan>,
}

impl ParquetSinkExec {

}

impl DisplayAs for ParquetSinkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for ParquetSinkExec {
    fn name(&self) -> &'static str {
        "ParquetSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let _ = plan_to_parquet(context, self.input, "", None);
    }
}