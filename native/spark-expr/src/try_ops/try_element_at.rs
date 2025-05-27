use datafusion::logical_expr::ColumnarValue;
use datafusion::common::Result;
use datafusion::error::DataFusionError;

pub fn spark_try_element_at(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    Err(DataFusionError::Execution(String::new()))
}

#[cfg(test)]
mod tests {

}