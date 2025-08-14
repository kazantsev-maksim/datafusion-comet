use std::any::Any;
use arrow::array::{as_map_array, Array, ArrayRef, MapArray};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, internal_datafusion_err, Result, ScalarValue};
use datafusion::common::utils::take_function_args;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct SparkMapContainsKey {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkMapContainsKey {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkMapContainsKey {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkMapContainsKey {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_contains_key"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [map_arg, key_arg] = take_function_args("map_contains_key", &args)?;

        let map_array = match map_arg.data_type() {
            DataType::Map(_, _) => as_map_array(&map_arg)?,
            _ => return exec_err!("The first argument in map_contains_key must be a map"),
        };

        let key_type = map_array.key_type();

        if key_type != key_arg.data_type() {
            return exec_err!(
                "The key type {} does not match the map key type {}",
                key_arg.data_type(),
                key_type
            );
        }
        spark_map_contains_key(map_arg, key_arg)
    }
}

fn spark_map_contains_key(map_array: &MapArray, query_keys_array: &dyn Array) -> Result<ArrayRef> {
    map_array.keys();
}