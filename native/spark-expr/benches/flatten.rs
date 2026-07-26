// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::array::{Array, ArrayRef, Int32Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::ColumnarValue;
use datafusion_comet_spark_expr::SparkFlatten;
use std::hint::black_box;
use std::sync::Arc;
use datafusion::logical_expr::ScalarUDFImpl;

fn create_nested_list_array(
    outer_rows: usize,
    inner_lists_per_row: usize,
    elems_per_sub_array: usize,
) -> ArrayRef {
    let total_elems = outer_rows * inner_lists_per_row * elems_per_sub_array;
    let values = Int32Array::from((0..total_elems as i32).collect::<Vec<i32>>());

    let inner_total_lists = outer_rows * inner_lists_per_row;
    let mut inner_offsets = Vec::with_capacity(inner_total_lists + 1);
    inner_offsets.push(0i32);
    for i in 1..=inner_total_lists {
        inner_offsets.push((i * elems_per_sub_array) as i32);
    }

    let inner_nulls = NullBuffer::from(
        (0..inner_total_lists)
            .map(|i| i % 7 != 0)
            .collect::<Vec<bool>>(),
    );
    let inner_field = Arc::new(Field::new("item", DataType::Int32, true));
    let inner_list_array = ListArray::new(
        inner_field,
        OffsetBuffer::new(inner_offsets.into()),
        Arc::new(values),
        Some(inner_nulls),
    );

    let mut outer_offsets = Vec::with_capacity(outer_rows + 1);
    outer_offsets.push(0i32);
    for i in 1..=outer_rows {
        outer_offsets.push((i * inner_lists_per_row) as i32);
    }

    let outer_nulls = NullBuffer::from(
        (0..outer_rows)
            .map(|i| i % 11 != 0)
            .collect::<Vec<bool>>(),
    );
    let outer_field = Arc::new(Field::new(
        "item",
        inner_list_array.data_type().clone(),
        true,
    ));

    Arc::new(ListArray::new(
        outer_field,
        OffsetBuffer::new(outer_offsets.into()),
        Arc::new(inner_list_array),
        Some(outer_nulls),
    ))
}

fn criterion_benchmark(c: &mut Criterion) {
    let outer_rows = 8192;
    let flatten_udf = SparkFlatten::new();

    let nested_short = create_nested_list_array(outer_rows, 4, 8);
    c.bench_function("spark_flatten: many short sub-arrays", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&nested_short))];
        b.iter(|| {
            black_box(
                flatten_udf
                    .invoke_with_args(datafusion::logical_expr::ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: vec![],
                        number_rows: outer_rows,
                        return_field: Arc::new(Field::new(
                            "flatten",
                            nested_short.data_type().clone(),
                            true,
                        )),
                        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
                    })
                    .unwrap(),
            )
        })
    });

    let nested_long = create_nested_list_array(outer_rows, 1, 1024);
    c.bench_function("spark_flatten: single long sub-array per row", |b| {
        let args = vec![ColumnarValue::Array(Arc::clone(&nested_long))];
        b.iter(|| {
            black_box(
                flatten_udf
                    .invoke_with_args(datafusion::logical_expr::ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: vec![],
                        number_rows: outer_rows,
                        return_field: Arc::new(Field::new(
                            "flatten",
                            nested_long.data_type().clone(),
                            true,
                        )),
                        config_options: Arc::new(datafusion::config::ConfigOptions::default()),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
