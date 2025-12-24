/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.benchmark

import org.apache.spark.sql.catalyst.expressions._

import org.apache.comet.CometConf

/**
 * Configuration for a bitwise expression benchmark.
 *
 * @param name
 *   Name for the benchmark
 * @param query
 *   SQL query to benchmark
 * @param extraCometConfigs
 *   Additional Comet configurations for the scan+exec case
 */
case class BitwiseExprConfig(
    name: String,
    query: String,
    extraCometConfigs: Map[String, String] = Map.empty)

// spotless:off
/**
 * Benchmark to measure performance of Comet bitwise expressions. To run this benchmark:
 * `SPARK_GENERATE_BENCHMARK_FILES=1 make
 * benchmark-org.apache.spark.sql.benchmark.CometBitwiseExpressionBenchmark` Results will be
 * written to "spark/benchmarks/CometBitwiseExpressionBenchmark-**results.txt".
 */
// spotless:on
object CometBitwiseExpressionBenchmark extends CometBenchmarkBase {

  /**
   * Generic method to run a bitwise expression benchmark with the given configuration.
   */
  def runBitwiseExprBenchmark(config: BitwiseExprConfig, values: Int): Unit = {
    withTempPath { dir =>
      withTempTable("parquetV1Table") {
        prepareTable(dir, spark.sql(s"SELECT CAST(value AS INT) AS c1 FROM $tbl"))

        val extraConfigs = Map(
          CometConf.getExprAllowIncompatConfigKey(classOf[BitwiseAnd]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[BitwiseOr]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[BitwiseXor]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[BitwiseCount]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[BitwiseGet]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[ShiftLeft]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(classOf[ShiftRight]) -> "true",
          CometConf.getExprAllowIncompatConfigKey(
            classOf[BitwiseNot]) -> "true") ++ config.extraCometConfigs
        runExpressionBenchmark(config.name, values, config.query, extraConfigs)
      }
    }
  }

  // Configuration for all bitwise expression benchmarks
  private val bitwiseExpressions = Seq(
    BitwiseExprConfig("bitwise_xor", "select c1 ^ 2 from parquetV1Table"),
    BitwiseExprConfig("bitwise_and", "select c1 & 2 from parquetV1Table"),
    BitwiseExprConfig("bitwise_not", "select ~(c1) from parquetV1Table"),
    BitwiseExprConfig("bitwise_or", "select c1 | 2 from parquetV1Table"),
    BitwiseExprConfig("bitwise_count", "select bit_count(c1) from parquetV1Table"),
    BitwiseExprConfig("bit_get", "select bit_get(c1, 2) from parquetV1Table"),
    BitwiseExprConfig("shiftleft", "select shiftleft(c1, 2) from parquetV1Table"),
    BitwiseExprConfig("shiftright", "select shiftright(c1, 2) from parquetV1Table"))

  override def runCometBenchmark(args: Array[String]): Unit = {
    val values = 1024 * 1024

    bitwiseExpressions.foreach { config =>
      runBenchmarkWithTable(config.name, values) { v =>
        runBitwiseExprBenchmark(config, v)
      }
    }
  }
}
