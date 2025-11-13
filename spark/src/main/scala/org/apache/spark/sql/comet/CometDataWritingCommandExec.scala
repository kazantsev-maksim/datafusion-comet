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

package org.apache.spark.sql.comet

import java.util.Objects

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{FileFormat, V1WriteCommand}

case class CometDataWritingCommandExec(
    override val fileFormat: FileFormat,
    override val bucketSpec: Option[BucketSpec],
    override val partitionColumns: Seq[Attribute],
    override val staticPartitions: TablePartitionSpec,
    override val outputColumnNames: Seq[String],
    override val options: Map[String, String],
    override val query: LogicalPlan)
    extends V1WriteCommand
    with CometPlan {

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometDataWritingCommandExec =>
        this.fileFormat == other.fileFormat &&
        this.bucketSpec == other.bucketSpec &&
        this.partitionColumns == other.partitionColumns &&
        this.staticPartitions == other.staticPartitions &&
        this.outputColumnNames == other.outputColumnNames &&
        this.options == other.options
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(
    fileFormat,
    bucketSpec,
    partitionColumns,
    staticPartitions,
    outputColumnNames,
    options)

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = Seq.empty

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    this.copy(query = newChild)

  override protected def doExecute(): RDD[InternalRow] = sparkContext.emptyRDD

  override def requiredOrdering: Seq[SortOrder] = Seq.empty
}
