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
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{FileFormat, InsertIntoHadoopFsRelationCommand, V1WriteCommand}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.comet.serde.operator.CometSink

case class CometDataWritingCommandExec(
    override val originalPlan: SparkPlan,
    override val fileFormat: FileFormat,
    override val bucketSpec: Option[BucketSpec],
    override val partitionColumns: Seq[Attribute],
    override val staticPartitions: TablePartitionSpec,
    override val outputColumnNames: Seq[String],
    override val options: Map[String, String],
    override val query: LogicalPlan)
    extends CometExec
    with V1WriteCommand {

  override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = Seq.empty

  override def requiredOrdering: Seq[SortOrder] = Seq.empty

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

  override def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    this.withNewChildrenInternal(newChildren = newChildren)

  override def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    this.copy(query = newChild)
}

object CometDataWritingCommandExec extends CometSink[DataWritingCommandExec] {

  override def enabledConfig: Option[ConfigEntry[Boolean]] = Some(
    CometConf.COMET_EXEC_WRITING_ENABLED)

  override def getSupportLevel(operator: DataWritingCommandExec): SupportLevel = {
    if (!operator.cmd.isInstanceOf[InsertIntoHadoopFsRelationCommand]) {
      return Unsupported(Some("Comet support only InsertIntoHadoopFsRelationCommand"))
    }
    val writingCmd = operator.cmd.asInstanceOf[InsertIntoHadoopFsRelationCommand]
    if (writingCmd.bucketSpec.isDefined) {
      return Unsupported(Some("Comet does not support bucketing"))
    }
    if (!writingCmd.fileFormat.isInstanceOf[ParquetFileFormat]) {
      return Unsupported(Some("Comet support only parquet file format to write"))
    }
    if (writingCmd.mode != SaveMode.Overwrite || writingCmd.mode != SaveMode.Append) {
      return Unsupported(Some("Comet support only overwrite and append write mode"))
    }
    Compatible(None)
  }

  override def convert(
      op: DataWritingCommandExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    None
  }

  override def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: DataWritingCommandExec): CometNativeExec = {
    val cmd = op.asInstanceOf[InsertIntoHadoopFsRelationCommand]
    CometSinkPlaceHolder(
      nativeOp,
      op,
      CometDataWritingCommandExec(
        op,
        cmd.fileFormat,
        cmd.bucketSpec,
        cmd.partitionColumns,
        cmd.staticPartitions,
        cmd.outputColumnNames,
        cmd.options,
        cmd.query))
  }

  private def saveMode2Proto(mode: SaveMode): Option[OperatorOuterClass.SaveMode] = {
    mode match {
      case SaveMode.Overwrite => Some(OperatorOuterClass.SaveMode.Overwrite)
      case SaveMode.Append => Some(OperatorOuterClass.SaveMode.Append)
      case _ => None
    }
  }
}
