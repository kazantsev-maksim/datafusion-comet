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

package org.apache.comet.serde.operator

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand
import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, Compatible, OperatorOuterClass, SupportLevel, Unsupported}
import org.apache.comet.serde.OperatorOuterClass.Operator
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

object CometDataWritingCommand extends CometOperatorSerde[DataWritingCommandExec] {

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

  private def saveMode2Proto(mode: SaveMode): Option[OperatorOuterClass.SaveMode] = {
    mode match {
      case SaveMode.Overwrite => Some(OperatorOuterClass.SaveMode.Overwrite)
      case SaveMode.Append => Some(OperatorOuterClass.SaveMode.Append)
      case _ => None
    }
  }
}
