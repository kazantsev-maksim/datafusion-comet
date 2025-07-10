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

package org.apache.comet.csv

import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

import org.apache.comet.vector.CometPlainVector

case class CometCsvPartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration],
    options: CSVOptions)
    extends FilePartitionReaderFactory {

  override def supportColumnarReads(partition: InputPartition): Boolean = true

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("Comet doesn't support 'buildReader'")
  }

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val linesReader =
      new HadoopFileLinesReader(file, options.lineSeparatorInRead, broadcastedConf.value.value)
    Option(TaskContext.get())
      .foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
    var idx = 0
    val batch = linesReader.map { line =>
      val parsedLine = new String(line.getBytes, 0, line.getLength, options.charset)
      val vector = new CsvColumnarVector("value");
      vector.setValue(idx, parsedLine.getBytes)
      idx += 1
      new CometPlainVector(vector.getVector, false, false, false)
    }
    val batchVector = Seq(new ColumnarBatch(batch.toArray))
    CometCsvPartitionReader(batchVector.toIterator)
  }
}

private case class CometCsvPartitionReader(iterator: Iterator[ColumnarBatch])
    extends PartitionReader[ColumnarBatch] {

  private var currentValue: ColumnarBatch = _

  override def next(): Boolean = {
    if (iterator.hasNext) {
      currentValue = iterator.next()
      true
    } else {
      false
    }
  }

  override def get(): ColumnarBatch = currentValue

  override def close(): Unit = {}
}
