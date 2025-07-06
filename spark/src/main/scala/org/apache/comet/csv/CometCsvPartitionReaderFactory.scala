package org.apache.comet.csv

import org.apache.hadoop.conf.Configuration
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFileLinesReader, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.vector.CometPlainVector

case class CometCsvPartitionReaderFactory(conf: Configuration, options: CSVOptions)
    extends FilePartitionReaderFactory {

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("Comet doesn't support 'buildReader'")
  }

  override def buildColumnarReader(
      partitionedFile: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val filePartition = partitionedFile.asInstanceOf[FilePartition]
    val batches = filePartition.files.map { file =>
      val linesReader = new HadoopFileLinesReader(file, options.lineSeparatorInRead, conf)
      Option(TaskContext.get())
        .foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      var idx = 0
      val batch = linesReader.map { line =>
        val vector = new CsvColumnarVector("value");
        vector.setValue(idx, line.getBytes)
        idx += 1
        new CometPlainVector(vector.getVector, false, false, false)
      }
      new ColumnarBatch(batch.toArray)
    }
    CometCsvPartitionReader(batches.toIterator)
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
