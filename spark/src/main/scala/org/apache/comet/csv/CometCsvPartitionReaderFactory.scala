package org.apache.comet.csv

import org.apache.comet.parquet.BatchReader
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.FilePartitionReaderFactory
import org.apache.spark.sql.vectorized.ColumnarBatch

class CometCsvPartitionReaderFactory(val options: CSVOptions) extends FilePartitionReaderFactory {

  private var cometReaders: Iterator[BatchReader] = _

  override def createColumnarReader(partition: InputPartition): PartitionReader[ColumnarBatch] =
    super.createColumnarReader(partition)

  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = {
    throw new UnsupportedOperationException("Comet doesn't support 'buildReader'")
  }
}
