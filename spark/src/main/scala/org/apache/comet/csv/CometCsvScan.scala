package org.apache.comet.csv

import scala.jdk.CollectionConverters.mapAsScalaMapConverter

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.execution.datasources.v2.csv.CSVScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

case class CometCsvScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    partitionFilters: Seq[Expression],
    dataFilters: Seq[Expression],
    options: CaseInsensitiveStringMap)
    extends TextBasedFileScan(sparkSession, options) {

  private lazy val csvOptions: CSVOptions = new CSVOptions(
    options.asScala.toMap,
    columnPruning = sparkSession.sessionState.conf.csvColumnPruning,
    sparkSession.sessionState.conf.sessionLocalTimeZone,
    sparkSession.sessionState.conf.columnNameOfCorruptRecord)

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    CometCsvPartitionReaderFactory(broadcastedConf.value.value, csvOptions)
  }
}

object CometCsvScan {
  def apply(scan: CSVScan): CometCsvScan = {
    CometCsvScan(
      scan.sparkSession,
      scan.fileIndex,
      scan.dataSchema,
      scan.readDataSchema,
      scan.readPartitionSchema,
      scan.partitionFilters,
      scan.dataFilters,
      scan.options)
  }
}
