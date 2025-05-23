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

package org.apache.spark.sql.comet.execution.shuffle

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Different from [[org.apache.spark.sql.execution.ShuffledRowRDD]], this RDD is specialized for
 * reading shuffled data through [[CometBlockStoreShuffleReader]]. The shuffled data is read in an
 * iterator of [[Product2[Int, ColumnarBatch]]] instead of [[Product2[Int, InternalRow]]].
 */
class CometShuffledBatchRDD(
    var dependency: ShuffleDependency[Int, _, _],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec])
    extends RDD[ColumnarBatch](dependency.rdd.context, Nil) {

  def this(dependency: ShuffleDependency[Int, _, _], metrics: Map[String, SQLMetric]) = {
    this(
      dependency,
      metrics,
      Array.tabulate(dependency.partitioner.numPartitions)(i => CoalescedPartitionSpec(i, i + 1)))
  }

  dependency.rdd.context.setLocalProperty(
    SortShuffleManager.FETCH_SHUFFLE_BLOCKS_IN_BATCH_ENABLED_KEY,
    SQLConf.get.fetchShuffleBlocksInBatch.toString)

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] =
    if (partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec])) {
      val indices = partitionSpecs.map(_.asInstanceOf[CoalescedPartitionSpec].startReducerIndex)
      // TODO this check is based on assumptions of callers' behavior but is sufficient for now.
      if (indices.toSet.size == partitionSpecs.length) {
        Some(new CoalescedPartitioner(dependency.partitioner, indices))
      } else {
        None
      }
    } else {
      None
    }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      ShuffledRowRDDPartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        // TODO order by partition size.
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialReducerPartitionSpec(_, startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)

      case PartialMapperPartitionSpec(mapIndex, _, _) =>
        tracker.getMapLocation(dependency, mapIndex, mapIndex + 1)

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, _) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[ColumnarBatch] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val reader = split.asInstanceOf[ShuffledRowRDDPartition].spec match {
      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex, _) =>
        SparkEnv.get.shuffleManager
          .getReader(
            dependency.shuffleHandle,
            startReducerIndex,
            endReducerIndex,
            context,
            sqlMetricsReporter)
          .asInstanceOf[CometBlockStoreShuffleReader[_, _]]

      case PartialReducerPartitionSpec(reducerIndex, startMapIndex, endMapIndex, _) =>
        SparkEnv.get.shuffleManager
          .getReader(
            dependency.shuffleHandle,
            startMapIndex,
            endMapIndex,
            reducerIndex,
            reducerIndex + 1,
            context,
            sqlMetricsReporter)
          .asInstanceOf[CometBlockStoreShuffleReader[_, _]]

      case PartialMapperPartitionSpec(mapIndex, startReducerIndex, endReducerIndex) =>
        SparkEnv.get.shuffleManager
          .getReader(
            dependency.shuffleHandle,
            mapIndex,
            mapIndex + 1,
            startReducerIndex,
            endReducerIndex,
            context,
            sqlMetricsReporter)
          .asInstanceOf[CometBlockStoreShuffleReader[_, _]]

      case CoalescedMapperPartitionSpec(startMapIndex, endMapIndex, numReducers) =>
        SparkEnv.get.shuffleManager
          .getReader(
            dependency.shuffleHandle,
            startMapIndex,
            endMapIndex,
            0,
            numReducers,
            context,
            sqlMetricsReporter)
          .asInstanceOf[CometBlockStoreShuffleReader[_, _]]
    }

    // TODO: Reads IPC by native code
    reader.read().asInstanceOf[Iterator[Product2[Int, ColumnarBatch]]].map(_._2)
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    dependency = null
  }
}

/**
 * The [[Partition]] used by [[CometShuffledRowRDD]].
 */
final case class ShuffledRowRDDPartition(index: Int, spec: ShufflePartitionSpec) extends Partition
