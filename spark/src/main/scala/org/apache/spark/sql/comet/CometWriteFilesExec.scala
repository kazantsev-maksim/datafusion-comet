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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.comet.serde.OperatorOuterClass.Operator

case class CometWriteFilesExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar()
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometWriteFilesExec =>
        this.child == other.child && this.output == other.output
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(child, output)
}
