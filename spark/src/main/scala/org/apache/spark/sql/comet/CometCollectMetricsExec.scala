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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{CollectMetricsExec, SparkPlan}

import org.apache.comet.{CometConf, ConfigEntry}
import org.apache.comet.serde.{CometOperatorSerde, OperatorOuterClass, QueryPlanSerde}
import org.apache.comet.serde.OperatorOuterClass.Operator

object CometCollectMetricsExec extends CometOperatorSerde[CollectMetricsExec] {
  def enabledConfig: Option[ConfigEntry[Boolean]] = Some(CometConf.COMET_EXEC_OBSERVE_ENABLED)

  def convert(
      op: CollectMetricsExec,
      builder: Operator.Builder,
      childOp: OperatorOuterClass.Operator*): Option[OperatorOuterClass.Operator] = {
    val metricExpressionsProto =
      op.metricExpressions.map(QueryPlanSerde.exprToProto(_, Seq.empty, binding = false))
    if (metricExpressionsProto.forall(_.isDefined)) {
      val collectMetricsBuilder = OperatorOuterClass.CollectMetrics
        .newBuilder()
        .setMetricName(op.name)
        .addAllMetricExpressions(metricExpressionsProto.map(_.get).asJava)
      Some(builder.setCollectMetrics(collectMetricsBuilder).build())
    } else {
      None
    }
  }

  def createExec(
      nativeOp: OperatorOuterClass.Operator,
      op: CollectMetricsExec): CometNativeExec = {
    CometCollectMetricsExec(
      nativeOp,
      op,
      op.name,
      op.metricExpressions,
      op.child,
      SerializedPlan(None))
  }
}

case class CometCollectMetricsExec(
    override val nativeOp: Operator,
    override val originalPlan: SparkPlan,
    name: String,
    metricExpressions: Seq[NamedExpression],
    child: SparkPlan,
    override val serializedPlanOpt: SerializedPlan)
    extends CometUnaryExec {

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = s"CometCollectMetrics $name"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometCollectMetricsExec =>
        this.name == other.name &&
        this.metricExpressions == other.metricExpressions &&
        this.child == other.child &&
        this.serializedPlanOpt == other.serializedPlanOpt
      case _ =>
        false
    }
  }

  override def hashCode(): Int = {
    Objects.hash(name, metricExpressions, child)
  }
}
