package org.apache.spark.sql.comet

import java.util.Objects

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.StructType

case class CometParquetWriteFilesExec(
    override val originalPlan: SparkPlan,
    override val output: Seq[Attribute],
    child: SparkPlan,
    partitionColumns: Seq[Attribute],
    options: Map[String, String])
    extends CometExec
    with UnaryExecNode {

  override def nodeName: String = "CometParquetWriteFilesExec"

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    this.copy(child = newChild)

  override def equals(obj: Any): Boolean = {
    obj match {
      case other: CometParquetWriteFilesExec => this.child == other.child
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(child)
}
