package org.apache.comet.serde
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

object CometTryElementAt extends CometExpressionSerde {

  override def convert(
      expr: Expression,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    ???
  }
}
