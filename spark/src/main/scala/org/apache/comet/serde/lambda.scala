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

package org.apache.comet.serde

import org.apache.spark.sql.catalyst.expressions.{Attribute, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.types.{IntegerType, StringType}

import org.apache.comet.serde.QueryPlanSerde.{exprToProto, serializeDataType}

object CometLambdaFunction extends CometExpressionSerde[LambdaFunction] {

  override def convert(
      expr: LambdaFunction,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val lambdaFunctionExprProto = exprToProto(expr.function, inputs, binding)
    expr.children.foreach { expr =>
      val exprProto = exprToProto(expr, inputs, binding)
      // scalastyle:off println
      println(expr.sql + " " + exprProto)
    // scalastyle:on println line=35 column=6
    }
    // scalastyle:off println
    println("BOOOM: " + lambdaFunctionExprProto)
    // scalastyle:on println line=36 column=4
    None
  }
}

object CometNamedLambdaVariable extends CometExpressionSerde[NamedLambdaVariable] {

  override def convert(
      expr: NamedLambdaVariable,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val literalBuilder = LiteralOuterClass.Literal
      .newBuilder()
      .setDatatype(serializeDataType(StringType).get)
      .setStringVal(expr.name)
    Some(ExprOuterClass.Expr.newBuilder().setLiteral(literalBuilder).build())
  }
}
