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

import scala.jdk.CollectionConverters.asJavaIterableConverter

import org.apache.spark.sql.catalyst.expressions.{Attribute, LambdaFunction, NamedLambdaVariable}
import org.apache.spark.sql.types.DataType

import org.apache.comet.CometSparkSessionExtensions.withInfo
import org.apache.comet.serde.ExprOuterClass.LambdaVariable
import org.apache.comet.serde.QueryPlanSerde.{exprToProtoInternal, serializeDataType}

object CometLambdaFunction extends CometExpressionSerde[LambdaFunction] with LambdaBase {

  override def convert(
      expr: LambdaFunction,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val conditionExprProto = exprToProtoInternal(expr.function, inputs, binding)
    val argsExprProto = expr.arguments.map(arg => lambdaVariable2Proto(arg.name, arg.dataType))
    if (argsExprProto.forall(_.isDefined) && conditionExprProto.isDefined) {
      val LambdaFunctionBuilder = ExprOuterClass.LambdaFunction
        .newBuilder()
        .setCondition(conditionExprProto.get)
        .addAllArgs(argsExprProto.map(_.get.build()).asJava)
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setLambdaFunction(LambdaFunctionBuilder)
          .build())
    } else {
      withInfo(expr, expr.children: _*)
      None
    }
  }
}

object CometNamedLambdaVariable
    extends CometExpressionSerde[NamedLambdaVariable]
    with LambdaBase {

  override def convert(
      expr: NamedLambdaVariable,
      inputs: Seq[Attribute],
      binding: Boolean): Option[ExprOuterClass.Expr] = {
    val lambdaVariableExprProto = lambdaVariable2Proto(expr.name, expr.dataType)
    if (lambdaVariableExprProto.isDefined) {
      Some(
        ExprOuterClass.Expr
          .newBuilder()
          .setLambdaVariable(lambdaVariableExprProto.get)
          .build())
    } else {
      withInfo(expr, expr.children: _*)
      None
    }
  }
}

sealed trait LambdaBase {
  def lambdaVariable2Proto(
      argName: String,
      dataType: DataType): Option[LambdaVariable.Builder] = {
    val dataTypeExpr = serializeDataType(dataType)
    if (dataTypeExpr.isDefined) {
      Some(
        ExprOuterClass.LambdaVariable
          .newBuilder()
          .setDatatype(dataTypeExpr.get)
          .setArg(argName))
    } else {
      None
    }
  }
}
