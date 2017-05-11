/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan._
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.{JoinInfo, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{BiRel, RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.api.common.functions.FlatJoinFunction
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.common.typeutils.CompositeType
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{StreamTableEnvironment, TableException}
import org.apache.flink.table.codegen.CodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.{FlatJoinRunner, StreamConnectCoMapFunction}
import org.apache.flink.types.Row

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class DataStreamJoin(
                      cluster: RelOptCluster,
                      traitSet: RelTraitSet,
                      leftNode: RelNode,
                      rightNode: RelNode,
                      rowRelDataType: RelDataType,
                      joinCondition: RexNode,
                      joinRowType: RelDataType,
                      joinInfo: JoinInfo,
                      keyPairs: List[IntPair],
                      joinType: JoinRelType,
                      joinHint: JoinHint,
                      ruleDescription: String)
  extends BiRel(cluster, traitSet, leftNode, rightNode)
    with DataStreamRel {

  override def deriveRowType() = rowRelDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamJoin(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      getRowType,
      joinCondition,
      joinRowType,
      joinInfo,
      keyPairs,
      joinType,
      joinHint,
      ruleDescription)
  }

  override def toString: String = {
    s"$joinTypeToString(where: ($joinConditionToString), join: ($joinSelectionToString))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("where", joinConditionToString)
      .item("join", joinSelectionToString)
      .item("joinType", joinTypeToString)
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }

  /**
    * Translates the FlinkRelNode into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    * @return DataStream of type expectedType or RowTypeInfo
    */
  override def translateToPlan(tableEnv: StreamTableEnvironment): DataStream[CRow] = {
    val config = tableEnv.getConfig

    val returnType = new RowSchema(getRowType).physicalTypeInfo

    // get the equality keys
    val leftKeys = ArrayBuffer.empty[Int]
    val rightKeys = ArrayBuffer.empty[Int]

    if (keyPairs.isEmpty) {
      // if no equality keys => not supported
      throw TableException(
        "Joins should have at least one equality condition.\n" +
          s"\tLeft: ${left.toString},\n" +
          s"\tRight: ${right.toString},\n" +
          s"\tCondition: ($joinConditionToString)"
      )
    }
    else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach(pair => {
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys.add(pair.source)
          rightKeys.add(pair.target)
        } else {
          throw TableException(
            "Equality join predicate on incompatible types.\n" +
              s"\tLeft: ${left.toString},\n" +
              s"\tRight: ${right.toString},\n" +
              s"\tCondition: ($joinConditionToString)"
          )
        }
      })
    }

    val leftDataStream =
      left.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)
    val rightDataStream =
      right.asInstanceOf[DataStreamRel].translateToPlan(tableEnv)

    val (connectOperator, nullCheck) = joinType match {
      case JoinRelType.LEFT | JoinRelType.INNER => (leftDataStream.connect(rightDataStream), false)
      case _ => throw new UnsupportedOperationException(s"An Unsupported JoinType [ $joinType ]")
    }

    if (nullCheck && !config.getNullCheck) {
      throw TableException("Null check in TableConfig must be enabled for outer joins.")
    }

    val generator = new CodeGenerator(
      config,
      nullCheck,
      leftDataStream.getType.asInstanceOf[CRowTypeInfo].rowType,
      Some(rightDataStream.getType.asInstanceOf[CRowTypeInfo].rowType))
    val conversion = generator.generateConverterResultExpression(
      returnType,
      joinRowType.getFieldNames)

    var body = ""

    if (joinInfo.isEqui) {
      // only equality condition
      body =
        s"""
           |${conversion.code}
           |${generator.collectorTerm}.collect(${conversion.resultTerm});
           |""".stripMargin
    }
    else {
      val condition = generator.generateExpression(joinCondition)
      body =
        s"""
           |${condition.code}
           |if (${condition.resultTerm}) {
           |  ${conversion.code}
           |  ${generator.collectorTerm}.collect(${conversion.resultTerm});
           |}
           |""".stripMargin
    }
    val genFunction = generator.generateFunction(
      ruleDescription,
      classOf[FlatJoinFunction[Row, Row, Row]],
      body,
      returnType)

    val joinFun = new FlatJoinRunner[Row, Row, Row](
      genFunction.name,
      genFunction.code,
      genFunction.returnType)
    val joinOpName = s"where: ($joinConditionToString), join: ($joinSelectionToString)"

    val defaultValue = if (joinType == JoinRelType.LEFT) {
      Some(new Row(right.getRowType.getFieldCount))
    } else {
      None
    }

    val coMapFun =
      new StreamConnectCoMapFunction(
        joinFun,
        leftDataStream.getType.asInstanceOf[CompositeType[CRow]],
        rightDataStream.getType.asInstanceOf[CompositeType[CRow]],
        CRowTypeInfo(returnType),
        defaultValue)

    connectOperator
      .keyBy(leftKeys.toArray, rightKeys.toArray)
      .flatMap[CRow](coMapFun).name(joinOpName).asInstanceOf[DataStream[CRow]]
  }

  private def joinSelectionToString: String = {
    getRowType.getFieldNames.asScala.toList.mkString(", ")
  }

  private def joinConditionToString: String = {

    val inFields = joinRowType.getFieldNames.asScala.toList
    getExpressionString(joinCondition, inFields, None)
  }

  private def joinTypeToString = joinType match {
    case JoinRelType.INNER => "InnerJoin"
    case JoinRelType.LEFT => "LeftOuterJoin"
    case JoinRelType.RIGHT => "RightOuterJoin"
    case JoinRelType.FULL => "FullOuterJoin"
  }

}

