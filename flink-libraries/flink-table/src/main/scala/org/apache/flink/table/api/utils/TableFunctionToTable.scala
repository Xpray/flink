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

package org.apache.flink.table.api.utils

import org.apache.flink.table.api.{Table, TableEnvironment, TableException}
import org.apache.flink.table.expressions._


object TableFunctionToTable {

  /**
    * this method is for converting a udtf String to Table
    * ex: TableFunctionToTable(tEnv, "split(c) as (a, b)")
    * @param tableEnv
    * @param udtf
    * @return
    */
  def apply(tableEnv: TableEnvironment, udtf: String): Table = {
    var alias: Option[Seq[String]] = None

    // unwrap an Expression until we get a TableFunctionCall
    def unwrap(expr: Expression): TableFunctionCall = expr match {
      case Alias(child, name, extraNames) =>
        alias = Some(Seq(name) ++ extraNames)
        unwrap(child)
      case Call(name, args) =>
        val function = tableEnv.getFunctionCatalog.lookupFunction(name, args)
        unwrap(function)
      case c: TableFunctionCall => c
      case _ =>
        throw new TableException(
          "Cross/Outer Apply operators only accept expressions that define table functions.")
    }

    val tableFunctionCall = unwrap(ExpressionParser.parseExpression(udtf))
      .as(alias).toLogicalTableFunctionCall(child = null)

    new Table(
      tableEnv,
      tableFunctionCall
    )
  }
}
