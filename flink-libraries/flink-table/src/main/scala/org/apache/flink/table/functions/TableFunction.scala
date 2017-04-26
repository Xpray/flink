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

package org.apache.flink.table.functions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.expressions.{Expression, TableFunctionCall, UnresolvedFieldReference}
import org.apache.flink.table.plan.logical.LogicalTableFunctionCall
import org.apache.flink.util.Collector

/**
  * Base class for a user-defined table function (UDTF). A user-defined table functions works on
  * zero, one, or multiple scalar values as input and returns multiple rows as output.
  *
  * The behavior of a [[TableFunction]] can be defined by implementing a custom evaluation
  * method. An evaluation method must be declared publicly, not static and named "eval".
  * Evaluation methods can also be overloaded by implementing multiple methods named "eval".
  *
  * User-defined functions must have a default constructor and must be instantiable during runtime.
  *
  * By default the result type of an evaluation method is determined by Flink's type extraction
  * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
  * complex, custom, or composite types. In these cases [[TypeInformation]] of the result type
  * can be manually defined by overriding [[getResultType()]].
  *
  * Internally, the Table/SQL API code generation works with primitive values as much as possible.
  * If a user-defined table function should not introduce much overhead during runtime, it is
  * recommended to declare parameters and result types as primitive types instead of their boxed
  * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
  *
  * Example:
  *
  * {{{
  *
  *   public class Split extends TableFunction<String> {
  *
  *     // implement an "eval" method with as many parameters as you want
  *     public void eval(String str) {
  *       for (String s : str.split(" ")) {
  *         collect(s);   // use collect(...) to emit an output row
  *       }
  *     }
  *
  *     // you can overload the eval method here ...
  *   }
  *
  *   val tEnv: TableEnvironment = ...
  *   val table: Table = ...    // schema: [a: String]
  *
  *   // for Scala users
  *   val split = new Split()
  *   table.join(split('c) as ('s)).select('a, 's)
  *
  *   // for Java users
  *   tEnv.registerFunction("split", new Split())   // register table function first
  *   table.join("split(a) as (s)").select("a, s")
  *
  *   // for SQL users
  *   tEnv.registerFunction("split", new Split())   // register table function first
  *   tEnv.sql("SELECT a, s FROM MyTable, LATERAL TABLE(split(a)) as T(s)")
  *
  * }}}
  *
  * @tparam T The type of the output row
  */
abstract class TableFunction[T] extends UserDefinedFunction {

  /**
    * Creates a call to a [[TableFunction]] in Scala Table API.
    *
    * @param params actual parameters of function
    * @return [[Expression]] in form of a [[TableFunctionCall]]
    */
  final def apply(params: Expression*)(implicit typeInfo: TypeInformation[T]): Table = {

    val resultType = if (getResultType == null) typeInfo else getResultType

    new Table(
      tableEnv = null,
      LogicalTableFunctionCall(
        this.getClass.getCanonicalName,
        this,
        params.toList,
        resultType,
        Array.empty,
        child = null
      )
    )
  }

  override def toString: String = getClass.getCanonicalName

  // ----------------------------------------------------------------------------------------------

  /**
    * Emit an output row.
    *
    * @param row the output row
    */
  protected def collect(row: T): Unit = {
    collector.collect(row)
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * The code generated collector used to emit row.
    */
  private var collector: Collector[T] = _

  /**
    * Internal use. Sets the current collector.
    */
  private[flink] final def setCollector(collector: Collector[T]): Unit = {
    this.collector = collector
  }

  // ----------------------------------------------------------------------------------------------

  /**
    * Returns the result type of the evaluation method with a given signature.
    *
    * This method needs to be overriden in case Flink's type extraction facilities are not
    * sufficient to extract the [[TypeInformation]] based on the return type of the evaluation
    * method. Flink's type extraction facilities can handle basic types or
    * simple POJOs but might be wrong for more complex, custom, or composite types.
    *
    * @return [[TypeInformation]] of result type or null if Flink should determine the type
    */
  def getResultType: TypeInformation[T] = null

}
