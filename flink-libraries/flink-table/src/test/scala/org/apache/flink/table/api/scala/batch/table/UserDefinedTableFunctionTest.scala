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
package org.apache.flink.table.api.scala.batch.table

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet => JDataSet, ExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment => ScalaExecutionEnv, _}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, TableFunctionEnvironment, Types}
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils.{PojoTableFunc, TableFunc2, _}
import org.apache.flink.types.Row
import org.junit.Test
import org.mockito.Mockito._

class UserDefinedTableFunctionTest extends TableTestBase {

  @Test
  def testJavaScalaTableAPIEquality(): Unit = {
    // mock
    val ds = mock(classOf[DataSet[Row]])
    val jDs = mock(classOf[JDataSet[Row]])
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    when(ds.javaSet).thenReturn(jDs)
    when(jDs.getType).thenReturn(typeInfo)

    // Scala environment
    val env = mock(classOf[ScalaExecutionEnv])
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val in1 = ds.toTable(tableEnv).as('a, 'b, 'c)

    // Java environment
    val javaEnv = mock(classOf[JavaExecutionEnv])
    val jtEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val in2 = jtEnv.fromDataSet(jDs).as("a, b, c")
    jtEnv.registerTable("MyTable", in2)

    // test cross join
    val func1 = new TableFunc1
    jtEnv.registerFunction("func1", func1)
    var scalaTable = in1.join(func1('c) as 's).select('c, 's)
    var javaTable = in2.join(jtEnv.tableApply("func1(c)")
      .as("s")).select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test left outer join
    scalaTable = in1.leftOuterJoin(func1('c) as 's).select('c, 's)
    javaTable = in2.leftOuterJoin(
      jtEnv.tableApply("func1(c)")
        .as("s")
    ).select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test overloading
    scalaTable = in1.join(func1('c, "$") as 's).select('c, 's)
    javaTable = in2.join(jtEnv.tableApply("func1(c, '$')").as("s")).select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    jtEnv.registerFunction("func2", func2)
    scalaTable = in1.join(func2('c) as ('name, 'len)).select('c, 'name, 'len)
    javaTable = in2.join(
      jtEnv.tableApply("func2(c)")
        .as("name, len"))
      .select("c, name, len")
    //verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    jtEnv.registerFunction("hierarchy", hierarchy)
    scalaTable = in1.join(hierarchy('c) as ('name, 'len, 'adult))
      .select('c, 'name, 'len, 'adult)
    javaTable = in2.join(jtEnv.tableApply("hierarchy(c)").as("name, len, adult"))
      .select("c, name, len, adult")
    verifyTableEquals(scalaTable, javaTable)

    // test pojo type
    val pojo = new PojoTableFunc
    jtEnv.registerFunction("pojo", pojo)
    scalaTable = in1.join(pojo('c))
      .select('c, 'name, 'age)
    javaTable = in2.join(jtEnv.tableApply("pojo(c)"))
      .select("c, name, age")
    verifyTableEquals(scalaTable, javaTable)

    // test with filter
    scalaTable = in1.join(func2('c) as ('name, 'len))
      .select('c, 'name, 'len).filter('len > 2)
    javaTable = in2.join(jtEnv.tableApply("func2(c)").as("name, len"))
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)

    // test with scalar function
    scalaTable = in1.join(func1('c.substring(2)) as 's)
      .select('a, 'c, 's)
    javaTable = in2.join(jtEnv.tableApply("func1(substring(c, 2))").as("s"))
      .select("a, c, s")
    verifyTableEquals(scalaTable, javaTable)
  }

  @Test
  def testCrossJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result1 = table.join(function('c) as 's).select('c, 's)

    val expected1 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("function", function),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result1, expected1)

    // test overloading

    val result2 = table.join(function('c, "$") as 's).select('c, 's)

    val expected2 = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2, '$$')"),
        term("function", function),
        term("rowType",
             "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result2, expected2)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.leftOuterJoin(function('c) as 's).select('c, 's)

    val expected = unaryNode(
      "DataSetCalc",
      unaryNode(
        "DataSetCorrelate",
        batchTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("function", function),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "LEFT")
      ),
      term("select", "c", "s")
    )

    util.verifyTable(result, expected)
  }
}
