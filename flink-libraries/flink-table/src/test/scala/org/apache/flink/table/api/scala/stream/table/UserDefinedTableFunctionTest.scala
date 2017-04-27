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
package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.{DataStream => JDataStream}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment => JavaExecutionEnv}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment => ScalaExecutionEnv}
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.utils.TableTestUtil._
import org.apache.flink.table.utils._
import org.apache.flink.types.Row
import org.junit.Assert.{assertTrue, fail}
import org.junit.Test
import org.mockito.Mockito._

class UserDefinedTableFunctionTest extends TableTestBase {

  @Test
  def testJavaScalaTableAPIEquality(): Unit = {
    // mock
    val ds = mock(classOf[DataStream[Row]])
    val jDs = mock(classOf[JDataStream[Row]])
    val typeInfo = new RowTypeInfo(Seq(Types.INT, Types.LONG, Types.STRING): _*)
    when(ds.javaStream).thenReturn(jDs)
    when(jDs.getType).thenReturn(typeInfo)

    // Scala environment
    val env = mock(classOf[ScalaExecutionEnv])
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val in1 = ds.toTable(tableEnv).as('a, 'b, 'c)

    // Java environment
    val javaEnv = mock(classOf[JavaExecutionEnv])
    val jtEnv = TableEnvironment.getTableEnvironment(javaEnv)
    val in2 = jtEnv.fromDataStream(jDs).as("a, b, c")

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
    javaTable = in2.join(jtEnv.tableApply("func1(c, '$')")
      .as("s")).select("c, s")
    verifyTableEquals(scalaTable, javaTable)

    // test custom result type
    val func2 = new TableFunc2
    jtEnv.registerFunction("func2", func2)
    scalaTable = in1.join(func2('c) as ('name, 'len)).select('c, 'name, 'len)
    javaTable = in2.join(
      jtEnv.tableApply("func2(c)")
        .as("name, len"))
      .select("c, name, len")
    verifyTableEquals(scalaTable, javaTable)

    // test hierarchy generic type
    val hierarchy = new HierarchyTableFunction
    jtEnv.registerFunction("hierarchy", hierarchy)
    scalaTable = in1.join(hierarchy('c) as ('name, 'len, 'adult))
      .select('c, 'name, 'len, 'adult)
    javaTable = in2.join(jtEnv.tableApply("hierarchy(c)")
      .as("name, len, adult"))
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
    javaTable = in2.join(jtEnv.tableApply("func2(c)") as ("name, len"))
      .select("c, name, len").filter("len > 2")
    verifyTableEquals(scalaTable, javaTable)

    // test with scalar function
    scalaTable = in1.join(func1('c.substring(2)) as 's)
      .select('a, 'c, 's)
    javaTable = in2.join(jtEnv.tableApply("func1(substring(c, 2))") as ("s"))
      .select("a, c, s")
    verifyTableEquals(scalaTable, javaTable)

    // check scala object is forbidden
    expectExceptionThrown(
      tableEnv.registerFunction("func3", ObjectTableFunction), "Scala object")
    expectExceptionThrown(
      jtEnv.registerFunction("func3", ObjectTableFunction), "Scala object")
    expectExceptionThrown(
      {
        in1.join(ObjectTableFunction('a, 1))
      },
      "Scala object"
    )

  }

  @Test
  def testInvalidTableFunction(): Unit = {
    // mock
    val util = streamTestUtil()
    val t = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val tEnv = TableEnvironment.getTableEnvironment(mock(classOf[JavaExecutionEnv]))
    val tc = new TableFunctionEnvironment(util.tEnv)
    expectExceptionThrown({
      t.leftOuterJoin(t)
    }, "")

    expectExceptionThrown({
      t.join(t as ('x, 'y, 'z))
    }, "")

    expectExceptionThrown({
      val tf1 = new TableFunc1
      util.addFunction("tf1", tf1)
      t.leftOuterJoin(t.join(tf1('c)))
    }, "")


    //=================== check scala object is forbidden =====================
    // Scala table environment register
    expectExceptionThrown(util.addFunction("udtf", ObjectTableFunction), "Scala object")
    // Java table environment register
    expectExceptionThrown(tEnv.registerFunction("udtf", ObjectTableFunction), "Scala object")
    // Scala Table API directly call
    expectExceptionThrown(
      {
        t.join(ObjectTableFunction('a, 1))
      },
      "Scala object")


    //============ throw exception when table function is not registered =========
    // Java Table API call
    expectExceptionThrown(
      t.join(tc.apply("nonexist(a)")
      ), "Undefined function: NONEXIST")
    // SQL API call
    expectExceptionThrown(
      util.tEnv.sql("SELECT * FROM MyTable, LATERAL TABLE(nonexist(a))"),
      "No match found for function signature nonexist(<NUMERIC>)")


    //========= throw exception when the called function is a scalar function ====
    util.tEnv.registerFunction("func0", Func0)

    // Java Table API call
    expectExceptionThrown(
      t.join(tc("func0(a)")),
      "only accept expressions that define table functions",
      classOf[TableException])
    // SQL API call
    // NOTE: it doesn't throw an exception but an AssertionError, maybe a Calcite bug
    expectExceptionThrown(
      util.tEnv.sql("SELECT * FROM MyTable, LATERAL TABLE(func0(a))"),
      null,
      classOf[AssertionError])

    //========== throw exception when the parameters is not correct ===============
    // Java Table API call
    util.addFunction("func2", new TableFunc2)
    expectExceptionThrown(
      t.join(tc.apply("func2(c, c)")),
      "Given parameters of function 'FUNC2' do not match any signature")
    // SQL API call
    expectExceptionThrown(
      util.tEnv.sql("SELECT * FROM MyTable, LATERAL TABLE(func2(c, c))"),
      "No match found for function signature func2(<CHARACTER>, <CHARACTER>)")
  }

  @Test
  def testCrossJoin(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("fun1", function)

    val result1 = table.join(function('c) as 's).select('c, 's)

    val expected1 = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.leftOuterJoin(function('c) as 's).select('c, 's)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
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

  @Test
  def testCustomType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc2
    util.addFunction("func2", function)

    val result = table.join(function('c) as ('name, 'len)).select('c, 'name, 'len)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("function", function),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, " +
           "VARCHAR(2147483647) name, INTEGER len)"),
        term("joinType", "INNER")
      ),
      term("select", "c", "name", "len")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testHierarchyType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new HierarchyTableFunction
    util.addFunction("hierarchy", function)

    val result = table.join(function('c) as ('name, 'adult, 'len))

    val expected = unaryNode(
      "DataStreamCorrelate",
      streamTableNode(0),
      term("invocation", s"${function.functionIdentifier}($$2)"),
      term("function", function),
      term("rowType",
        "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c," +
        " VARCHAR(2147483647) name, BOOLEAN adult, INTEGER len)"),
      term("joinType", "INNER")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testPojoType(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new PojoTableFunc
    util.addFunction("pojo", function)

    val result = table.join(function('c))

    val expected = unaryNode(
      "DataStreamCorrelate",
      streamTableNode(0),
      term("invocation", s"${function.functionIdentifier}($$2)"),
      term("function", function),
      term("rowType",
        "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, " +
         "INTEGER age, VARCHAR(2147483647) name)"),
      term("joinType", "INNER")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testFilter(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc2
    util.addFunction("func2", function)

    val result = table
      .join(function('c) as ('name, 'len))
      .select('c, 'name, 'len)
      .filter('len > 2)

    val expected = unaryNode(
      "DataStreamCalc",
      unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
        term("invocation", s"${function.functionIdentifier}($$2)"),
        term("function", function),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, " +
          "VARCHAR(2147483647) name, INTEGER len)"),
        term("joinType", "INNER"),
        term("condition", ">(AS($1, 'len'), 2)")
      ),
      term("select", "c", "name", "len")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testScalarFunction(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)
    val function = new TableFunc1
    util.addFunction("func1", function)

    val result = table.join(function('c.substring(2)) as 's)

    val expected = unaryNode(
        "DataStreamCorrelate",
        streamTableNode(0),
        term("invocation",  s"${function.functionIdentifier}(SUBSTRING($$2, 2, CHAR_LENGTH($$2)))"),
        term("function", function),
        term("rowType",
          "RecordType(INTEGER a, BIGINT b, VARCHAR(2147483647) c, VARCHAR(2147483647) s)"),
        term("joinType", "INNER")
    )

    util.verifyTable(result, expected)
  }

  // ----------------------------------------------------------------------------------------------

  private def expectExceptionThrown(
      function: => Unit,
      keywords: String,
      clazz: Class[_ <: Throwable] = classOf[ValidationException])
    : Unit = {
    try {
      function
      fail(s"Expected a $clazz, but no exception is thrown.")
    } catch {
      case e if e.getClass == clazz =>
        if (keywords != null) {
          assertTrue(
            s"The exception message '${e.getMessage}' doesn't contain keyword '$keywords'",
            e.getMessage.contains(keywords))
        }
      case e: Throwable => fail(s"Expected throw ${clazz.getSimpleName}, but is $e.")
    }
  }

}
