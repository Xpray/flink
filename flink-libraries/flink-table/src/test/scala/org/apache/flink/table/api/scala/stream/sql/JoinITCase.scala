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

package org.apache.flink.table.api.scala.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class JoinITCase extends TableTestBase {

  val data2 = List(
    (1, 1L, "Hi"),
    (2, 2L, "Hello"),
    (3, 2L, "Hello world")
  )

  val data = List(
    (1, 1L, 0, "Hallo", 1L),
    (2, 2L, 1, "Hallo Welt", 2L),
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  val dataCannotBeJoin = List(
    (2, 3L, 2, "Hallo Welt wie", 1L),
    (3, 4L, 3, "Hallo Welt wie gehts?", 2L),
    (3, 5L, 4, "ABC", 2L),
    (3, 6L, 5, "BCD", 3L)
  )

  @Test
  def testLeftOuterJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds1 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(dataCannotBeJoin).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 LEFT OUTER JOIN ds2 ON b = e"
    val result = tEnv.sql(query)
    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()
    val expected = Seq("1,Hi,null,null", "2,Hello world,null,null", "2,Hello,null,null")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testInnerJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds1 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)
    tEnv.registerTable("ds1", ds1)
    tEnv.registerTable("ds2", ds2)
    val query = "SELECT b, c, e, g FROM ds1 JOIN ds2 ON b = e"
    val result = tEnv.sql(query)
    val results = result.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()
    println(StreamITCase.testResults.sorted)
    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


  @Test
  def testStreamJoinWithSameRecord(): Unit = {
    val data1 = List(
      (1, 1),
      (1, 1),
      (2, 2),
      (2, 2),
      (3, 3),
      (3, 3),
      (4, 4),
      (4, 4),
      (5, 5),
      (5, 5)
    )

    val data2 = List(
      (1, 1),
      (2, 2),
      (3, 3),
      (4, 4),
      (5, 5),
      (6, 6),
      (7, 7),
      (8, 8),
      (9, 9),
      (10, 10)
    )

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)

    val stream1 = env.fromCollection(data1)
    val table1 = stream1.toTable(tEnv, 'pk, 'a)

    val stream2 = env.fromCollection(data2)
    val table2 = stream2.toTable(tEnv, 'pk, 'a)

    tEnv.registerTable("ds1", table1)
    tEnv.registerTable("ds2", table2)

    val resultTable = tEnv.sql("" +
      "SELECT " +
      "ds1.pk as leftPk, " +
      "ds1.a as leftA, " +
      "ds2.pk as rightPk, " +
      "ds2.a as rightA " +
      "FROM ds1 JOIN ds2 ON ds1.pk = ds2.pk")


    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(10)
    val testResult = Seq(StreamITCase.testResults.size)
    assertEquals(expected.sorted, testResult)
  }

}
