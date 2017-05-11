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

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.StreamITCase
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
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

  val dataCannotBeJoinedByData2 = List(
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
    val ds2 = env.fromCollection(dataCannotBeJoinedByData2).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.leftOuterJoin(ds2, 'b === 'e).select('b, 'c, 'e, 'g)
    val results = joinT.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()
    val expected = Seq("1,Hi,null,null", "2,Hello world,null,null", "2,Hello,null,null")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testStreamJoin(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds1 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    val joinT = ds1.join(ds2).where('b === 'e).select('b, 'c, 'e, 'g)
    val results = joinT.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1,Hi,1,Hallo", "2,Hello world,2,Hallo Welt", "2,Hello,2,Hallo Welt")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testStreamJoinWithWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    env.setParallelism(8)
    StreamITCase.testResults = mutable.MutableList()
    val ds1 = env.fromCollection(data2).toTable(tEnv, 'a, 'b, 'c)
    val ds2 = env.fromCollection(data).toTable(tEnv, 'd, 'e, 'f, 'g, 'h)

    /*
    val joinT = ds1.join(ds2).where('b === 'e)
      .window()
      .groupBy('w,'b)
      .select('b, 'e.count)
    val results = joinT.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1,1", "2,1", "2,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
    */
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

    val leftTable = table1.select('pk as 'leftPk, 'a as 'leftA)
    val rightTable = table2.select('pk as 'rightPk, 'a as 'rightA)

    val resultTable = rightTable
      .join(leftTable)
      .where('leftPk === 'rightPk)


    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("10")
    val testResult = Seq(StreamITCase.testResults.size + "")
    assertEquals(expected.sorted, testResult)
  }

}
