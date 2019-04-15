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

package org.apache.flink.table.interactive

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sinks.{BatchTableSink, TableSinkBase}
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

class TableServiceSink(
  tableEnv: TableEnvironment,
  tableProperties: Configuration,
  tableName: String,
  resultType: RowTypeInfo)
  extends TableSinkBase[Row] with BatchTableSink[Row] {

  override protected def copy(): TableSinkBase[Row] =
    new TableServiceSink(tableEnv, tableProperties, tableName, resultType)

  override def emitDataSet(dataSet: DataSet[Row]): Unit = {
    dataSet
      .flatMap(new TableServiceFlatMapFunction(tableName, tableProperties))
      .cache()
  }

  override def getOutputType: TypeInformation[Row] = resultType
}

class TableServiceFlatMapFunction(
  tableName: String,
  tableProperties: Configuration) extends RichFlatMapFunction[Row, Row] {

  override def open(parameters: Configuration): Unit = {
    println("TableServiceFlatMapFunction open")
  }

  override def close(): Unit = {
    println("TableServiceFlatMapFunction close")
  }

  override def flatMap(value: Row, out: Collector[Row]): Unit = {
    out.collect(value)
  }
}

