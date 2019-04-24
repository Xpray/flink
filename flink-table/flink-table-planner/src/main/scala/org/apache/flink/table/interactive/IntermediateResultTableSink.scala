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

import java.util.UUID

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.java.io.IntermediateResultOutputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.sinks.{BatchTableSink, TableSinkBase}
import org.apache.flink.types.Row

class IntermediateResultTableSink(
  tableEnv: TableEnvironment,
  tableProperties: Configuration,
  tableName: String,
  resultType: RowTypeInfo)
  extends TableSinkBase[Row] with BatchTableSink[Row] {

  override protected def copy(): TableSinkBase[Row] = {
    new IntermediateResultTableSink(tableEnv, tableProperties, tableName, resultType)
  }

  override def emitDataSet(dataSet: DataSet[Row]): Unit = {
    val outputFormat = new IntermediateResultOutputFormat[Row](UUID.fromString(tableName))
    dataSet.output(outputFormat)
      .setParallelism(1)
      .name(s"IntermediateResultOutputFormat:$tableName")
  }

  override def getOutputType: TypeInformation[Row] = resultType
}


