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

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory}
import org.apache.flink.table.interactive.util.TableCacheUtil
import org.apache.flink.table.sinks.BatchTableSink
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.types.Row

class IntermediateResultTableFactory extends BatchTableSinkFactory[Row]
  with BatchTableSourceFactory[Row] {

  private var tEnv: TableEnvironment = _

  override def createBatchTableSink(
    properties: java.util.Map[String, String]): BatchTableSink[Row] = {

    val config = new Configuration()
    TableCacheUtil.putAllProperties(config, properties)

    val tableName = TableCacheUtil.getTableNameFromConfig(config)
    val schema = TableCacheUtil
      .readSchemaFromConfig(config, classOf[IntermediateResultTableFactory].getClassLoader)
    val sink = new IntermediateResultTableSink(
      tEnv,
      config,
      tableName,
      new RowTypeInfo(schema.getFieldTypes, schema.getFieldNames)
    )
    val configuredSink = sink.configure(schema.getFieldNames, schema.getFieldTypes)
    configuredSink.asInstanceOf[BatchTableSink[Row]]
  }

  override def createBatchTableSource(properties: java.util.Map[String, String]): BatchTableSource[Row] = {
    val config = new Configuration()
    TableCacheUtil.putAllProperties(config, properties)

    val tableName = TableCacheUtil.getTableNameFromConfig(config)
    val schema = TableCacheUtil
      .readSchemaFromConfig(config, classOf[IntermediateResultTableFactory].getClassLoader)

    new IntermediateResultTableSource(
      tEnv,
      config,
      tableName,
      new RowTypeInfo(schema.getFieldTypes, schema.getFieldNames)
    )
  }

  override def requiredContext(): java.util.Map[String, String] =
    java.util.Collections.emptyMap()

  override def supportedProperties(): java.util.List[String] =
    java.util.Collections.emptyList()

  def setTableEnv(tEnv: TableEnvironment): Unit = {
    this.tEnv = tEnv
  }
}
