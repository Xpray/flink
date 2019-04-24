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

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{Table, TableEnvironment, TableImpl}
import org.apache.flink.table.factories.TableFactory
import org.apache.flink.table.plan.logical.LogicalNode

/**
  * TableServiceManager is responsible for table service and cached table management.
  */
class TableServiceManager(tEnv: TableEnvironment) {

  private[flink] val toBeCachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  def getTableServiceFactory(): Option[TableFactory] = {
    Option(tEnv.getConfig.getTableServiceFactoryDescriptor().getTableFactory)
  }

  def getTableServiceFactoryProperties(): Option[Configuration] = {
    Option(tEnv.getConfig.getTableServiceFactoryDescriptor().getConfiguration)
  }

  private[flink] val cachePlanBuilder: CacheAwarePlanBuilder =
    new CacheAwarePlanBuilder(tEnv)

  /**
    * Record the uuid of a table to be cached & adding Source/Sink LogicalNode
    * @param table The table to be cached.
    */
  private def cacheTable(table: Table, tableUUID: String): Unit = {
    val plan = table match {
      case tableImpl: TableImpl => tableImpl.logicalPlan
      case _ => throw new RuntimeException("Table do not support Cache().")
    }

    toBeCachedTables.put(plan, tableUUID)

    val cacheSink = cachePlanBuilder.createCacheTableSink(tableUUID, plan)
    tEnv.registerTableSink(tableUUID, cacheSink)
    tEnv.insertInto(table, tableUUID, tEnv.queryConfig)

    val cacheSource = cachePlanBuilder.createCacheTableSource(tableUUID, plan)
    //if (tEnv.scanInternal(Array(tableUUID)).isEmpty) {
      tEnv.registerTableSource(tableUUID, cacheSource)
    //}
  }

  def cacheTable(table: Table): Unit = {
    val name = java.util.UUID.randomUUID().toString
    cacheTable(table, name)
  }

  private[flink] def getToBeCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(toBeCachedTables.get(logicalPlan))
  }

  /**
    * mock for now
    * @param table
    */
  def invalidateCache(table: Table): Unit = {
    val plan = table match {
      case tableImpl: TableImpl => tableImpl.logicalPlan
      case _ => throw new RuntimeException("Table do not support Cache().")
    }
    toBeCachedTables.remove(plan)
  }

}
