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
import java.util.concurrent.{ExecutorService, Executors}

import org.apache.flink.api.common.JobSubmissionResult
import org.apache.flink.configuration.Configuration
import org.apache.flink.service.{ServiceDescriptor, ServiceInstance}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, TableImpl}
import org.apache.flink.table.factories.TableFactory
import org.apache.flink.table.interactive.rpc.{TableServiceClient, TableServiceRegistry}
//import org.apache.flink.table.plan.CacheAwareRelNodePlanBuilder
import org.apache.flink.table.plan.logical.LogicalNode

import scala.concurrent.ExecutionContext

/**
  * TableServiceManager is responsible for table service and cached table management.
  */
class TableServiceManager(tEnv: TableEnvironment) {

  private[flink] val toBeCachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  private[flink] val cachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  private[flink] var tableServiceStarted = false

  private[flink] var submitResult: JobSubmissionResult = _

  private var tableServiceEnv: StreamExecutionEnvironment = _

  private lazy val tableServiceId: String = UUID.randomUUID().toString

  private var tableServiceRegistry: TableServiceRegistry = _

  private val threadPool: ExecutorService = Executors.newFixedThreadPool(1)

  private val executeContext: ExecutionContext =
    ExecutionContext.fromExecutor(threadPool)

  /**
    * used for deleting table partitions.
    */
  private var tableServiceClient: TableServiceClient = _

  def getTableServiceFactory(): Option[TableFactory] = {
    Option(tEnv.getConfig.getTableServiceFactoryDescriptor().getTableFactory)
  }

  def getTableServiceFactoryProperties(): Option[Configuration] = {
    Option(tEnv.getConfig.getTableServiceFactoryDescriptor().getConfiguration)
  }

  def getTableServiceDescriptor(): Option[ServiceDescriptor] = {
    Option(tEnv.getConfig.getTableServiceDescriptor)
  }

  def getTableServiceInstance(): java.util.Map[Integer, ServiceInstance] =
    tableServiceRegistry.getRegistedServices

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

//    val cacheSink = cachePlanBuilder.createCacheTableSink(tableUUID, plan)
//    tEnv.registerTableSink(tableUUID, cacheSink)
//    tEnv.insertInto(table, tableUUID, tEnv.queryConfig)
//
//    val cacheSource = cachePlanBuilder.createCacheTableSource(tableUUID, plan)
//    if (tEnv.scanInternal(Array(tableUUID)).isEmpty) {
//      tEnv.registerTableSource(tableUUID, cacheSource)
//    }
  }

  def cacheTable(table: Table): Unit = {
    val name = java.util.UUID.randomUUID().toString
    cacheTable(table, name)
  }

  private[flink] def getToBeCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(toBeCachedTables.get(logicalPlan))
  }

  private[flink] def getCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(cachedTables.get(logicalPlan))
  }

}
