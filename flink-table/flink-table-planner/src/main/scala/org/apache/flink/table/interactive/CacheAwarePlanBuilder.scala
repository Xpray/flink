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
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.{BatchTableEnvironment, TableEnvironment, TableImpl, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory, TableFactory}
import org.apache.flink.table.interactive.util.TableCacheUtil
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources.TableSource
import org.apache.flink.table.util.LogicalNodeUtil
import org.apache.flink.util.AbstractID

import scala.collection.JavaConverters._

/**
  * A helper class for rebuilding LogicalNode Plan when Table.persist/cache is enabled.
  * @param tEnv
  */
class CacheAwarePlanBuilder(tEnv: TableEnvironment) {

  /**
    * store the the mapping of original node and new node,
    * so that we won't clone twice for one node when building plan.
    */
  private val originalNewNodeMap = new java.util.IdentityHashMap[LogicalNode, LogicalNode]

  /**
    * It will rebuild the logicalNode trees if any table has been cached successfully.
    * @param sinkNodes LogicalNodes which stores in TableEnvironment.
    * @return new LogicalNodes which has been rebuilt.
    */
  def buildPlanIfNeeded(sinkNodes: Seq[LogicalNode]): Seq[LogicalNode] = {
    // clear the map, because the new created tree can also be changed again
    originalNewNodeMap.clear()

    if (tEnv.tableCacheManager.toBeCachedTables.size() > 0) {
      buildPlan(sinkNodes)
    } else {
      sinkNodes
    }
  }

  private def buildPlan(nodes: Seq[LogicalNode]): Seq[LogicalNode] = {
    nodes.map { node =>
      // if the original node has been rebuilt/cloned, just reuse it.
      Option(originalNewNodeMap.get(node)).getOrElse {
        val tableName = tEnv.tableCacheManager.getToBeCachedTableName(node)
        val newNode = tableName match {
          // if a child node(table) has been cached successfully, create a Source node
          case Some(name) if checkIntermediateResult(name)
            => tEnv.scan(name).asInstanceOf[TableImpl].logicalPlan

          // use new children to clone a new node
          case _ => LogicalNodeUtil.cloneLogicalNode(node, buildPlan(node.children))

        }
        originalNewNodeMap.put(node, newNode)
        newNode
      }
    }
  }

  private def checkIntermediateResult(tableName: String): Boolean = {
    val uuid = UUID.fromString(tableName)
    // todo need refactor
    val id = new AbstractID(uuid.getLeastSignificantBits, uuid.getMostSignificantBits)
    tEnv.asInstanceOf[BatchTableEnvironment].execEnv.getResultLocations.containsKey(id)
  }

  private class CacheSourceSinkTableBuilder(name: String, logicalPlan: LogicalNode) {
    var tableFactory : Option[TableFactory] = tEnv.tableCacheManager.getTableServiceFactory()
    var schema: Option[TableSchema] = None
    var properties: Option[Configuration] =
      tEnv.tableCacheManager.getTableServiceFactoryProperties()

    tableFactory match {
      case Some(factory: TableServiceFactory) => factory.setTableEnv(tEnv)
      case _ =>
    }

    def createCacheTableSink(): TableSink[_] = {
      build()
      tableFactory.getOrElse(
        throw new Exception("TableServiceFactory is not configured")
      ) match {
        case factory: BatchTableSinkFactory[_] => {
          val tableProperties = properties.get
          TableCacheUtil.putTableNameToConfig(tableProperties, name)
          TableCacheUtil.putSchemaIntoConfig(tableProperties, schema.get)
          factory.createBatchTableSink(tableProperties.toMap)
        }
        case _ => throw new RuntimeException("Do not supported: " + tableFactory)
      }
    }

    def createCacheTableSource(): TableSource[_] = {
      build()
      tableFactory.getOrElse(
        throw new Exception("TableServiceFactory is not configured")
      ) match {
        case factory: BatchTableSourceFactory[_] => {
          val tableProperties = properties.get
          TableCacheUtil.putTableNameToConfig(tableProperties, name)
          TableCacheUtil.putSchemaIntoConfig(tableProperties, schema.get)
          factory.createBatchTableSource(tableProperties.toMap)
        }
        case _ => throw new RuntimeException("Do not supported: " + tableFactory)
      }
    }

    private def build() = {
      val relNode = logicalPlan.toRelNode(tEnv.getRelBuilder)
      val rowType = relNode.getRowType
      val names: Array[String] = rowType.getFieldNames.asScala.toArray
      val types: Array[TypeInformation[_]] = rowType.getFieldList.asScala
        .map(field => FlinkTypeFactory.toTypeInfo(field.getType)).toArray

      schema = Some(new TableSchema(names, types))
    }
  }

  /**
    * Create a Sink node for caching a table.
    * @param name A unique generated table name.
    * @param logicalPlan LogicalNode of the cached table.
    * @return TableSink[_] TableSink which the cached table will write to.
    */
  def createCacheTableSink(name: String, logicalPlan: LogicalNode): TableSink[_] = {
    val builder = new CacheSourceSinkTableBuilder(name, logicalPlan)
    builder.createCacheTableSink()
  }

  /**
    * Create a Source logical node to scan the cached table.
    * @param name A unique generated table name.
    * @param logicalPlan LogicalNode of the cached table.
    * @return TableSource TableSource of which the data will be read from.
    */
  def createCacheTableSource(name: String, logicalPlan: LogicalNode): TableSource[_] = {
    val builder = new CacheSourceSinkTableBuilder(name, logicalPlan)
    builder.createCacheTableSource()
  }
}
