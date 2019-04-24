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

package org.apache.flink.table.interactive.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.service.ServiceDescriptor;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.SchemaValidator;
import org.apache.flink.table.interactive.FlinkTableServiceFactoryDescriptor;
import org.apache.flink.table.interactive.TableServiceFactory;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.hashing.MurmurHash3;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_CLASS_NAME;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_CPU_CORES;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_DIRECT_MEMORY_MB;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_HEAP_MEMORY_MB;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_NATIVE_MEMORY_MB;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_PARALLELISM;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_READY_RETRY_BACKOFF_MS;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_READY_RETRY_TIMES;
import static org.apache.flink.table.interactive.TableServiceOptions.TABLE_SERVICE_STORAGE_ROOT_PATH;

/**
 * Helper class for TableService.
 */
public final class TableServiceUtil {

	private static final Logger LOG = LoggerFactory.getLogger(TableServiceUtil.class);

	private static final String TABLE_NAME = "_table_name_";

	private TableServiceUtil() {}

	public static FlinkTableServiceFactoryDescriptor getDefaultTableServiceFactoryDescriptor(){
		return new FlinkTableServiceFactoryDescriptor(
			new TableServiceFactory(), new Configuration());
	}

	public static void shutdownAndAwaitTermination(ExecutorService pool, long waitTimeInSeconds) {
		pool.shutdown(); // Disable new tasks from being submitted
		try {
			// Wait a while for existing tasks to terminate
			if (!pool.awaitTermination(waitTimeInSeconds, TimeUnit.SECONDS)) {
				pool.shutdownNow(); // Cancel currently executing tasks
				// Wait a while for tasks to respond to being cancelled
				if (!pool.awaitTermination(waitTimeInSeconds, TimeUnit.SECONDS)) {
					LOG.error("Pool did not terminate");
				}
			}
		} catch (InterruptedException ie) {
			// (Re-)Cancel if current thread also interrupted
			pool.shutdownNow();
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
	}

	public static String getTableNameFromConfig(Configuration configuration) {
		return configuration.getString(TABLE_NAME, null);
	}

	public static void putTableNameToConfig(Configuration configuration, String tableName) {
		configuration.setString(TABLE_NAME, tableName);
	}

	public static void putAllProperties(Configuration configuration, Map<String, String> properties) {
		if (properties != null) {
			for (Map.Entry<String, String> entry : properties.entrySet()) {
				configuration.setString(entry.getKey(), entry.getValue());
			}
		}
	}

	public static TableSchema readSchemaFromConfig(Configuration configuration, ClassLoader classLoader) {
		try {
			String encoded = configuration.getString(SchemaValidator.SCHEMA(), null);
			return InstantiationUtil.deserializeObject(Base64.getDecoder().decode(encoded),
				classLoader);
		} catch (ClassNotFoundException | IOException cne) {
			LOG.error("Exception when put rich table schema to configuration: {}", cne.getCause());
			throw new RuntimeException(cne.getMessage());
		}
	}

	public static void putSchemaIntoConfig(Configuration configuration, TableSchema schema) {
		try {
			byte[] serialized = InstantiationUtil.serializeObject(schema);
			String encoded = Base64.getEncoder().encodeToString(serialized);
			configuration.setString(SchemaValidator.SCHEMA(), encoded);
		} catch (IOException ioe) {
			LOG.error("Exception when put rich table schema to configuration: {}", ioe.getCause());
			throw new RuntimeException(ioe.getMessage());
		}
	}

}
