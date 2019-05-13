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

package org.apache.flink.test.operators;

import org.apache.flink.api.common.ResultPartitionDescriptor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Test ExecutionEnvironment from user perspective.
 */
@SuppressWarnings("serial")
public class ExecutionEnvironmentITCase extends TestLogger {

	private static final int PARALLELISM = 5;

	/**
	 * Ensure that the user can pass a custom configuration object to the LocalEnvironment.
	 */
	@Test
	public void testLocalEnvironmentWithConfig() throws Exception {
		Configuration conf = new Configuration();
		conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, PARALLELISM);

		final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
		env.getConfig().disableSysoutLogging();

		DataSet<Integer> result = env.createInput(new ParallelismDependentInputFormat())
				.rebalance()
				.mapPartition(new RichMapPartitionFunction<Integer, Integer>() {
					@Override
					public void mapPartition(Iterable<Integer> values, Collector<Integer> out) throws Exception {
						out.collect(getRuntimeContext().getIndexOfThisSubtask());
					}
				});
		List<Integer> resultCollection = result.collect();
		assertEquals(PARALLELISM, resultCollection.size());
	}

	@Test
	public void testAccessingBlockingPersistentResultPartition() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);

		DataSet<Tuple2<Long, Long>> input = env.fromElements(new Tuple2<>(1L, 2L));

		DataSet ds = input.map((MapFunction<Tuple2<Long, Long>, Object>) value -> new Tuple2<>(value.f0 + 1, value.f1));

		// specify IntermediateDataSetID
		UUID uuid = UUID.randomUUID();

		// this output branch will be excluded.
		ds.output(new BlockingShuffleOutputFormat(uuid))
			.setParallelism(1);

		ds.collect();

		Map<AbstractID, Map<AbstractID, ResultPartitionDescriptor>> resultPartitionDescriptors = env.getResultPartitionDescriptors();

		// only one cached IntermediateDataSet
		Assert.assertEquals(1, resultPartitionDescriptors.size());

		AbstractID intermediateDataSetID = resultPartitionDescriptors.keySet().iterator().next();

		// IntermediateDataSetID should be the same
		Assert.assertEquals(intermediateDataSetID, new AbstractID(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits()));

		Map<AbstractID, ResultPartitionDescriptor> descriptors = resultPartitionDescriptors.get(intermediateDataSetID);

		Assert.assertEquals(1, descriptors.size());

		ResultPartitionDescriptor descriptor = descriptors.values().iterator().next();

		Assert.assertTrue(
			descriptor != null &&
				descriptor.getAddress() != null &&
				descriptor.getProducerId() != null &&
				(descriptor.getDataPort() == -1 || descriptor.getDataPort() > 0));

	}

	private static class ParallelismDependentInputFormat extends GenericInputFormat<Integer> {

		private transient boolean emitted;

		@Override
		public GenericInputSplit[] createInputSplits(int numSplits) throws IOException {
			assertEquals(PARALLELISM, numSplits);
			return super.createInputSplits(numSplits);
		}

		@Override
		public boolean reachedEnd() {
			return emitted;
		}

		@Override
		public Integer nextRecord(Integer reuse) {
			if (emitted) {
				return null;
			}
			emitted = true;
			return 1;
		}
	}
}
