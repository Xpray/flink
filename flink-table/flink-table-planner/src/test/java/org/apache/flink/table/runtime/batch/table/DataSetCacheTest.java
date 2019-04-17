package org.apache.flink.table.runtime.batch.table;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;

public class DataSetCacheTest {

	@Test
	public void testDataSetCache() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));

		UUID uuid = UUID.randomUUID();
		System.out.println(new IntermediateDataSetID(uuid));
		input1.cache(uuid);

		env.execute();
	}
}
