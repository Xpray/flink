package org.apache.flink.table.runtime.batch.table;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.Collection;

public class DataSetCacheTest {

	@Test
	public void testDataSetCache() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, Long>> input1 = env.fromElements(new Tuple2<Long, Long>(0L, 0L));

		input1.cache();

		env.execute();
	}
}
