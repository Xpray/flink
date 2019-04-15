package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSink;

public class CachedDataSink<T> extends DataSink<T> {
	public CachedDataSink(DataSet<T> data, TypeInformation<T> type) {
		super(data, new DiscardingOutputFormat(), type);
	}

	@Override
	protected GenericDataSinkBase<T> translateToDataFlow(Operator<T> input) {
		GenericDataSinkBase<T> sink =  super.translateToDataFlow(input);
		sink.setCachedSink(true);
		return sink;
	}
}
