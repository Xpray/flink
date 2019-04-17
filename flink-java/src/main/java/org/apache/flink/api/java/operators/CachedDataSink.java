package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DataSink;

import java.util.UUID;

public class CachedDataSink<T> extends DataSink<T> {

	private final UUID uuid;

	public CachedDataSink(UUID uuid, DataSet<T> data, TypeInformation<T> type) {
		super(data, new DiscardingOutputFormat(), type);
		this.uuid = uuid;
	}

	@Override
	protected GenericDataSinkBase<T> translateToDataFlow(Operator<T> input) {
		GenericDataSinkBase<T> sink =  super.translateToDataFlow(input);
		sink.setCachedSink(true);
		sink.setUuid(uuid);
		return sink;
	}

}
