package org.apache.flink.api.java.io;

import org.apache.flink.api.common.ResultLocation;

public interface InputGateWrapper<OUT> {

	OUT getNextRecord();

	void open(ResultLocation resultLocation);

	void close();

	boolean hasNext();
}
