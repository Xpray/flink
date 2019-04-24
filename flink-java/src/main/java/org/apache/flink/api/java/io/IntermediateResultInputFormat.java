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

package org.apache.flink.api.java.io;

import org.apache.flink.api.common.ResultLocation;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.util.List;

/** todo 需要一个抽象来描述InputGate
 *
 * @param <OT>
 */
public class IntermediateResultInputFormat<OT> extends RichInputFormat<OT, GenericInputSplit> {

	private List<Tuple2<AbstractID, ResultLocation>> resultLocations;

	private InputGateWrapper<OT> inputGateWrapper;

	@Override
	public void configure(Configuration parameters) {

	}

	public void initResultLocations(List<Tuple2<AbstractID, ResultLocation>> locations) {
		resultLocations = locations;
	}

	public void setInputGateWrapper(InputGateWrapper<OT> inputGateWrapper) {
		this.inputGateWrapper = inputGateWrapper;
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return null;
	}

	@Override
	public GenericInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		if (resultLocations == null) {
			return new GenericInputSplit[0];
		} else {
			GenericInputSplit[] inputSplits = new GenericInputSplit[resultLocations.size()];
			for (int i = 0; i < inputSplits.length; i++) {
				inputSplits[i] = new GenericInputSplit(i, inputSplits.length);
			}
			return inputSplits;
		}
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(GenericInputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		System.out.println("IntermediateResultInputFormat opened");
		inputGateWrapper.open(resultLocations.get(split.getSplitNumber()).f1);
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return inputGateWrapper.hasNext();
	}

	@Override
	public OT nextRecord(OT reuse) throws IOException {
		return inputGateWrapper.getNextRecord();
	}

	@Override
	public void close() throws IOException {
		inputGateWrapper.close();
	}
}
