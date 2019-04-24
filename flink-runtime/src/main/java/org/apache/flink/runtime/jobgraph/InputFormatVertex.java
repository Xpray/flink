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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.api.common.ResultLocation;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.java.io.IntermediateResultInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.util.AbstractID;

import java.util.List;

public class InputFormatVertex extends JobVertex {

	private static final long serialVersionUID = 1L;
	
	private String formatDescription;

	private boolean cached;

	private IntermediateDataSetID intermediateDataSetID;

	private List<Tuple2<AbstractID, ResultLocation>> resultLocations;

	public InputFormatVertex(String name) {
		super(name);
	}
	
	public InputFormatVertex(String name, JobVertexID id) {
		super(name, id);
	}

	public InputFormatVertex(String name, JobVertexID id, List<JobVertexID> alternativeIds, List<OperatorID> operatorIds, List<OperatorID> alternativeOperatorIds) {
		super(name, id, alternativeIds, operatorIds, alternativeOperatorIds);
	}
	
	
	public void setFormatDescription(String formatDescription) {
		this.formatDescription = formatDescription;
	}
	
	public String getFormatDescription() {
		return formatDescription;
	}
	
	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		final TaskConfig cfg = new TaskConfig(getConfiguration());
		
		// deserialize from the payload
		UserCodeWrapper<InputFormat<?, ?>> wrapper;
		try {
			
			wrapper = cfg.getStubWrapper(loader);
		}
		catch (Throwable t) {
			throw new Exception("Deserializing the InputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}
		if (wrapper == null) {
			throw new Exception("No input format present in InputFormatVertex's task configuration.");
		}
		
		// instantiate, if necessary
		InputFormat<?, ?> inputFormat;
		try {
			inputFormat = wrapper.getUserCodeObject(InputFormat.class, loader);
		}
		catch (Throwable t) {
			throw new Exception("Instantiating the InputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}

		Thread thread = Thread.currentThread();
		ClassLoader original = thread.getContextClassLoader();
		// configure
		try {
			thread.setContextClassLoader(loader);
			inputFormat.configure(cfg.getStubParameters());
			if (inputFormat instanceof IntermediateResultInputFormat) {
				((IntermediateResultInputFormat) inputFormat).initResultLocations(resultLocations);
			}
		}
		catch (Throwable t) {
			throw new Exception("Configuring the InputFormat (" + formatDescription + ") failed: " + t.getMessage(), t);
		}
		finally {
			thread.setContextClassLoader(original);
		}
		
		setInputSplitSource(inputFormat);
	}

	public boolean isCached() {
		return cached;
	}

	public void setCached(boolean cached) {
		this.cached = cached;
	}

	public IntermediateDataSetID getIntermediateDataSetID() {
		return intermediateDataSetID;
	}

	public void setIntermediateDataSetID(IntermediateDataSetID intermediateDataSetID) {
		this.intermediateDataSetID = intermediateDataSetID;
	}

	public List<Tuple2<AbstractID, ResultLocation>> getResultLocations() {
		return resultLocations;
	}

	public void setResultLocations(List<Tuple2<AbstractID, ResultLocation>> resultLocations) {
		this.resultLocations = resultLocations;
	}
}
