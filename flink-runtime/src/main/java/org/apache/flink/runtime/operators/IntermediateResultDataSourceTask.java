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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.java.io.InputGateWrapper;
import org.apache.flink.api.java.io.IntermediateResultInputFormat;
import org.apache.flink.runtime.execution.Environment;

public class IntermediateResultDataSourceTask<OT> extends DataSourceTask<OT> {

	private InputGateWrapper<OT> inputGateWrapper;

	public IntermediateResultDataSourceTask(Environment environment) {
		super(environment);
	}

	public void setInputGateWrapper(InputGateWrapper<OT> inputGateWrapper) {
		this.inputGateWrapper = inputGateWrapper;
	}

	@Override
	protected void initInputFormat() {
		super.initInputFormat();
		((IntermediateResultInputFormat) format).setInputGateWrapper(inputGateWrapper);
	}
}
