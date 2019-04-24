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

package org.apache.flink.api.java.operators;

import org.apache.flink.api.common.ResultLocation;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.InputGateWrapper;
import org.apache.flink.api.java.io.IntermediateResultInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.AbstractID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IntermediateResultDataSource<T> extends DataSource<T> {

	private final UUID uuid;

	public IntermediateResultDataSource(ExecutionEnvironment context,
							InputFormat<T, ?> inputFormat,
							TypeInformation<T> type,
							UUID uuid,
							String dataSourceLocationName) {
		super(context, inputFormat, type, dataSourceLocationName);
		this.uuid = uuid;
	}

	@Override
	protected GenericDataSourceBase<T, ?> translateToDataFlow() {
		GenericDataSourceBase<T, ?> source = super.translateToDataFlow();
		source.setCachedSource(true);
		source.setUuid(uuid);
		AbstractID id = new AbstractID(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
		Map<AbstractID, ResultLocation> locations = context.getResultLocations().getOrDefault(id, new HashMap<>());
		List<Tuple2<AbstractID, ResultLocation>> resultLocations = new ArrayList<>();
		for (Map.Entry<AbstractID, ResultLocation> entry : locations.entrySet()) {
			resultLocations.add(new Tuple2<>(entry.getKey(), entry.getValue()));
		}
		source.setResultLocations(resultLocations);
		return source;
	}
}
