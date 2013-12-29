/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.configuration.Configuration;

/**
 *
 * @param <IN> The type of the data set consumed by the operator.
 * @param <OUT> The type of the data set created by the operator.
 */
public class MapOperator<IN extends Tuple, OUT extends Tuple> extends SingleInputOperator<IN, OUT> {
	
	protected final MapFunction<IN, OUT> function;
	
	
	public MapOperator(DataSet<IN> input, MapFunction<IN, OUT> function) {
		super(input);
		
		if (function == null)
			throw new NullPointerException("Map function must not be null.");
		
		this.function = function;
	}
	
	
	public MapOperator<IN, OUT> withParameters(Configuration parameters) {
		setParameters(parameters);
		return this;
	}
}
