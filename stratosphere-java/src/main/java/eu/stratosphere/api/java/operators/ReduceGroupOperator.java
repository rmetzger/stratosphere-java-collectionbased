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
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.configuration.Configuration;

/**
 *
 * @param <T> The type of the data set created by the operator.
 */
public class ReduceGroupOperator<IN, OUT> extends SingleInputOperator<IN, OUT> {
	
	private final GroupReduceFunction<IN, OUT> function;
	
	private final int[] groupingFields;
	
	private boolean combinable;
	
	
	public ReduceGroupOperator(DataSet<IN> input, GroupReduceFunction<IN, OUT> function) {
		super(input, TypeExtractor.getGroupReduceReturnTypes(function));
		
		if (function == null)
			throw new NullPointerException("GroupReduce function must not be null.");
		
		this.function = function;
		this.groupingFields = OperatorUtil.EMPTY_INTS;
	}
	
	public ReduceGroupOperator(GroupedDataSet<IN> input, GroupReduceFunction<IN, OUT> function) {
		super(input != null ? input.getDataSet() : null, TypeExtractor.getGroupReduceReturnTypes(function));
		
		if (function == null)
			throw new NullPointerException("GroupReduce function must not be null.");
		
		this.function = function;
		this.groupingFields = input.getGroupingFields();
	}
	
	
	
	// --------------------------------------------------------------------------------------------
	//  Fluent API methods
	// --------------------------------------------------------------------------------------------
	
	public ReduceGroupOperator<IN, OUT> withParameters(Configuration parameters) {
		setParameters(parameters);
		return this;
	}
	
	public ReduceGroupOperator<IN, OUT> combinable() {
		this.combinable = true;
		return this;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Properties
	// --------------------------------------------------------------------------------------------
	
	public boolean isCombinable() {
		return combinable;
	}
	
	public void setCombinable(boolean combinable) {
		this.combinable = combinable;
	}
}
