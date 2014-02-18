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
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.translation.PlanReduceOperator;

/**
 *
 * @param <IN> The type of the data set reduced by the operator.
 */
public class ReduceOperator<IN> extends SingleInputUdfOperator<IN, IN, ReduceOperator<IN>> {
	
	private final ReduceFunction<IN> function;
	
	private final Grouping<IN> grouper;
	
	/**
	 * 
	 * This is the case for a reduce-all case (in contrast to the reduce-per-group case).
	 * 
	 * @param input
	 * @param function
	 */
	public ReduceOperator(DataSet<IN> input, ReduceFunction<IN> function) {
		super(input, input.getType());
		
		if (function == null)
			throw new NullPointerException("Reduce function must not be null.");
		
		this.function = function;
		this.grouper = null;
	}
	
	
	public ReduceOperator(Grouping<IN> input, ReduceFunction<IN> function) {
		super(input.getDataSet(), input.getDataSet().getType());
		
		if (function == null)
			throw new NullPointerException("Reduce function must not be null.");
		
		this.function = function;
		this.grouper = input;
	}


	@Override
	protected PlanReduceOperator<IN> translateToDataFlow() {
		String name = getName() != null ? getName() : function.getClass().getName();
		int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();
		
		return new PlanReduceOperator<IN>(function, logicalKeyPositions, name, getInputType());
	}
}
