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
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.translation.PlanMapOperator;
import eu.stratosphere.api.java.operators.translation.PlanReduceOperator;
import eu.stratosphere.api.java.operators.translation.PlanUnwrappingReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
	protected List<? extends eu.stratosphere.api.common.operators.SingleInputOperator<?>> translateToDataFlow() {
		String name = getName() != null ? getName() : function.getClass().getName();
		
		// distinguish between grouped reduce and non-grouped reduce
		if (grouper == null) {
			// non grouped reduce
			return Collections.singletonList(new PlanReduceOperator<IN>(function, new int[0], name, getInputType()));
		}
		
		
		if (grouper.getKeys() instanceof Keys.SelectorFunctionKeys<?, ?>) {
			
			final Keys.SelectorFunctionKeys<IN, ?> keys = (Keys.SelectorFunctionKeys<IN, ?>) grouper.getKeys();

			List<eu.stratosphere.api.common.operators.SingleInputOperator<?>> result = new ArrayList<eu.stratosphere.api.common.operators.SingleInputOperator<?>>();

			PlanUnwrappingReduceOperator<IN> reducer = new PlanUnwrappingReduceOperator<IN>(function, keys, name, getInputType());
			PlanMapOperator<IN, Tuple2<?, IN>> mapper = new PlanMapOperator<IN, Tuple2<?, IN>>(new MapFunction<IN, Tuple2<?, IN>>() {
				@Override
				public Tuple2<?, IN> map(IN value) throws Exception {
					Object key = keys.getKeyExtractor().getKey(value);
					return new Tuple2<Object, IN>(key, value);
				}
			}, "Key Extractor", getInputType(), reducer.getInputType());

			reducer.setInput(mapper);
			result.add(mapper);
			result.add(reducer);
			return result;
		} else {
			int[] logicalKeyPositions = grouper.getKeys().computeLogicalKeyPositions();

			List<PlanReduceOperator<IN>> result = new ArrayList<PlanReduceOperator<IN>>();
			result.add(new PlanReduceOperator<IN>(function, logicalKeyPositions, name, getInputType()));
			return result;
		}
	}
}
