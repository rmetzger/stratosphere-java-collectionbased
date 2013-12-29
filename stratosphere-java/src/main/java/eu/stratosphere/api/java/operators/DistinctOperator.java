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
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.configuration.Configuration;

/**
 * @param <IN> The type of the data set filtered by the operator.
 */
public class DistinctOperator<IN extends Tuple> extends SingleInputOperator<IN, IN> {
	
	private final int[] fields;
	
	public DistinctOperator(DataSet<IN> input, int ... fields) {
		super(input, input.getTypes());
		
		final int inLength = input.getTupleArity();
		
		// null parameter means all fields are considered
		if (fields == null || fields.length == 0) {
			this.fields = new int[inLength];
			for (int i = 0; i < inLength; i++) {
				this.fields[i] = i;
			}
		} else {
			this.fields = OperatorUtil.rangeCheckAndOrderFields(fields, inLength - 1);
		}
	}
	
	public DistinctOperator<IN> withParameters(Configuration parameters) {
		setParameters(parameters);
		return this;
	}
}
