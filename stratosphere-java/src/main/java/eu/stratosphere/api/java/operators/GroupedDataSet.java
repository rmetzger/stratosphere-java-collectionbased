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
import eu.stratosphere.api.java.aggregation.Aggregations;

public class GroupedDataSet<T> {
	
	private final DataSet<T> dataSet;

	private final int[] groupingFields;
	

	public GroupedDataSet(DataSet<T> set, int[] groupingFields) {
		if (set == null)
			throw new NullPointerException();

		this.dataSet = set;
		this.groupingFields = makeFields(groupingFields, set);
	}
	
	
	public DataSet<T> getDataSet() {
		return this.dataSet;
	}
	
	public int[] getGroupingFields() {
		return this.groupingFields;
	}
	
	// --------------------------------------------------------------------------------------------
	//  Operations / Transformations
	// --------------------------------------------------------------------------------------------
	
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}
	
//	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
//		return new ReduceOperator<T>(this, reducer);
//	}
//	
//	public <R extends Tuple> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
//		return new ReduceGroupOperator<T, R>(this, reducer);
//	}
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	private static int[] makeFields(int[] fields, DataSet<?> dataSet) {
		int inLength = dataSet.getType().getArity();
		
		// null parameter means all fields are considered
		if (fields == null || fields.length == 0) {
			fields = new int[inLength];
			for (int i = 0; i < inLength; i++) {
				fields[i] = i;
			}
			return fields;
		} else {
			return OperatorUtil.rangeCheckAndOrderFields(fields, inLength-1);
		}
		
	}
}
