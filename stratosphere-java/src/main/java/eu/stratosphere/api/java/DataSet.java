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
package eu.stratosphere.api.java;

import java.io.PrintWriter;
import java.util.Arrays;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.io.FileOutputFormat;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.AggregateOperator;
import eu.stratosphere.api.java.operators.DistinctOperator;
import eu.stratosphere.api.java.operators.FilterOperator;
import eu.stratosphere.api.java.operators.FlatMapOperator;
import eu.stratosphere.api.java.operators.GroupedDataSet;
import eu.stratosphere.api.java.operators.JoinOperator.JoinOperatorSets;
import eu.stratosphere.api.java.operators.MapOperator;
import eu.stratosphere.api.java.operators.ReduceGroupOperator;
import eu.stratosphere.api.java.operators.ReduceOperator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.fs.Path;

/**
 *
 * @param <T> The data type of the data set.
 */
public abstract class DataSet<T extends Tuple> {
	
	private final ExecutionContext context;
	
	private final Class<?>[] types;
	
	
	protected DataSet(ExecutionContext context, Class<?>[] types) {
		if (context == null || types == null)
			throw new NullPointerException();
		
		this.context = context;
		this.types = types;
	}

	
	public ExecutionContext getExecutionContext() {
		return this.context;
	}
	
	public int getTupleArity() {
		return this.types.length;
	}
	
	public Class<?>[] getTypes() {
		return this.types;
	}
	
	public Class<?> getDataType(int field) {
		if (field >= 0 && field < this.types.length)
			return this.types[field];
		else 
			throw new IndexOutOfBoundsException("Field " + field + " is out of the tuple range [0, " + (this.types.length-1) + "].");
	}
	
	// --------------------------------------------------------------------------------------------
	//  Operations / Transformations
	// --------------------------------------------------------------------------------------------
	
	public <R extends Tuple> MapOperator<T, R> map(MapFunction<T, R> mapper) {
		return new MapOperator<T, R>(this, mapper);
	}
	
	public <R extends Tuple> FlatMapOperator<T, R> flatMap(FlatMapFunction<T, R> flatMapper) {
		return new FlatMapOperator<T, R>(this, flatMapper);
	}
	
	public FilterOperator<T> filter(FilterFunction<T> filter) {
		return new FilterOperator<T>(this, filter);
	}
	
	public AggregateOperator<T> aggregate(Aggregations agg, int field) {
		return new AggregateOperator<T>(this, agg, field);
	}
	
	public ReduceOperator<T> reduce(ReduceFunction<T> reducer) {
		return new ReduceOperator<T>(this, reducer);
	}
	
	public <R extends Tuple> ReduceGroupOperator<T, R> reduceGroup(GroupReduceFunction<T, R> reducer) {
		return new ReduceGroupOperator<T, R>(this, reducer);
	}
	
	public DistinctOperator<T> distinct(int... fields) {
		return new DistinctOperator<T>(this, fields);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Grouping and Joining
	// --------------------------------------------------------------------------------------------
	
	public GroupedDataSet<T> groupBy(int... fields) {
		return new GroupedDataSet<T>(this, fields);
	}
	
	public <R extends Tuple> JoinOperatorSets<T, R> join(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, false, false);
	}
	
	public <R extends Tuple> JoinOperatorSets<T, R> leftOuterJoin(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, true, false);
	}
	
	public <R extends Tuple> JoinOperatorSets<T, R> rightOuterJoin(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, false, true);
	}
	
	public <R extends Tuple> JoinOperatorSets<T, R> fullOuterJoin(DataSet<R> other) {
		return new JoinOperatorSets<T, R>(this, other, true, true);
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Result writing
	// --------------------------------------------------------------------------------------------
	
	public void writeAsText(Path filePath) {
		// check that it is possible to write this as a text file, i.e., that the data type is Tuple1<String>
		if (getTupleArity() != 1 || getDataType(0) != String.class) {
			throw new InvalidProgramException("Can only write Tuple1<String> as text files.");
		}
	}
	
	public void writeAsCsv(Path filePath) {
		writeAsCsv(filePath, "\n", ",");
	}
	
	public void writeAsCsv(String filePath) {
		writeAsCsv(new Path(filePath));
	}
	
	public void writeAsCsv(Path filePath, String rowDelimiter, String fieldDelimiter) {
		
	}
	
	public void writeAsCsv(String filePath, String rowDelimiter, String fieldDelimiter) {
		writeAsCsv(new Path(filePath), rowDelimiter, fieldDelimiter);
	}
	
	
	public void print() {

	}
	
	public void printTo(PrintWriter writer) {
		
	}
	
	public void write(FileOutputFormat<T> outputFormat) {
		
	}
	
	public void sendResultTo(OutputFormat<T> outputFormat) {
		
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	protected static void checkSameExecutionContext(DataSet<?> set1, DataSet<?> set2) {
		if (set1.context != set2.context)
			throw new IllegalArgumentException("The two inputs have different execution contexts.");
	}
}
