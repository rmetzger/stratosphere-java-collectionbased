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
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.configuration.Configuration;

/**
 *
 */
public class JoinOperator<I1, I2, OUT> extends TwoInputOperator<I1, I2, OUT> {
	
	public static enum JoinHint {
		/**
		 * leave the choice how to do the join to the optimizer. If in doubt, the
		 * optimizer will choose a repartitioning join.
		 */
		OPTIMIZER_CHOOSES,
		
		/**
		 * Hint that the first join input is much smaller than the second. This results in
		 * broadcasting and hashing the first input, unless the optimizer infers that
		 * prior existing partitioning is available that is even cheaper to exploit.
		 */
		BROADCAST_HASH_FIRST,
		
		/**
		 * Hint that the second join input is much smaller than the second. This results in
		 * broadcasting and hashing the second input, unless the optimizer infers that
		 * prior existing partitioning is available that is even cheaper to exploit.
		 */
		BROADCAST_HASH_SECOND,
		
		/**
		 * Hint that the first join input is a bit smaller than the second. This results in
		 * repartitioning both inputs and hashing the first input, unless the optimizer infers that
		 * prior existing partitioning and orders are available that are even cheaper to exploit.
		 */
		REPARTITION_HASH_FIRST,
		
		/**
		 * Hint that the second join input is a bit smaller than the second. This results in
		 * repartitioning both inputs and hashing the second input, unless the optimizer infers that
		 * prior existing partitioning and orders are available that are even cheaper to exploit.
		 */
		REPARTITION_HASH_SECOND,
		
		/**
		 * Hint that the join should repartitioning both inputs and use sorting and merging
		 * as the join strategy.
		 */
		REPARTITION_SORT_MERGE,
	};
	
	
	private final JoinFunction<I1, I2, OUT> function;
	
	private final int[] keyFields1;
	private final int[] keyFields2;
	
	private boolean preserve1;
	private boolean preserve2;
	
	
	public JoinOperator(DataSet<I1> input1, DataSet<I2> input2, 
			int[] keyFields1, int[] keyFields2, JoinFunction<I1, I2, OUT> function)
	{
		super(input1, input2, TypeExtractor.getJoinReturnTypes(function));
		
		if (keyFields1 == null || keyFields2 == null || keyFields1.length == 0 || keyFields2.length == 0)
			throw new IllegalArgumentException("Equi-Join requires key fields.");
		
		if (keyFields1.length != keyFields2.length)
			throw new IllegalArgumentException("The number of key fields is not the same for both inputs.");
		
		if (function == null)
			throw new NullPointerException("Map function must not be null.");
		
		// range checks
		for (int field : keyFields1) {
			if (field < 0 || field >= input1.getType().getArity())
				throw new IllegalArgumentException("Tuple field is out of range.");
		}
		
		for (int field : keyFields2) {
			if (field < 0 || field >= input2.getType().getArity())
				throw new IllegalArgumentException("Tuple field is out of range.");
		}
		
		this.function = function;
		this.keyFields1 = keyFields1;
		this.keyFields2 = keyFields2;
	}
	
	
	public void setPreserveFirstInput(boolean preserve) {
		this.preserve1 = preserve;
	}
	
	public void setPreserveSecondInput(boolean preserve) {
		this.preserve2 = preserve;
	}
	
	public boolean isPreservingFirstInput() {
		return this.preserve1;
	}
	
	public boolean isPreservingSecondInput() {
		return this.preserve2;
	}
	
	public JoinOperator<I1, I2, OUT> withParameters(Configuration parameters) {
		setParameters(parameters);
		return this;
	}
	
	
	public static final class JoinOperatorSets<I1, I2> {
		
		private final DataSet<I1> input1;
		private final DataSet<I2> input2;
		
		private final JoinHint joinHint;
		
		public JoinOperatorSets(DataSet<I1> input1, DataSet<I2> input2) {
			if (input1 == null || input2 == null)
				throw new NullPointerException();
			
			this.input1 = input1;
			this.input2 = input2;
		}
		
		public JoinOperatorSetsPredicate1 where(int... fields) {
			return new JoinOperatorSetsPredicate1(fields);
		}
	
		public final class JoinOperatorSetsPredicate1 {
			
			private final int[] fields1;
			
			private JoinOperatorSetsPredicate1(int[] fields1) {
				if (fields1 == null || fields1.length == 0)
					throw new IllegalArgumentException("Equi-Join requires key fields.");
				
				int maxField = input1.getType().getArity();
				for (int field : fields1) {
					if (field < 0 || field > maxField)
						throw new IllegalArgumentException("Tuple field is out of range.");
				}
				
				this.fields1 = fields1;
			}
			
			
			public JoinOperatorSetsPredicate2 equalTo(int... fields) {
				return new JoinOperatorSetsPredicate2(fields);
			}
			
	
			public final class JoinOperatorSetsPredicate2 {
				
				private final int[] fields2;
				
				private JoinOperatorSetsPredicate2(int[] fields2) {
					if (fields2 == null || fields2.length == 0)
						throw new IllegalArgumentException("Equi-Join requires key fields.");
					
					int maxField = input2.getType().getArity();
					for (int field : fields2) {
						if (field < 0 || field > maxField)
							throw new IllegalArgumentException("Tuple field is out of range.");
					}
					
					this.fields2 = fields2;
				}
				
				
				public <R extends Tuple> JoinOperator<I1, I2, R> with(JoinFunction<I1, I2, R> function) {
					JoinOperator<I1, I2, R> op = new JoinOperator<I1, I2, R>(input1, input2, fields1, fields2, function);
					op.setPreserveFirstInput(preserve1);
					op.setPreserveSecondInput(preserve2);
					return op;
				}
				
				public JoinOperator<I1, I2, I1> leftSemiJoin() {
					return null;
				}
				
				public JoinOperator<I1, I2, I2> rightSemiJoin() {
					return null;
				}
				
				public JoinOperator<I1, I2, I1> leftAntiJoin() {
					return null;
				}
				
				public JoinOperator<I1, I2, I2> rightAntiJoin() {
					return null;
				}
				
				public <OUT extends Tuple> JoinOperator<I1, I2, OUT> join(JoinFunction<I1, I2, OUT> function) {
					return null;
				}
			}
		}
	}
}
