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
package eu.stratosphere.api.java.operators.translation;

import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.operators.base.GroupReduceOperatorBase;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.Keys;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Reference;

import java.util.Iterator;

/**
 *
 */
public class PlanUnwrappingReduceOperator<T> extends GroupReduceOperatorBase<GenericGroupReduce<Reference<Tuple2<?, T>>,Reference<T>>>
	implements UnaryJavaPlanNode<Tuple2<?, T>, T>
{

	private final TypeInformation<T> type;
	private final Keys.SelectorFunctionKeys<T, ?> key;


	public PlanUnwrappingReduceOperator(ReduceFunction<T> udf, Keys.SelectorFunctionKeys<T, ?> key, String name, TypeInformation<T> type) {
		super(new ReferenceWrappingReducer<T>(udf), key.computeLogicalKeyPositions(), name);
		this.type = type;
		this.key = key;
	}
	
	
	@Override
	public TypeInformation<T> getReturnType() {
		return this.type;
	}

	@Override
	public TypeInformation<Tuple2<?, T>> getInputType() {
		return new TupleTypeInfo<Tuple2<?, T>>(this.key.getTypeInformation(), type);
	}
	
	
	// --------------------------------------------------------------------------------------------
	
	public static final class ReferenceWrappingReducer<T> extends WrappingFunction<ReduceFunction<T>>
		implements GenericGroupReduce<Reference<Tuple2<?, T>>, Reference<T>>
	{

		private static final long serialVersionUID = 1L;
		
		private final Reference<T> ref = new Reference<T>();

		private ReferenceWrappingReducer(ReduceFunction<T> wrapped) {
			super(wrapped);
		}


		@Override
		public void reduce(Iterator<Reference<Tuple2<?, T>>> values, Collector<Reference<T>> out) throws Exception {
			T curr = values.next().ref.T2();
			
			while (values.hasNext()) {
				curr = this.wrappedFunction.reduce(curr, values.next().ref.T2());
			}
			
			ref.ref = curr;
			out.collect(ref);
		}

		@Override
		public void combine(Iterator<Reference<Tuple2<?, T>>> values, Collector<Reference<Tuple2<?, T>>> out) throws Exception {
			final Reference<Tuple2<?, T>> combineRef = new Reference<Tuple2<?, T>>();
			Tuple2<?, T> first = values.next().ref;
			Object key = first.T1();
			T curr = first.T2();

			while (values.hasNext()) {
				curr = this.wrappedFunction.reduce(curr, values.next().ref.T2());
			}

			combineRef.ref = new Tuple2<Object, T>(key, curr);
			out.collect(combineRef);
		}

	}
}
