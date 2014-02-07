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

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.java.functions.KeyExtractor;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;


public abstract class Keys<T> {


	public int getNumberOfKeyFields() {
		return 1;
	}
	
	public boolean isEmpty() {
		return getNumberOfKeyFields() == 0;
	}
	
	public boolean areCompatibale(Keys<?> other) {
		return true;
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Specializations for field indexed / expression-based / extractor-based grouping
	// --------------------------------------------------------------------------------------------
	
	public static class FieldPositionKeys<T> extends Keys<T> {
		
		public FieldPositionKeys(int[] groupingFields, TypeInformation<T> type) {
		
			if (groupingFields == null) {
				groupingFields = new int[0];
			}
			
			if (!type.isTupleType()) {
				throw new InvalidProgramException("Specifying keys via field positions is only valid for tuple data types");
			}
	
			groupingFields = makeFields(groupingFields, (TupleTypeInfo<?>) type);
		}
	
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class SelectorFunctionKeys<T, K> extends Keys<T> {
		
		public SelectorFunctionKeys(KeyExtractor<T, K> keyExtractor, TypeInformation<T> type) {
			
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static class ExpressionKeys<T> extends Keys<T> {

		public ExpressionKeys(String expression, TypeInformation<T> type) {
		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	private static int[] makeFields(int[] fields, TupleTypeInfo<?> type) {
		int inLength = type.getArity();
		
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
