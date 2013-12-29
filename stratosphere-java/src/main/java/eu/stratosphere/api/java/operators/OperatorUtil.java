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

import java.util.Arrays;


/**
 * A collection of utility methods for operators.
 */
public class OperatorUtil {
	
	public static final int[] EMPTY_INTS = new int[0];
	

	public static final int[] rangeCheckAndOrderFields(int[] fields, int maxAllowedField) {
		// order
		Arrays.sort(fields);
		
		// range check and duplicate eliminate
		int i = 1, k = 0;
		int last = fields[0];
		
		if (last < 0 || last > maxAllowedField)
			throw new IllegalArgumentException("Tuple position is out of range.");
		
		for (; i < fields.length; i++) {
			if (fields[i] < 0 || i > maxAllowedField)
				throw new IllegalArgumentException("Tuple position is out of range.");
			
			if (fields[i] != last) {
				k++;
				fields[k] = fields[i];
			}
		}
		
		// check if we eliminated something
		if (k == fields.length - 1) {
			return fields;
		} else {
			return Arrays.copyOfRange(fields, 0, k);
		}
	}
	
	
	/**
	 * Class not meant to be instantiated.
	 */
	private OperatorUtil() {}

}
