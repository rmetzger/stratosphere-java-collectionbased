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
package eu.stratosphere.api.java.util;


import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.functions.JoinFunction;


public class TypeExtractor {

	
	public static Class<?>[] getReturnTypes(FlatMapFunction<?, ?> flatMapFunction) {
		ParameterizedType returnTupleType = getTemplateTypesChecked(flatMapFunction.getClass(), 1);
		return getTemplateClassTypes(returnTupleType);
	}
	
	
	public static Class<?>[] getReturnTypes(JoinFunction<?, ?, ?> joinFunction) {
		ParameterizedType returnTupleType = getTemplateTypesChecked(joinFunction.getClass(), 2);
		return getTemplateClassTypes(returnTupleType);
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Generic utility methods
	// --------------------------------------------------------------------------------------------
	
	public static ParameterizedType getTemplateTypesChecked(Class<?> clazz, int pos) {
		Type t = getTemplateTypes(clazz, pos);
		if (t instanceof ParameterizedType) {
			return (ParameterizedType) t;
		} else {
			throw new InvalidTypesException("The generic function type is no Tuple.");
		}
	}
	
	
	public static Type getTemplateTypes(Class<?> clazz, int pos) {
		return getTemplateTypes(getSuperParameterizedType(clazz))[pos];
	}
	
	public static Type[] getTemplateTypes(ParameterizedType paramterizedType) {
		Type[] types = new Type[paramterizedType.getActualTypeArguments().length];
		
		int i = 0;
		for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
			types[i++] = templateArgument;
		}
		return types;
	}
	
	public static ParameterizedType getSuperParameterizedType(Class<?> clazz) {
		Type type = clazz.getGenericSuperclass();
		while (true) {
			if (type instanceof ParameterizedType) {
				return (ParameterizedType) type;
			}

			if (clazz.getGenericSuperclass() == null) {
				throw new IllegalArgumentException();
			}

			type = clazz.getGenericSuperclass();
			clazz = clazz.getSuperclass();
		}
	}
	
	public static Class<?>[] getTemplateClassTypes(ParameterizedType paramterizedType) {
		Class<?>[] types = new Class<?>[paramterizedType.getActualTypeArguments().length];
		int i = 0;
		for (Type templateArgument : paramterizedType.getActualTypeArguments()) {
			types[i++] = (Class<?>) templateArgument;
		}
		return types;
	}
	
	
	private TypeExtractor() {}
}
