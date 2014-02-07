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
package eu.stratosphere.api.java.typeutils;


import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.InvalidTypesException;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;


public class TypeExtractor {

	
	public static <X> TypeInformation<X> getMapReturnTypes(MapFunction<?, X> mapFunction) {
		return null;
	}
	
	public static <X> TypeInformation<X> getFlatMapReturnTypes(FlatMapFunction<?, X> flatMapFunction) {
		Type returnType = getTemplateTypes (flatMapFunction.getClass(), 1);
		return createTypeInfo(returnType);
	}
	
	public static <X> TypeInformation<X> getGroupReduceReturnTypes(GroupReduceFunction<?, X> groupReduceFunction) {
		return null;
	}
	
	public static <X> TypeInformation<X> getJoinReturnTypes(JoinFunction<?, ?, X> joinFunction) {
		return null;
	}
	
	

	
	public static <X> TypeInformation<X> extractInputFormatTypes(InputFormat<X, ?> format) {
		@SuppressWarnings("unchecked")
		Class<InputFormat<X, ?>> formatClass = (Class<InputFormat<X, ?>>) format.getClass();
		return extractInputFormatTypes(formatClass);
	}
	
	public static <X> TypeInformation<X> extractInputFormatTypes(Class<InputFormat<X, ?>> formatClass) {
		Type type = findGenericParameter(formatClass, InputFormat.class, 0);
		return getTypeInformation(type);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Generic utility methods
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <X> TypeInformation<X> createTypeInfo(Type t) {
		
		if (t instanceof ParameterizedType) {
			ParameterizedType pt = (ParameterizedType) t;
			
			Type raw = pt.getRawType();
			if (raw instanceof Class) {
				
				if (Tuple.class.isAssignableFrom((Class<?>) raw)) {
					Type[] subtypes = pt.getActualTypeArguments();
					
					TypeInformation<?>[] tupleSubTypes = new TypeInformation<?>[subtypes.length];
					for (int i = 0; i < subtypes.length; i++) {
						tupleSubTypes[i] = createTypeInfo(subtypes[i]);
					}
					
					return new TupleTypeInfo(tupleSubTypes);
				}
			}
			
		} else if (t instanceof Class) {
			// basic or arbitrary
			TypeInformation<?> basic = BasicTypeInfo.getInfoFor((Class) t);
			if (basic != null) {
				return (TypeInformation<X>) basic;
			}
		}
		
		return null;
	}
	
	
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
	
	
	public static <X> TypeInformation<X> getTypeInformation(Type type) {
		return null;
	}
	
	public static Type findGenericParameter(Class<?> clazz, Class<?> genericSuperClass, int genericArgumentNum) {
		return null;
	}
	
	
	
	
	private TypeExtractor() {}
}
