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
package eu.stratosphere.api.common.typeutils.base;

import eu.stratosphere.api.common.typeutils.ImmutableTypeUtil;
import eu.stratosphere.api.common.typeutils.Serializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

import java.io.IOException;


public class DoubleSerializer extends Serializer<Double> implements ImmutableTypeUtil {

	private static final long serialVersionUID = 1L;
	
	public static final DoubleSerializer INSTANCE = new DoubleSerializer();
	
	private static final Double ZERO = Double.valueOf(0);


	@Override
	public Double createInstance() {
		return ZERO;
	}

	@Override
	public Double copy(Double from, Double reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Double record, DataOutputView target) throws IOException {
		target.writeDouble(record.doubleValue());
	}

	@Override
	public Double deserialize(Double reuse, DataInputView source) throws IOException {
		return Double.valueOf(source.readDouble());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeDouble(source.readDouble());
	}
}
