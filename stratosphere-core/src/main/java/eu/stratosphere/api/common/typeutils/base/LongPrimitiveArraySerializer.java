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

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.Serializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


public class LongPrimitiveArraySerializer extends Serializer<long[]> {

	private static final long serialVersionUID = 1L;
	
	public static final LongPrimitiveArraySerializer INSTANCE = new LongPrimitiveArraySerializer();
	
	private static final long[] EMPTY = new long[0];


	@Override
	public long[] createInstance() {
		return EMPTY;
	}

	@Override
	public long[] copy(long[] from, long[] reuse) {
		if (reuse.length != from.length) {
			reuse = new long[from.length];
		}
		
		System.arraycopy(from, 0, reuse, 0, from.length);
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(long[] value, DataOutputView target) throws IOException {
		target.writeInt(value.length);
		for (int i = 0; i < value.length; i++) {
			target.writeLong(value[i]);
		}
	}

	@Override
	public long[] deserialize(long[] reuse, DataInputView source) throws IOException {
		int len = source.readInt();
		if (reuse.length != len) {
			reuse = new long[len];
		}
		
		for (int i = 0; i < len; i++) {
			reuse[i] = source.readLong();
		}
		
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		int len = source.readInt();
		target.writeInt(len);
		
		target.write(source, len * 8);
	}
}
