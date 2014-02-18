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
package eu.stratosphere.api.java.io;


import eu.stratosphere.api.common.io.GenericCsvInputFormat;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.StringUtils;


public class CsvInputFormat<OUT extends Tuple> extends GenericCsvInputFormat<OUT> {

	private static final long serialVersionUID = 1L;

//	private transient Value
	
	public CsvInputFormat(Path filePath, String lineDelimiter, char fieldDelimiter, Class<?> ... types) {
		super(filePath);
		
		setDelimiter(lineDelimiter);
		setFieldDelim(fieldDelimiter);
		
//		setFieldTypes(types);
	}
	

	@Override
	public OUT readRecord(OUT reuse, byte[] bytes, int offset, int numBytes) {
		return null;
	}
	
	@Override
	public String toString() {
		return "CSV Input (" + StringUtils.showControlCharacters(String.valueOf(getFieldDelim())) + ")";
	}
}
