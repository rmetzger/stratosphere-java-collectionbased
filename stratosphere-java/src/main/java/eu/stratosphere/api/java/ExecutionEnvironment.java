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

import java.util.UUID;

import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.api.java.io.CsvReader;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.io.TextValueInputFormat;
import eu.stratosphere.api.java.operators.DataSink;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.typeutils.BasicTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.api.java.typeutils.ValueTypeInfo;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.StringValue;


public abstract class ExecutionEnvironment {
	
	private final UUID executionId;
	
	private int degreeOfParallelism = 1;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructor and Properties
	// --------------------------------------------------------------------------------------------
	
	protected ExecutionEnvironment() {
		this.executionId = UUID.randomUUID();
	}
	
	public int getDegreeOfParallelism() {
		return degreeOfParallelism;
	}
	
	public void setDegreeOfParallelism(int degreeOfParallelism) {
		if (degreeOfParallelism < 1)
			throw new IllegalArgumentException("Degree of parallelism must be at least one.");
		
		this.degreeOfParallelism = degreeOfParallelism;
	}
	
	public UUID getId() {
		return this.executionId;
	}
	
	public String getIdString() {
		return this.executionId.toString();
	}
	
	// --------------------------------------------------------------------------------------------
	//  Data set creations
	// --------------------------------------------------------------------------------------------

	// ---------------------------------- Text Input Format ---------------------------------------
	
	public DataSet<String> readTextFile(String filePath) {
		return readTextFile(new Path(filePath));
	}
	
	public DataSet<String> readTextFile(Path filePath) {
		return new DataSource<String>(this, new TextInputFormat(filePath), BasicTypeInfo.getStringInfo() );
	}
	
	public DataSet<String> readTextFile(String filePath, String charsetName, boolean skipInvalidLines) {
		return readTextFile(new Path(filePath), charsetName, skipInvalidLines);
	}
	
	public DataSet<String> readTextFile(Path filePath, String charsetName, boolean skipInvalidLines) {
		TextInputFormat format = new TextInputFormat(filePath);
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return new DataSource<String>(this, format, BasicTypeInfo.getStringInfo() );
	}
	
	// -------------------------- Text Input Format With String Value------------------------------
	
	public DataSet<StringValue> readTextFileWithValue(String filePath) {
		return readTextFileWithValue(new Path(filePath));
	}
	
	public DataSet<StringValue> readTextFileWithValue(Path filePath) {
		return new DataSource<StringValue>(this, new TextValueInputFormat(filePath), new ValueTypeInfo<StringValue>(StringValue.class) );
	}
	
	public DataSet<StringValue> readTextFileWithValue(String filePath, String charsetName, boolean skipInvalidLines) {
		return readTextFileWithValue(new Path(filePath), charsetName, skipInvalidLines);
	}
	
	public DataSet<StringValue> readTextFileWithValue(Path filePath, String charsetName, boolean skipInvalidLines) {
		TextValueInputFormat format = new TextValueInputFormat(filePath);
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return new DataSource<StringValue>(this, format, new ValueTypeInfo<StringValue>(StringValue.class) );
	}
	
	// ----------------------------------- CSV Input Format ---------------------------------------
	
	public CsvReader readCsvFile(Path filePath) {
		return new CsvReader(filePath, this);
	}
	
	public CsvReader readCsvFile(String filePath) {
		return new CsvReader(filePath, this);
	}
	
	// ----------------------------------- Generic Input Format ---------------------------------------
	
	public <X> DataSet<X> createInput(InputFormat<X, ?> inputFormat) {
		if (inputFormat == null)
			throw new IllegalArgumentException("InputFormat must not be null.");
		
		return new DataSource<X>(this, inputFormat, TypeExtractor.extractInputFormatTypes(inputFormat));
	}
	
	// --------------------------------------------------------------------------------------------
	//  Results
	// --------------------------------------------------------------------------------------------
	
	public void execute() {
		// go do the magic
	}
	
	void registerDataSink(DataSink<?> sink) {
		
	}
	
	
	// --------------------------------------------------------------------------------------------
	//  Instantiation of Execution Contexts
	// --------------------------------------------------------------------------------------------
	
	public static ExecutionEnvironment getExecutionEnvironment() {
		return ContextEnvironment.getContextEnvironment();
	}
	
	public static ExecutionEnvironment createLocalEnvironment() {
		return new LocalEnvironment();
	}
	
	public static ExecutionEnvironment createLocalEnvironment(int degreeOfParallelism) {
		LocalEnvironment lee = new LocalEnvironment();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}
	
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port) {
		return new RemoteEnvironment(host, port);
	}
	
	public static ExecutionEnvironment createRemoteEnvironment(String host, int port, int degreeOfParallelism) {
		RemoteEnvironment rec = new RemoteEnvironment(host, port);
		rec.setDegreeOfParallelism(degreeOfParallelism);
		return rec;
	}
}
