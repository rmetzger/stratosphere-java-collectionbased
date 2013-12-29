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

import eu.stratosphere.api.java.io.CsvReader;
import eu.stratosphere.api.java.io.TextInputFormat;
import eu.stratosphere.api.java.io.TextValueInputFormat;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.types.StringValue;


public abstract class ExecutionContext {
	
	private final UUID executionId;
	
	private int degreeOfParallelism = 1;
	
	
	// --------------------------------------------------------------------------------------------
	//  Constructor and Properties
	// --------------------------------------------------------------------------------------------
	
	protected ExecutionContext() {
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
	
	public DataSet<Tuple1<String>> readTextFile(String filePath) {
		return readTextFile(new Path(filePath));
	}
	
	public DataSet<Tuple1<String>> readTextFile(Path filePath) {
		return new DataSource<Tuple1<String>>(this, new TextInputFormat(filePath), new Class<?>[] { String.class } );
	}
	
	public DataSet<Tuple1<String>> readTextFile(String filePath, String charsetName, boolean skipInvalidLines) {
		return readTextFile(new Path(filePath), charsetName, skipInvalidLines);
	}
	
	public DataSet<Tuple1<String>> readTextFile(Path filePath, String charsetName, boolean skipInvalidLines) {
		TextInputFormat format = new TextInputFormat(filePath);
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return new DataSource<Tuple1<String>>(this, format, new Class<?>[] { String.class } );
	}
	
	// -------------------------- Text Input Format With String Value------------------------------
	
	public DataSet<Tuple1<StringValue>> readTextFileWithValue(String filePath) {
		return readTextFileWithValue(new Path(filePath));
	}
	
	public DataSet<Tuple1<StringValue>> readTextFileWithValue(Path filePath) {
		return new DataSource<Tuple1<StringValue>>(this, new TextValueInputFormat(filePath), new Class<?>[] { StringValue.class } );
	}
	
	public DataSet<Tuple1<StringValue>> readTextFileWithValue(String filePath, String charsetName, boolean skipInvalidLines) {
		return readTextFileWithValue(new Path(filePath), charsetName, skipInvalidLines);
	}
	
	public DataSet<Tuple1<StringValue>> readTextFileWithValue(Path filePath, String charsetName, boolean skipInvalidLines) {
		TextValueInputFormat format = new TextValueInputFormat(filePath);
		format.setCharsetName(charsetName);
		format.setSkipInvalidLines(skipInvalidLines);
		return new DataSource<Tuple1<StringValue>>(this, format, new Class<?>[] { StringValue.class } );
	}
	
	// ----------------------------------- CSV Input Format ---------------------------------------
	
	public CsvReader readCsvFile(Path filePath) {
		return new CsvReader(filePath, this);
	}
	
	public CsvReader readCsvFile(String filePath) {
		return new CsvReader(filePath, this);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Results
	// --------------------------------------------------------------------------------------------
	
	public void execute() {
		// go do the magic
	}
	
	void registerDataSink() {
		
	}
	
	
	
	// --------------------------------------------------------------------------------------------
	//  Instantiation of Execution Contexts
	// --------------------------------------------------------------------------------------------
	
	public static ExecutionContext getExecutionContext() {
		return SystemExecutionContext.getSystemExecutionContext();
	}
	
	public static ExecutionContext createLocalExecutionContext() {
		return new LocalExecutionContext();
	}
	
	public static ExecutionContext createLocalExecutionContext(int degreeOfParallelism) {
		LocalExecutionContext lee = new LocalExecutionContext();
		lee.setDegreeOfParallelism(degreeOfParallelism);
		return lee;
	}
	
	public static ExecutionContext createRemoteExecutionContext(String host, int port) {
		return new RemoteExecutionContext(host, port);
	}
	
	public static ExecutionContext createRemoteExecutionContext(String host, int port, int degreeOfParallelism) {
		RemoteExecutionContext rec = new RemoteExecutionContext(host, port);
		rec.setDegreeOfParallelism(degreeOfParallelism);
		return rec;
	}
}
