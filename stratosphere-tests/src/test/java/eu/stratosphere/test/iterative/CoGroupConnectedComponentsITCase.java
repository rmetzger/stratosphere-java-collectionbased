/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.test.iterative;

import java.io.BufferedReader;
import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.CoGroupFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsFirst;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.api.java.record.operators.CoGroupOperator.CombinableFirst;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents.DuplicateLongMap;
import eu.stratosphere.example.java.record.connectedcomponents.WorksetConnectedComponents.NeighborWithComponentIDJoin;
import eu.stratosphere.test.testdata.ConnectedComponentsData;
import eu.stratosphere.test.util.TestBase2;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;


public class CoGroupConnectedComponentsITCase extends TestBase2 {
	
	private static final long SEED = 0xBADC0FFEEBEEFL;
	
	private static final int NUM_VERTICES = 1000;
	
	private static final int NUM_EDGES = 10000;

	
	protected String verticesPath;
	protected String edgesPath;
	protected String resultPath;
	
	
	@Override
	protected void preSubmit() throws Exception {
		verticesPath = createTempFile("vertices.txt", ConnectedComponentsData.getEnumeratingVertices(NUM_VERTICES));
		edgesPath = createTempFile("edges.txt", ConnectedComponentsData.getRandomOddEvenEdges(NUM_EDGES, NUM_VERTICES, SEED));
		resultPath = getTempFilePath("results");
	}
	
	@Override
	protected Plan getTestJob() {
		return getPlan(4, verticesPath, edgesPath, resultPath, 100);
	}

	@Override
	protected void postSubmit() throws Exception {
		for (BufferedReader reader : getResultReader(resultPath)) {
			ConnectedComponentsData.checkOddEvenResult(reader);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  The test program
	// --------------------------------------------------------------------------------------------
	

	@CombinableFirst
	@ConstantFieldsFirst(0)
	@ConstantFieldsSecond(0)
	public static final class MinIdAndUpdate extends CoGroupFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		private final LongValue newComponentId = new LongValue();
		
		@Override
		public void coGroup(Iterator<Record> candidates, Iterator<Record> current, Collector<Record> out) throws Exception {
			if (!current.hasNext()) {
				throw new Exception("Error: Id not encountered before.");
			}
			Record old = current.next();
			long oldId = old.getField(1, LongValue.class).getValue();
			
			long minimumComponentID = Long.MAX_VALUE;

			while (candidates.hasNext()) {
				long candidateComponentID = candidates.next().getField(1, LongValue.class).getValue();
				if (candidateComponentID < minimumComponentID) {
					minimumComponentID = candidateComponentID;
				}
			}
			
			if (minimumComponentID < oldId) {
				newComponentId.setValue(minimumComponentID);
				old.setField(1, newComponentId);
				out.collect(old);
			}
		}
		
		@Override
		public void combineFirst(Iterator<Record> records, Collector<Record> out) throws Exception {
			Record next = null;
			long min = Long.MAX_VALUE;
			while (records.hasNext()) {
				next = records.next();
				min = Math.min(min, next.getField(1, LongValue.class).getValue());
			}
			
			newComponentId.setValue(min);
			next.setField(1, newComponentId);
			out.collect(next);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static Plan getPlan(int numSubTasks, String verticesInput, String edgeInput, String output, int maxIterations) {

		// data source for initial vertices
		FileDataSource initialVertices = new FileDataSource(new CsvInputFormat(' ', LongValue.class), verticesInput, "Vertices");
		
		MapOperator verticesWithId = MapOperator.builder(DuplicateLongMap.class).input(initialVertices).name("Assign Vertex Ids").build();
		
		DeltaIteration iteration = new DeltaIteration(0, "Connected Components Iteration");
		iteration.setInitialSolutionSet(verticesWithId);
		iteration.setInitialWorkset(verticesWithId);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		// create DataSourceContract for the edges
		FileDataSource edges = new FileDataSource(new CsvInputFormat(' ', LongValue.class, LongValue.class), edgeInput, "Edges");

		// create CrossOperator for distance computation
		JoinOperator joinWithNeighbors = JoinOperator.builder(new NeighborWithComponentIDJoin(), LongValue.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(edges)
				.name("Join Candidate Id With Neighbor")
				.build();

		CoGroupOperator minAndUpdate = CoGroupOperator.builder(new MinIdAndUpdate(), LongValue.class, 0, 0)
				.input1(joinWithNeighbors)
				.input2(iteration.getSolutionSet())
				.name("Min Id and Update")
				.build();
		
		iteration.setNextWorkset(minAndUpdate);
		iteration.setSolutionSetDelta(minAndUpdate);

		// create DataSinkContract for writing the new cluster positions
		FileDataSink result = new FileDataSink(new CsvOutputFormat(), output, iteration, "Result");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(LongValue.class, 0)
			.field(LongValue.class, 1);

		// return the PACT plan
		Plan plan = new Plan(result, "Workset Connected Components");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
}
