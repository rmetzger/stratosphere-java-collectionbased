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
package eu.stratosphere.example.java.wordcount;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.KeyExtractor;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;


public class WordCount2WithTuples {

	
	@SuppressWarnings("serial")
	public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {
		
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W");
			for (String token : tokens) {
				out.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
	}
	
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("Usage: <input path> <output path>");
			return;
		}
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		
		final ExecutionEnvironment context = ExecutionEnvironment.getExecutionEnvironment();
		context.setDegreeOfParallelism(1);
		
		DataSet<String> text = context.readTextFile(inputPath);
		
		DataSet<Tuple2<String, Integer>> tokenized = text.flatMap(new Tokenizer());
		
		DataSet<Tuple2<String, Integer>> result = tokenized
				
				.groupBy(new KeyExtractor<Tuple2<String, Integer>, String>() { public String getKey(Tuple2<String, Integer> v) { return v.T1(); } })
				
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
						return new Tuple2<String, Integer>(value1.T1(), value1.T2() + value2.T2());
					}
				});
		
		result.writeAsText(new Path(outputPath));
		context.execute();
	}
}
