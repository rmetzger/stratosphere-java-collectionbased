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

package eu.stratosphere.client;

import java.util.List;

import org.apache.log4j.Level;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.dag.DataSinkNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plandump.PlanJSONDumpGenerator;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.util.LogUtils;

/**
 * A class for executing a {@link Plan} on a local Nephele instance. Note that
 * no HDFS instance or anything of that nature is provided. You must therefore
 * only use data sources and sinks with paths beginning with "file://" in your
 * plan.
 * 
 * When the class is instantiated a local nephele instance is started, this can
 * be stopped by calling stopNephele.
 */
public class LocalExecutor implements PlanExecutor {

	private final Object lock = new Object();	// we lock to ensure singleton execution
	
	private NepheleMiniCluster nephele;

	
	public LocalExecutor() {
		if (System.getProperty("log4j.configuration") == null && !LogUtils.isLoggingConfigured()) {
			LogUtils.initializeDefaultConsoleLogger(Level.INFO);
		}
	}
	
	public void start() throws Exception {
		synchronized (this.lock) {
			this.nephele = new NepheleMiniCluster();
			this.nephele.start();
		}
	}

	/**
	 * Stop the local executor instance. You should not call executePlan after this.
	 */
	public void stop() throws Exception {
		synchronized (this.lock) {
			this.nephele.stop();
			this.nephele = null;
		}
	}

	/**
	 * Execute the given plan on the local Nephele instance, wait for the job to
	 * finish and return the runtime in milliseconds.
	 * 
	 * @param plan The plan of the program to execute.
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		synchronized (this.lock) {
			if (this.nephele == null) {
				throw new Exception("The local executor has not been started.");
			}

			PactCompiler pc = new PactCompiler(new DataStatistics());
			OptimizedPlan op = pc.compile(plan);
			
			NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
			JobGraph jobGraph = jgg.compileJobGraph(op);
			
			JobClient jobClient = this.nephele.getJobClient(jobGraph);
			JobExecutionResult result = jobClient.submitJobAndWait();
			return result;
		}
	}

	/**
	 * Returns a JSON dump of the optimized plan.
	 * 
	 * @param plan
	 *            The program's plan.
	 * @return JSON dump of the optimized plan.
	 * @throws Exception
	 */
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		if (this.nephele == null) {
			throw new Exception("The local executor has not been started.");
		}

		PactCompiler pc = new PactCompiler(new DataStatistics());
		OptimizedPlan op = pc.compile(plan);
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();

		return gen.getOptimizerPlanAsJSON(op);
	}

	/**
	 * Executes the program described by the given plan assembler.
	 * 
	 * @param pa The program's plan assembler. 
	 * @param args The parameters.
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public static JobExecutionResult execute(Program pa, String... args) throws Exception {
		return execute(pa.getPlan(args));
	}
	
	/**
	 * Executes the program represented by the given Pact plan.
	 * 
	 * @param pa The program's plan. 
	 * @return The net runtime of the program, in milliseconds.
	 * 
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	public static JobExecutionResult execute(Plan plan) throws Exception {
		LocalExecutor exec = new LocalExecutor();
		try {
			exec.start();
			return exec.executePlan(plan);
		} finally {
			exec.stop();
		}
	}

	/**
	 * Returns a JSON dump of the optimized plan.
	 * 
	 * @param plan
	 *            The program's plan.
	 * @return JSON dump of the optimized plan.
	 * @throws Exception
	 */
	public static String optimizerPlanAsJSON(Plan plan) throws Exception {
		LocalExecutor exec = new LocalExecutor();
		try {
			exec.start();
			PactCompiler pc = new PactCompiler(new DataStatistics());
			OptimizedPlan op = pc.compile(plan);
			PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();

			return gen.getOptimizerPlanAsJSON(op);
		} finally {
			exec.stop();
		}
	}

	/**
	 * Return unoptimized plan as JSON.
	 * @return
	 */
	public static String getPlanAsJSON(Plan plan) {
		PlanJSONDumpGenerator gen = new PlanJSONDumpGenerator();
		List<DataSinkNode> sinks = PactCompiler.createPreOptimizedPlan(plan);
		return gen.getPactPlanAsJSON(sinks);
	}
}