package eu.stratosphere.api.java.collectionbased;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;

public class InMemoryEnviroment extends ExecutionEnvironment {

	private Object[] data;
	@Override
	public JobExecutionResult execute(String jobName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getExecutionPlan() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public <X> DataSet<X> fromElements(X... data) {
		this.data = data;
		return new InMemoryDataSet(context, type, data);
	}

}
