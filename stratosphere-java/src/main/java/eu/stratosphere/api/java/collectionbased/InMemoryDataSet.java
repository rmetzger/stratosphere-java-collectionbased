package eu.stratosphere.api.java.collectionbased;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.operators.MapOperator;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class InMemoryDataSet extends DataSet {
	
	private Object[] data;
	
	protected InMemoryDataSet(ExecutionEnvironment context, TypeInformation type, Object[] data) {
		super(context, type);
	}

	
	@Override
	public MapOperator map(MapFunction mapper) {
		for(int i = 0; i < data.length; i++) {
			mapper.map(data[i]);
		}
	}
	
}
