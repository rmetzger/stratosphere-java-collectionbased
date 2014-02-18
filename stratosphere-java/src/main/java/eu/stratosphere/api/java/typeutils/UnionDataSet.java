package eu.stratosphere.api.java.typeutils;

import eu.stratosphere.api.java.DataSet;

public class UnionDataSet<T> extends DataSet<T> {
	private final DataSet<T> input1;
	private final DataSet<T> input2;

	public UnionDataSet(DataSet<T> input1, DataSet<T> input2) {
		super(input1.getExecutionEnvironment(), input1.getType());
		this.input1 = input1;
		this.input2 = input2;
	}
}
