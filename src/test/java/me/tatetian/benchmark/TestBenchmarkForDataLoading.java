package me.tatetian.benchmark;

import org.junit.Ignore;
import org.junit.Test;

public class TestBenchmarkForDataLoading {
	private final int numRecords = 1000 * 500 * 5 ;//* 10;//* 5 * 5;//* 10;//0 * 5;
	
	@Test
	public void testLocal() {
		String destFile = "file:/tmp/hadoop-sampling/test_cascading_sampling.data";
		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords, 3);
		for(int i = 1; i <= 20; i+= 2) {
			benchmark.getDataSet().setNumReords(numRecords * i);
			benchmark.setDescription("# of records = " + numRecords * i);
			benchmark.run();
		}
	}
	
	@Test @Ignore
	public void testHDFS() {
		String destFile = "hdfs://localhost/hadoop-sampling/test_cascading_sampling.data";
		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords);
		benchmark.run();
	}
}
