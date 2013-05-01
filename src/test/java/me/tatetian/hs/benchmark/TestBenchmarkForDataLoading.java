package me.tatetian.hs.benchmark;

import me.tatetian.hs.benchmark.BenchmarkForDataLoading;

import org.junit.Ignore;
import org.junit.Test;

public class TestBenchmarkForDataLoading {
	//private final int numRecords = 1000 * 1000* 27;// 8.6GB, 2 sampling level, 16% overhead
	private final int numRecords = 1000 ;//* 100; 
	
	@Test
	public void testLocal() {
		String destFile = "file:/tmp/hadoop-sampling/test_cascading_sampling.data";
		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords, 1);
		for(int i = 1; i <= 1; i+= 2) {
			benchmark.getDataSet().setNumReords(numRecords * i);
			benchmark.setDescription("# of records = " + numRecords * i);
			benchmark.run();
			System.out.println();
		}
	}
	
	@Test @Ignore
	public void testHDFS() {
		String destFile = "hdfs://localhost/hadoop-sampling/test_cascading_sampling.data";
		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords);
		benchmark.run();
	}
}
