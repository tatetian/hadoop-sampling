package me.tatetian.benchmark;

public class TestBenchmarkForDataLoading extends junit.framework.TestCase {
	private final int numRecords = 1000 * 500 * 10;//* 5 * 5;//* 10;//0 * 5;
	
	public void testLocal() {
		String destFile = "file:/tmp/hadoop-sampling/test_cascading_sampling.data";
		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords, 2);
		benchmark.run();
	}
	
//	public void testHDFS() {
//		String destFile = "hdfs://localhost/hadoop-sampling/test_cascading_sampling.data";
//		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords);
//		benchmark.run();
//	}
}
