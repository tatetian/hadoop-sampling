package me.tatetian.benchmark;

public class TestBenchmarkForDataLoading extends junit.framework.TestCase {
	private final int numRecords = 1000 * 1000 ;
	
	public void testLocal() {
		String destFile = "file:/tmp/hadoop-sampling/test_cascading_sampling.data";
		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords);
		benchmark.run();
	}
	
//	public void testHDFS() {
//		String destFile = "hdfs://localhost/hadoop-sampling/test_cascading_sampling.data";
//		BenchmarkForDataLoading benchmark = new BenchmarkForDataLoading(destFile, numRecords);
//		benchmark.run();
//	}
}
