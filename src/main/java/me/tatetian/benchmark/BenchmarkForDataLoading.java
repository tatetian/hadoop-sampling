package me.tatetian.benchmark;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import me.tatetian.cps.io.CascadingSampledDataOutputStream;
import me.tatetian.dataset.DataSet;
import me.tatetian.dataset.NumericDataSet;

public class BenchmarkForDataLoading extends Benchmark {
	public BenchmarkForDataLoading(String destFile, int numRecords) {
		this(destFile, numRecords, Benchmark.DEFAULT_NUM_REPEATS);
	}
	
	public BenchmarkForDataLoading(String destFile, int numRecords, int numRepeats) {
		super("Benchamark for Data Loading with or without Sampling", numRepeats);
		
		DataSet ds = new NumericDataSet(numRecords);
    addTask(new DataLoadingTaskWithoutSampling(destFile, ds));
    addTask(new DataLoadingTaskWithSampling(destFile + ".sampled", ds));
	}
	
	private static abstract class DataLoadingTask extends Task {
		protected String destFile;
		protected DataSet dataSet;
		
		public DataLoadingTask(String name, String destFile, DataSet dataSet) {
			super(name);
			this.destFile = destFile;
			this.dataSet = dataSet;
		}
	}
	
	private static class DataLoadingTaskWithoutSampling extends DataLoadingTask {
		public DataLoadingTaskWithoutSampling(String destFile, DataSet dataSet) {
			super("Loading File without Sampling", destFile, dataSet);
		}

		@Override
		protected void doRun() {
			Configuration conf = new Configuration(); 
			//conf.setInt("dfs.block.size", 10);	// Make it much smaller than default 
	    //conf.addResource(new Path("/opt/hadoop-0.20.0/conf/core-site.xml"));
	    //conf.addResource(new Path("/opt/hadoop-0.20.0/conf/hdfs-site.xml"));
			FileSystem fs;
			try {
				fs = FileSystem.get(URI.create(destFile), conf);
				Path destPath = new Path(destFile);
				FSDataOutputStream out = fs.create(destPath);
				dataSet.dump(out);
        out.hflush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	private static class DataLoadingTaskWithSampling extends DataLoadingTask {
		public DataLoadingTaskWithSampling(String destFile, DataSet dataSet) {
			super("Loading File with Sampling", destFile, dataSet);
		}

		@Override
		protected void doRun() {
			Configuration conf = new Configuration(); 
			//conf.setInt("dfs.block.size", 10);	// Make it much smaller than default 
	    //conf.addResource(new Path("/opt/hadoop-0.20.0/conf/core-site.xml"));
	    //conf.addResource(new Path("/opt/hadoop-0.20.0/conf/hdfs-site.xml"));
			FileSystem fs;
			try {
				fs = FileSystem.get(URI.create(destFile), conf);
				Path destPath = new Path(destFile);
				CascadingSampledDataOutputStream out = CascadingSampledDataOutputStream.create(fs, destPath);
				dataSet.dump(out);
        out.hflush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
