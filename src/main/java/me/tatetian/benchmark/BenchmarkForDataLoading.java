package me.tatetian.benchmark;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import me.tatetian.cps.io.CascadingSampledDataOutputStream;
import me.tatetian.dataset.DataSet;
import me.tatetian.dataset.DataSetFactory;

public class BenchmarkForDataLoading extends Benchmark {
	private DataSet dataSet = null;
	
	public BenchmarkForDataLoading(String destFile, int numRecords) {
		this(destFile, numRecords, Benchmark.DEFAULT_NUM_REPEATS);
	}
	
	public BenchmarkForDataLoading(String destFile, int numRecords, int numRepeats) {
		super("Benchamark for Data Loading with or without Sampling", numRepeats);
		
		removeOldFile(destFile);
		
		dataSet = DataSetFactory.makeRepeatedString();
		dataSet.setNumReords(numRecords);
    addTask(new DataLoadingTaskWithoutSampling(destFile, dataSet));
    addTask(new DataLoadingTaskWithSampling(destFile + ".sampled", dataSet));
	}
	
	public DataSet getDataSet() {
		return dataSet;
	}
	
	private void removeOldFile(String destFile) {
		FileSystem fs;
		Configuration conf = new Configuration(); 
		try {
			fs = FileSystem.get(URI.create(destFile), conf);
			Path pathPattern = new Path(destFile + "*");
			FileStatus[] filesToRemove = fs.globStatus(pathPattern); 
			for(FileStatus f : filesToRemove) {
				fs.delete(f.getPath(), false);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
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
				int bufferSize = 4096 * 64;
				FSDataOutputStream out = fs.create(destPath, true, bufferSize);
				dataSet.dump(out);
       // out.hflush();
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
        //out.hflush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
