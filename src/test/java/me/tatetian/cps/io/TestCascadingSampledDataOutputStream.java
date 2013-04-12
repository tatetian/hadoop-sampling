package me.tatetian.cps.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import junit.framework.Assert;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class TestCascadingSampledDataOutputStream extends junit.framework.TestCase {
	//===========================================================================
	// Test Cases
	//===========================================================================
	public void testLocalCreate() throws IOException {
		String dest = "file:/tmp/hadoop-sampling/test_cascading_sampling.data.sampled";
		int size = 1024;
		create(dest, size);
		validate(dest, size);
	}
	
//	public void testHDFSCreate() throws IOException {
//		String dest = "hdfs://localhost/hadoop-sampling/test_cascading_sampling.data.sampled";
//		int size = 1024;
//		create(dest, size);
//		validate(dest, size);
//	}
//	

	//===========================================================================
	// Helper functions
	//===========================================================================
	
	/**
	 * Get the proper file system object according to URI of file destination
	 * */
	private FileSystem getFS(String dest) throws IOException {
		Configuration conf = new Configuration(); 
		conf.setInt("dfs.block.size", 10);	// Make it much smaller than default 
    //conf.addResource(new Path("/opt/hadoop-0.20.0/conf/core-site.xml"));
    //conf.addResource(new Path("/opt/hadoop-0.20.0/conf/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(URI.create(dest), conf);
		return fs;
	}
	
	/**
	 * Create the file and write a sequence number 0, 1, ..., size-1 to it
	 * */
	private void create(String dest, int size) throws IOException {
		FileSystem fs = getFS(dest);
		Path path = new Path(dest);
		CascadingSampledDataOutputStream out = CascadingSampledDataOutputStream.create(fs, path); 
		for(long i = 0; i < size; i++) {
			out.writeBytes(Long.toString(i));
			out.writeSeparator();
		}
		out.close();
	}
	
	/**
	 * Read the file to check if its content is 0, 1, ..., size-1
	 * */
	private void validate(String dest, int size) throws IOException {
		FileSystem fs = getFS(dest);
		Path basePath = new Path(dest);
		
		int level = 0;
		int lastSize = size;
		while(true) {
			// Concat path
			Path path = level == 0 ? basePath : basePath.suffix("." + Integer.toString(level));
			// Check path for termination
			if(!fs.exists(path)) break;
			// Prepare reader
			FSDataInputStream in  = fs.open(path);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			// Count lines
			int lineCount = 0;
			while(reader.readLine() != null) lineCount ++;
			// Validate count
			if(level == 0)
				Assert.assertEquals(size, lineCount);
			else {
				// Given sample size n and sampling ratio k, the sample size of 
				// Bernoulli sampling follows Binomial distribution, the mean of which is
				//		e = n * k
				// and the standard variance of which is
				// 		v = sqrt(n*k*(1-k)).
				// So we expect the lineCount to be within interval [e - 3v, e + 3v] with high confidence. 
				double n = lastSize, k = 1.0 / fs.getConf().getInt("cps.sampling.ratio", 16);		
				double v = Math.sqrt(n * k * (1-k));
				double e = n * k;
				Assert.assertEquals(true, lineCount >= e - 4 * v);
				Assert.assertEquals(true, lineCount <= e + 4 * v);
			}
			
			// Next level
			lastSize = lineCount;
			level ++;
		}
	}
}
