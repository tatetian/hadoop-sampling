package me.tatetian.hs.dataset;

import java.io.IOException;
import java.net.URI;

import me.tatetian.hs.dataset.DataSet;
import me.tatetian.hs.dataset.DataSetFactory;
import me.tatetian.hs.io.CascadingSampledDataOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestDataSetFactory {
	@Test
	public void generateMeanDataSet() {
		double mean = 80, sd = 10; 
		DataSet dataSet = DataSetFactory.makeNormalDist(mean, sd);
		String destFile = "file:/tmp/hadoop-sampling/test_mean_dataset.data.sampled"; 
		Configuration conf = new Configuration(); 
		FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(destFile), conf);
			Path destPath = new Path(destFile);
			CascadingSampledDataOutputStream out = CascadingSampledDataOutputStream.create(fs, destPath);
			dataSet.dump(out);
		} catch (IOException e) {
			e.printStackTrace();
		}
	} 
}
