package me.tatetian.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import me.tatetian.jobs.CalculateMean.CalculateMeanMapper;
import me.tatetian.jobs.CalculateMean.CalculateMeanReducer;
import me.tatetian.jobs.CalculateMean.Pair;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCalculateMeanMapperAndReducer {
	MapDriver<LongWritable, Text, Text, Pair> mapDriver;
  ReduceDriver<Text, Pair, Text, DoubleWritable> reduceDriver;

  @Before
  public void setUp() {
  	CalculateMeanMapper 	mapper = new CalculateMeanMapper();
  	CalculateMeanReducer reducer = new CalculateMeanReducer();
    mapDriver = MapDriver.newMapDriver(mapper);;
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }

  @Test
  public void testMapper() {
    mapDriver.withInput(new LongWritable(), new Text("2.2"));
    mapDriver.withOutput(new Text(), new Pair(1, 2.2));
    mapDriver.runTest();
  }

	@Test
	public void testReducer() {
	  List<Pair> values = new ArrayList<Pair>();
	  values.add(new Pair(3, 3.0));
	  values.add(new Pair(7, 1.0));
	  reduceDriver.withInput(new Text(), values);
	  reduceDriver.withOutput(new Text("count"), new DoubleWritable(10));
	  reduceDriver.withOutput(new Text("sum"), new DoubleWritable(4.0));
	  reduceDriver.withOutput(new Text("average"), new DoubleWritable(0.4));
	  reduceDriver.runTest();
	}
	
//	@Test
//	public void test() throws Exception {
//		JobConf conf = new JobConf();
//		conf.set("fs.default.name", "file:///");
//		conf.set("mapred.job.tracker", "local");
//		
//		Path input = new Path("");
//		Path output = new Path("");
//		
//		FileSystem fs = FileSystem.getLocal(conf);
//		fs.delete(output, true);	// delete old file
//		
//		CalculateMean driver = new CalculateMean();
//		driver.setConf(conf);
//		
//		int exitCode = driver.run(new String[] {
//			input.toString(), output.toString() });
//		Assert.assertEquals(0, exitCode);
//		
//		checkOutput(conf, output);
//	}
//	
//	private void checkOutput(JobConf conf, Path output) {
//		
//	}
}
