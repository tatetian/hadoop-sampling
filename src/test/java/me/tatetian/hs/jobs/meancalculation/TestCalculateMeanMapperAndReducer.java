package me.tatetian.hs.jobs.meancalculation;

import java.util.ArrayList;
import java.util.List;

import me.tatetian.hs.jobs.meancalculation.CalculateMeanMapper;
import me.tatetian.hs.jobs.meancalculation.CalculateMeanReducer;
import me.tatetian.hs.jobs.meancalculation.Pair;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestCalculateMeanMapperAndReducer {
	MapDriver<LongWritable, Text, NullWritable, Pair> mapDriver;
  ReduceDriver<NullWritable, Pair, Text, DoubleWritable> reduceDriver;

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
    mapDriver.withOutput(NullWritable.get(), new Pair(1, 2.2));
    mapDriver.runTest();
  }

	@Test
	public void testReducer() {
	  List<Pair> values = new ArrayList<Pair>();
	  values.add(new Pair(3, 3.0));
	  values.add(new Pair(7, 1.0));
	  reduceDriver.withInput(NullWritable.get(), values);
	  reduceDriver.withOutput(new Text("count"), new DoubleWritable(10));
	  reduceDriver.withOutput(new Text("sum"), new DoubleWritable(4.0));
	  reduceDriver.withOutput(new Text("average"), new DoubleWritable(0.4));
	  reduceDriver.runTest();
	}
}
