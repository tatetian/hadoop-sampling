package me.tatetian.hs.jobs.meancalculation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateMeanMapper extends Mapper<LongWritable, Text, Text, Pair> {
	protected Text emptyKey = new Text();
	
	private double sum = 0;
	private int count = 0;

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		count ++;
		sum += Double.parseDouble(line);
	}
	
	@Override
	/**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
		context.write(emptyKey, new Pair(count, sum));
	}
}