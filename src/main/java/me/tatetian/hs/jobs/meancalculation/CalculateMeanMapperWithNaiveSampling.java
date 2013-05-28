package me.tatetian.hs.jobs.meancalculation;

import java.io.IOException;
import java.util.Random;

import me.tatetian.hs.io.IOConfigKeys;
import me.tatetian.hs.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class CalculateMeanMapperWithNaiveSampling extends Mapper<LongWritable, Text, NullWritable, Pair> {
	protected Text emptyKey = new Text();
	
	private double sum = 0;
	private int count = 0;
	private Random random = new Random();
	private float samplingRatio = 1.0f;
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if(random.nextFloat() < samplingRatio) {
			//String line = value.toString();
			count ++;
			//sum += Double.parseDouble(line);
			sum += value.getLength();
		}
	}

	/**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context
                       ) throws IOException, InterruptedException {
  	Configuration conf = context.getConfiguration();
  	samplingRatio = conf.getFloat(IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO, 
  															  IOConfigKeys.HS_INDEXED_RECORD_READER_SAMPLING_RATIO_DEFAULT); 
  }
	
	@Override
	/**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context
                         ) throws IOException, InterruptedException {
		context.write(NullWritable.get(), new Pair(count, sum));
	}
}
