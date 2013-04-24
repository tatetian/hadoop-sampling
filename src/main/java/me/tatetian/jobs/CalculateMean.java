package me.tatetian.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.math.complex.Complex;
import org.apache.commons.math.util.MathUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalculateMean extends Configured implements Tool {
	public static class Pair implements Writable{
		private int count = 0;
		private double sum = 0.0;
		private int hashValue = 0;	// 0 indicates the hash value is not calculated yet
		
		public Pair() {
		}
		
		public Pair(int count, double sum) {
			this.count = count;
			this.sum = sum;
		}
		
		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Pair))
	      return false;
	    Pair p = (Pair)o;
	    return this.count == p.count && this.sum == p.sum;
		}
		
		@Override
	  public int hashCode() {
			if(hashValue == 0) {
				long[] vals = new long[]{count, Double.doubleToLongBits(sum)}; 
				hashValue = Arrays.hashCode(vals);
			}
			return hashValue;
	  }
		
		@Override
		public String toString() {
			return "<" + count + "," + sum + ">";
		}
		
		public int getCount() {
			return count;
		}
		
		public double getSum() {
			return sum;
		}
		
		public double getAverage() {
			return sum / count;
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(count);
			out.writeDouble(sum);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			count = in.readInt();
			sum = in.readDouble();
		}
	}
	
	public static class CalculateMeanMapper extends Mapper<LongWritable, Text, Text, Pair> {
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

	public static class CalculateMeanReducer extends Reducer<Text, Pair, Text, DoubleWritable> {
		@Override
		protected void reduce(Text key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			double sum = 0.0;
			for(Pair v : values) {
				count += v.getCount();
				sum += v.getSum();
			}
			context.write(new Text("count"), new DoubleWritable(count));
			context.write(new Text("sum"), new DoubleWritable(sum));
			context.write(new Text("average"), new DoubleWritable(sum / count));
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Configuration conf = getConf();

		Job job = new Job(conf, "Mean Calculation");
    job.setJarByClass(CalculateMean.class);
    job.setMapperClass(CalculateMeanMapper.class);
    job.setReducerClass(CalculateMeanReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Pair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setNumReduceTasks(1);
    Path input = new Path(args[0]);
    FileInputFormat.addInputPath(job,input);
    Path output = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, output);
		 
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMean(), args);
		System.exit(res);
	}
}
