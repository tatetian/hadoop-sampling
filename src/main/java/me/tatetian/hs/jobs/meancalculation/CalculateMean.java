package me.tatetian.hs.jobs.meancalculation;

import me.tatetian.hs.io.Text;
import me.tatetian.hs.io.TextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CalculateMean extends Configured implements Tool {
	private float samplingRatio = -1;
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2 && args.length != 3) {
			System.err.printf("mean <input> <output> [<sampling_ratio>]\n");
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		if(args.length == 3) {
			samplingRatio = Float.parseFloat(args[2]);
			if(samplingRatio <= 0 || samplingRatio > 1) {
				System.err.println("<sampling_ratio> must be between 0 and 1");
				return -1;
			}
		}
		
		Configuration conf = getConf();
		if(samplingRatio > 0) {
			conf.setFloat("cps.sampling.ratio", samplingRatio);
		}
		
		Job job = new Job(conf, "Mean Calculation");
    job.setJarByClass(CalculateMean.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(CalculateMeanMapper.class);
    job.setReducerClass(CalculateMeanReducer.class);
    job.setCombinerClass(CalculateMeanCombiner.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Pair.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setNumReduceTasks(1);
    Path input = new Path(args[0]);
    FileInputFormat.addInputPath(job,input);
    Path output = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, output);
    
    customConfiguration(job);
    
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	protected void customConfiguration(Job job) {
		// no operation
		String msg = "!!!!!!with basic!!!!!!";
		System.out.println(msg);
		System.err.println(msg);
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMean(), args);
		System.exit(res);
	}
}
