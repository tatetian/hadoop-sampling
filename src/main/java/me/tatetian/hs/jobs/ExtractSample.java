package me.tatetian.hs.jobs;

import java.io.IOException;

import me.tatetian.hs.io.IndexedTextInputFormat;
import me.tatetian.hs.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExtractSample  extends Configured implements Tool {
	public static class ExtractSampleMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(NullWritable.get(), value);
		}
	}
	
	public static class ExtractSampleReducer extends Reducer<NullWritable, Text, NullWritable, Text> {		
		@Override
		protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
			for(Text value : values) {
				context.write(key, value);
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		// Job
		Configuration conf = getConf();
		Job job = new Job(conf, "Sample Extraction");
    job.setJarByClass(ExtractSample.class);
    job.setInputFormatClass(IndexedTextInputFormat.class);
    job.setMapperClass(ExtractSampleMapper.class);
    job.setReducerClass(ExtractSampleReducer.class);
    job.setMapOutputKeyClass(NullWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    // Input
    Path input = new Path(args[0]);
    FileInputFormat.addInputPath(job,input);
    // Output
    // TODO: change outputDir
    Path outputDir = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputDir);
   
		int success = (job.waitForCompletion(true) ? 0 : 1);
	
		return success;
	}

	public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new ExtractSample(), args);
    System.exit(res);
  }
}
