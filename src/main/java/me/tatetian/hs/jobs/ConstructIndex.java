package me.tatetian.hs.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import me.tatetian.hs.index.Index;
import me.tatetian.hs.index.IndexMeta;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils.NullOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConstructIndex extends Configured implements Tool  {
	public static class InputSplitMeta implements WritableComparable<InputSplitMeta> {
		private Text fileName = new Text();
		private LongWritable start = new LongWritable(-1);
		private int hashValue = 0;
	
		public InputSplitMeta() {}
		
		public InputSplitMeta(String fileName, long start) {
			setFileName(fileName);
			setStart(start);
		}
		
		public void setFileName(String fileName) {
			hashValue = 0;
			this.fileName.set(fileName);
		}
		
		public void setStart(long start) {
			hashValue = 0;
			this.start.set(start);
		}
		
		public String getFileName() {
			return fileName.toString();
		}
		
		public long getStart() {
			return start.get();
		}
		
		@Override
		public void write(DataOutput out) throws IOException {
			hashValue = 0;
			fileName.write(out);
			start.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			fileName.readFields(in);
			start.readFields(in);
		}

		@Override
		public int compareTo(InputSplitMeta o) {
			long thisValue = start.get();
	    long thatValue = o.start.get();
	    return (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
		}
		
		@Override
		public int hashCode() {
			if(hashValue == 0) {
				long[] vals = new long[]{fileName.hashCode(), start.hashCode()}; 
				hashValue = Arrays.hashCode(vals);
			}
			return hashValue;
	  }
	}
	
	public static class InputSplitSummary implements Writable {
		private IndexMeta meta = new IndexMeta();
		// TODO: remove hard-code buffer size of index
		private Index index = new Index(16 * 1024 * 1024);
		
		public IndexMeta getIndexMeta() { return meta; }
		public Index getIndex() { return index; }
		
		@Override
		public void write(DataOutput out) throws IOException {
			meta.write(out);
			index.write(out);
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			meta.readFields(in);
			index.readFields(in);
		}
	}
	
	public static class ConstructIndexMapper extends Mapper<LongWritable, Text, InputSplitMeta, InputSplitSummary> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
		}
		
		/**
		 * Called once at the beginning of the task.
		 */
	  protected void setup(Context context) throws IOException, InterruptedException {
	  	
	  }
	  
		/**
	   * Called once at the end of the task.
	   */		
		@Override
	  protected void cleanup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
	  	String fileName = conf.get("mapreduce.map.input.file");
	  	long start = conf.getLong("mapreduce.map.input.start", -1);
	  	System.out.println("mapper: fileName = " + fileName);
	  	System.out.println("mapper: start = " + start);
			InputSplitMeta meta = new InputSplitMeta(fileName, start);
			InputSplitSummary summary = new InputSplitSummary();
			context.write(meta, summary);
		}
	}
	
	public static class ConstructIndexReducer extends Reducer<InputSplitMeta, InputSplitSummary, NullWritable, NullOutputStream> {		
		private MultipleOutputs mo = null;	
		
		/**
	   * Called once at the start of the task.
	   */
	  protected void setup(Context context
	                       ) throws IOException, InterruptedException {
	  	mo = new MultipleOutputs(context);
	  }
		
		@Override
		protected void reduce(InputSplitMeta key, Iterable<InputSplitSummary> values, Context context) throws IOException, InterruptedException {
			int blockId = 0;
		}
	}
	
	public static class ConstructIndexPartitioner 
											extends Partitioner<LongWritable, InputSplitMeta> 
											implements Configurable {
		private Configuration conf = null;
		
		@Override
		public int getPartition(LongWritable key, InputSplitMeta value,
				int numPartitions) {
			return 0;
		}

		@Override
		public void setConf(Configuration conf) {
			System.out.println("!!!!!!!!!!!set configuration!!!!!!!!!!!!!");
			System.out.println(conf);
			this.conf = conf;
		}

		@Override
		public Configuration getConf() {
			return conf;
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

		Job job = new Job(conf, "Index Construction");
    job.setJarByClass(ConstructIndex.class);
    job.setMapperClass(ConstructIndexMapper.class);
    job.setReducerClass(ConstructIndexReducer.class);
    job.setCombinerClass(ConstructIndexReducer.class);
    job.setMapOutputKeyClass(InputSplitMeta.class);
    job.setMapOutputValueClass(InputSplitSummary.class);
    job.setPartitionerClass(ConstructIndexPartitioner.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullOutputStream.class);
    Path input = new Path(args[0]);
    FileInputFormat.addInputPath(job,input);
    Path output = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, output);
	
    int pathIndex = 0;
    Path[] inputPaths = FileInputFormat.getInputPaths(job);
    for(Path path : inputPaths) {
    	System.out.println("input path = " + path);
    	MultipleOutputs.addNamedOutput(job, "meta",// + pathIndex, 
    		SequenceFileOutputFormat.class, IntWritable.class, IndexMeta.class);
    	MultipleOutputs.addNamedOutput(job, "index",// + pathIndex, 
    		SequenceFileOutputFormat.class, IntWritable.class, Index.class);
    	pathIndex ++;
    }
    
		int success = (job.waitForCompletion(true) ? 0 : 1);
		// If successful, rename the output files
		if(success == 0) {
			Path outputDir = FileOutputFormat.getOutputPath(job);
			System.out.println("output dir = " + outputDir);
		}
		
		return success;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMean(), args);
		System.exit(res);
	}
}