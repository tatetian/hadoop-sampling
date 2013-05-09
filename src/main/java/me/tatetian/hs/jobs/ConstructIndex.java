package me.tatetian.hs.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import me.tatetian.hs.index.Index;
import me.tatetian.hs.index.IndexFile;
import me.tatetian.hs.index.IndexMeta;
import me.tatetian.hs.index.IndexMetaFile;
import me.tatetian.hs.index.IndexUtil;
import me.tatetian.hs.jobs.meancalculation.CalculateMean;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils.NullOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
			// First, compare file names as primary key 
			String thisFileName = fileName.toString();
			String thatFileName = o.fileName.toString();
			int res = thisFileName.compareTo(thatFileName);
			// If equal, compare start position as secondary key
			if(res == 0) {
				long thisValue = start.get();
		    long thatValue = o.start.get();
		    res = (thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
			}
			return res;
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
		private IndexMeta meta = null;
		// TODO: remove hard-code buffer size of index
		private Index index = null;
		
		public InputSplitSummary() {
			meta = new IndexMeta();
			index = new Index(16 * 1024 * 1024);
		}
		public InputSplitSummary(IndexMeta meta, Index index) {
			setIndexMeta(meta);
			setIndex(index);
		}
		
		public IndexMeta getIndexMeta() { return meta; }
		public Index getIndex() { return index; }
		public void setIndexMeta(IndexMeta meta) { this.meta = meta; }
		public void setIndex(Index index) { this.index = index; }
		
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
		private InputSplitMeta inputSplitMeta = null;
		private InputSplitSummary inputSplitSummary = null;
		private IndexMeta meta = null;
		private Index index = null;
		private long realStart = -1;	// Note: start of split is not the position of first record
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(realStart < 0) realStart = key.get();
			
			String line = value.toString();
			int recordLen = line.length() + 1;
			meta.dataBlockLen += recordLen;
			meta.dataRecordNum += 1;
			index.add(recordLen);
		}
		
		/**
		 * Called once at the beginning of the task.
		 */
	  protected void setup(Context context) throws IOException, InterruptedException {
	  	// Get configuration
	  	Configuration conf 		= context.getConfiguration();
			// Get input file info
	  	FileSplit inputSplit 	= (FileSplit) context.getInputSplit();
			Path inputFile 				= inputSplit.getPath();
	  	// Init meta
	  	meta  = new IndexMeta();
	  	// Init index
	  	index = Index.createIndex(conf);
	  	// Init intermediate results
	  	inputSplitMeta 		= new InputSplitMeta(inputFile.toString(), realStart);
	  	inputSplitSummary = new InputSplitSummary(meta, index);
	  }
	  
		/**
	   * Called once at the end of the task.
	   */		
		@Override
	  protected void cleanup(Context context) throws IOException, InterruptedException {
			if(realStart < 0) return;
			
			meta.dataBlockOffset 	= realStart;
			inputSplitMeta.setStart(realStart);
			
			meta.indexBlockOffset = -1;			// determine later in reducer
			meta.indexBlockLen = -1;				// determine later in reducer
			// write intermidate result
			context.write(inputSplitMeta, inputSplitSummary);
		}
	}
	
	public static class ConstructIndexReducer extends Reducer<InputSplitMeta, InputSplitSummary, NullWritable, NullOutputStream> {		
		private String lastInputFile = null;
		private long	 lastStart = -1;
		private String indexOutputFile = null;
		private String metaOutputFile = null;
		
		private IndexFile.Writer indexWriter = null;
		private IndexMetaFile.Writer metaWriter = null;
		
		private Configuration conf = null;
		
		/**
	   * Called once at the start of the task.
	   */
	  protected void setup(Context context
	                       ) throws IOException, InterruptedException {
	  	conf = context.getConfiguration();
	  }
		
	  /**
	   * Called once at the end of the task.
	   */		
		@Override
	  protected void cleanup(Context context) throws IOException, InterruptedException {
			closeWriters();
		}
		
		private void closeWriters() throws IOException {
			if(indexWriter != null) indexWriter.close();
			if(metaWriter != null) 	metaWriter.close();
		}
	  
		@Override
		protected void reduce(InputSplitMeta key, Iterable<InputSplitSummary> values, Context context) throws IOException, InterruptedException {
			String currentInputFile = key.getFileName();
			long currentStart = key.getStart();
			// Prepare to write to new meta & index file
			if(!currentInputFile.equals(lastInputFile) ){
				Path inputPath 				= new Path(currentInputFile);
				Path indexOutputPath 	= IndexUtil.getIndexPath(inputPath);
				Path metaOutputPath 	= IndexUtil.getIndexMetaPath(inputPath);
				
				indexOutputFile = indexOutputPath.toString();
				metaOutputFile  = metaOutputPath.toString();
				
				closeWriters();
				
				indexWriter 	= new IndexFile.Writer(conf, indexOutputPath);
				metaWriter		= new IndexMetaFile.Writer(conf, metaOutputPath);
				
				lastInputFile = currentInputFile;
				lastStart 		= -1;
			}
			// Blocks should be sorted by start position
			assert(lastStart < currentStart);
			lastStart = currentStart;
			// Handle current block
			Iterator<InputSplitSummary> summaryIter = values.iterator();
			InputSplitSummary summary = null;
			Index index 	 = null;
			IndexMeta meta = null;
			while(summaryIter.hasNext()) {
				// there should be only one value
				assert(summary == null);
				
				summary = summaryIter.next();
				long indexBlockOffset = indexWriter.getPosition();
				index 	= summary.getIndex();
				meta		= summary.getIndexMeta();
				indexWriter.append(index);
				long indexBlockLen = indexWriter.getPosition() - indexBlockOffset;
				meta.indexBlockOffset = indexBlockOffset;
				meta.indexBlockLen = indexBlockLen;
				metaWriter.append(meta);
				System.out.println("meta :" + summary.getIndexMeta());
				System.out.println("index :" + summary.getIndex());
			}
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
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
	
		// Job
		Configuration conf = getConf();
		Job job = new Job(conf, "Index Construction");
    job.setJarByClass(ConstructIndex.class);
    job.setMapperClass(ConstructIndexMapper.class);
    job.setReducerClass(ConstructIndexReducer.class);
    //job.setCombinerClass(ConstructIndexReducer.class);
    job.setMapOutputKeyClass(InputSplitMeta.class);
    job.setMapOutputValueClass(InputSplitSummary.class);
    job.setPartitionerClass(ConstructIndexPartitioner.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullOutputStream.class);
    // Input
    Path input = new Path(args[0]);
    FileInputFormat.addInputPath(job,input);
    // Output
    // TODO: change outputDir
    Path outputDir = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, outputDir);
    // Remove old output
    Path[] indexOutputPaths = getIndexOutputPaths(job);
    Path[] indexMetaOutputPaths = getIndexMetaOutputPaths(job);
    removePaths(indexOutputPaths, conf);
    removePaths(indexMetaOutputPaths, conf);
   
		int success = (job.waitForCompletion(true) ? 0 : 1);
	
		return success;
	}
	
	private Path[] getIndexOutputPaths(JobContext job) {
		Path[] inputPaths = FileInputFormat.getInputPaths(job);
		Path[] outputIndexPaths = new Path[inputPaths.length];
		for(int i = 0; i < inputPaths.length; i++) 
			outputIndexPaths[i] = IndexUtil.getIndexPath(inputPaths[i]);
		return outputIndexPaths;
	}
	
	private Path[] getIndexMetaOutputPaths(JobContext job) {
		Path[] inputPaths = FileInputFormat.getInputPaths(job);
		Path[] outputIndexMetaPaths = new Path[inputPaths.length];
		for(int i = 0; i < inputPaths.length; i++) 
			outputIndexMetaPaths[i] = IndexUtil.getIndexMetaPath(inputPaths[i]);
		return outputIndexMetaPaths;
	}
	
	private void removePaths(Path[] paths, Configuration conf) throws IOException {
		if(paths == null || paths.length == 0) return;
		
		FileSystem fs = paths[0].getFileSystem(conf);
		for(Path p : paths) fs.delete(p, false);
	}
	
	private boolean renamePaths(Path[] paths, Configuration conf) throws IOException {
		if(paths == null || paths.length == 0) return false;
		
		FileSystem fs = paths[0].getFileSystem(conf);
		for(Path p : paths) {
			FileStatus[] samePrefix = fs.globStatus(p.suffix("-*"));
			if(samePrefix.length != 1) return false;	// we expect exact one output file
			fs.rename(samePrefix[0].getPath(), p);
		}
		return true;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new CalculateMean(), args);
		SequenceFileOutputFormat outputFormat = null;
		System.exit(res);
	}
}