package me.tatetian.hs.io;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import me.tatetian.hs.index.IndexFile;
import me.tatetian.hs.index.IndexMetaFile;
import me.tatetian.hs.index.IndexUtil;
import me.tatetian.hs.jobs.ConstructIndex;

import org.apache.commons.math.random.RandomDataImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestIndexedRecordReader {
	private static final int numRecords = 2048;
	private static final int BLOCK_SIZE = 8 * 1024 * 1024;
	
	private Configuration conf = null;
	private Path input = null;
	private FileSystem fs = null;

	@Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    conf.setInt("dfs.blocksize", BLOCK_SIZE);
    conf.setFloat("cps.sampling.ratio", 1.0f);
    
    input = new Path("tmp/test_indexed_text_record_reader.data");
    
    fs = FileSystem.getLocal(conf);
  }
	
	@Test
	public void test() throws IOException, InterruptedException, ClassNotFoundException {
		List<String> recordsWritten 	= new ArrayList<String>(),
								 recordsRead			= new ArrayList<String>();
		writeTextRecords(recordsWritten);
		constructInputIndex();
		readTextRecords(recordsRead);
		validateResult(recordsWritten, recordsRead);
	}
	
	private void writeTextRecords(List<String> recordsWritten) throws IOException {
		// Init streams
		FSDataOutputStream out = FileUtil.createFile(input, conf);
		// Write data and create index
		double avgLen = 100; double sdLen = 50;
		RandomDataImpl random = new RandomDataImpl();
		for(int i = 0; i < numRecords; i++) {
			int len = (int) Math.ceil(random.nextGaussian(avgLen, sdLen));
			StringBuilder sb = new StringBuilder();
			for(int j = 0; j < len - 1; j++) 
				sb.append('*');
			String record = sb.toString();
			out.write(record.getBytes());			
			out.write('\n');
			recordsWritten.add(record);
		}
		// Close streams
		out.close();
	}
	
	private void constructInputIndex() throws IOException, InterruptedException, ClassNotFoundException {
    Path output = new Path("tmp/test_indexed_text_record_reader"); 
    // remove old output
    fs.delete(output, true);
    
  	ConstructIndex constructIndex = new ConstructIndex();
  	constructIndex.setConf(conf); 
    int exitCode = constructIndex.run(new String[] {input.toString(), output.toString()});
    Assert.assertEquals(0, exitCode);
	}
	
	private void readTextRecords(List<String> recordsRead) throws IOException, InterruptedException {
		IndexedRecordReader reader = getIndexedRecordReader();
		
		Text record = null;
		while(reader.nextKeyValue()) {
			record = reader.getCurrentValue();
			recordsRead.add(record.toString());
		}
		reader.close();
	}
	
	private void validateResult(List<String> recordsWritten, List<String> recordsRead) {
		Assert.assertEquals(recordsWritten.size(), recordsRead.size());
		
		int size = recordsWritten.size();
		assert(size > 0);
		
		for(int i = 0; i < size; i++) {
			String written = recordsWritten.get(i);
			String read		 = recordsRead.get(i);
			Assert.assertEquals(written, read);
		}
	}
	
	private IndexedRecordReader getIndexedRecordReader() throws IOException {
		Job job = new Job(conf, "readTextRecords");
		job.setJobID(new JobID("readTextRecords", 1));
		
		IndexedTextInputFormat format = new IndexedTextInputFormat();
		IndexedTextInputFormat.setInputPaths(job, input);
		boolean splitable = false; // make sure the whole file is a single split
		List<InputSplit> splits = format.getSplits(job, splitable);
		
		assert(splits.size() == 1);
		
		boolean trimCR = true;
		IndexedRecordReader reader = new IndexedRecordReader(trimCR);
		reader.initialize(splits.get(0), 
										  new TaskAttemptContextImpl(conf, //null) );
										  		new TaskAttemptID(
										  				new TaskID(job.getJobID(), TaskType.MAP, 1), 1)));
		return reader;
	}
}
