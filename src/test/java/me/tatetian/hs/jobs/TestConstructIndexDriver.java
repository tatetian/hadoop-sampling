package me.tatetian.hs.jobs;

import java.io.BufferedReader;
import java.io.IOException;

import me.tatetian.hs.index.Index;
import me.tatetian.hs.index.IndexFile;
import me.tatetian.hs.index.IndexMeta;
import me.tatetian.hs.index.IndexMetaFile;
import me.tatetian.hs.index.IndexUtil;
import me.tatetian.hs.io.FileUtil;

import org.apache.commons.math.random.RandomDataImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConstructIndexDriver {
	// For the convenience of test, it's desirable to have block size smaller
	private static final int BLOCK_SIZE = 32 * 1024 * 1024;
	
	private Configuration conf;
  private Path input;
  private Path output;
  private FileSystem fs;
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    // TODO: this parameter seems not to take effect
    conf.setInt("fs.local.block.size", BLOCK_SIZE);
    
    input = new Path("tmp/test_construct_index.data");
    output = new Path("tmp/test_construct_index"); 
    
    fs = FileSystem.getLocal(conf);
    // remove old output
    fs.delete(output, true);
  }

  @Test
  public void test() throws Exception {
  	// Construct input on which the index will be built
  	int numRecords = 1024 * 1024;
  	int averageSize = 256;
  	constructInput(numRecords, averageSize);
  	// Run index construction job
  	ConstructIndex constructIndex = new ConstructIndex();
  	constructIndex.setConf(conf); 
    int exitCode = constructIndex.run(new String[] {input.toString(), output.toString()});
    Assert.assertEquals(0, exitCode);
    // Validate the index is as expected
    validateOuput();
  }

  private void constructInput(int numRecords, int averageSize) throws IOException {
  	// Create input file
		FSDataOutputStream out = FileUtil.createFile(input, conf);
		// The sizes of record follow normal distribution
		
		RandomDataImpl sizeDist = new RandomDataImpl();
		// Write records of input file
		for(int i = 0; i < numRecords; i++) {
			double size = sizeDist.nextGaussian(averageSize, 0.5 * averageSize);
			// write a record of random size
			int recordSize = Math.max( 1, (int) Math.ceil(size) );
			for(int j = 0; j < recordSize; j++)
				out.write('*');
			out.write('\n');
		}
		out.close();
  }
  
  private void validateOuput() throws IOException {
    // Get output path
  	Path indexPath 			= IndexUtil.getIndexPath(input);
    Path indexMetaPath 	= IndexUtil.getIndexMetaPath(input);
    // Init reader
    IndexFile.Reader 		 indexReader 		 = null;
    IndexMetaFile.Reader indexMetaReader = null;
    BufferedReader			 inputReader		 = null;
		inputReader			= FileUtil.readTextFile(input, conf);
		indexReader 		= new IndexFile.Reader(conf, indexPath);
		indexMetaReader = new IndexMetaFile.Reader(conf, indexMetaPath);
		Index index 		= new Index(BLOCK_SIZE / 8);
		IndexMeta meta  = new IndexMeta();
		// Iterate blocks
		long dataBlockOffset = 0;
		while(indexMetaReader.next(meta)) {
			Assert.assertEquals(true, indexReader.next(index));
			Assert.assertEquals(dataBlockOffset, meta.dataBlockOffset);
			Assert.assertEquals(indexReader.getPosition(), meta.indexBlockOffset + meta.indexBlockLen);
		
			
			long dataBlockLen = 0;
			int recordsInBlock = meta.dataRecordNum;
			String record = null;
			int recordLen = 0;
			for(int recordIndex = 0; recordIndex < recordsInBlock; recordIndex++) {
				record = inputReader.readLine();
				recordLen = record.length() + 1;
				dataBlockOffset += recordLen;
				dataBlockLen += recordLen;
				Assert.assertEquals( index.get(recordIndex), recordLen);
			}
			Assert.assertEquals(dataBlockLen, meta.dataBlockLen);
		}
		// If we reached EOF of meta file, then so should input/index file
		Assert.assertEquals(null, inputReader.readLine());
		Assert.assertEquals(false, indexReader.next(index));
		// Close
  	inputReader.close();
  	indexReader.close();
  	indexMetaReader.close();
  }

}