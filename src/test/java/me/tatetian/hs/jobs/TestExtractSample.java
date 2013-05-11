package me.tatetian.hs.jobs;

import java.io.IOException;

import me.tatetian.hs.sampler.TestSamplerUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestExtractSample {
	// For the convenience of test, it's desirable to have block size smaller
	private static final int BLOCK_SIZE = 32 * 1024 * 1024;
	
	private static final float SAMPLING_RATIO = 0.01f;
  private Configuration conf;
  private Path input;
  private Path output;
  private FileSystem fs;
  
	// For the convenience of test, it's desirable to have block size smaller
//	private static final int BLOCK_SIZE = 8 * 1024 * 1024;
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    conf.setInt("fs.local.block.size", BLOCK_SIZE);
    
    input = new Path("tmp/test_mean_calculation.data");
    output = new Path("tmp/sample_output");
    	
    fs = FileSystem.getLocal(conf);
    // overwrite input file
    //double mean = 80, sd = 40; 
		//DataSet dataSet = DataSetFactory.makeNormalDist(mean, sd);
		//dataSet.setNumReords(1000 * 1000 * 50 * 8);
		//CascadingSampledDataOutputStream out = CascadingSampledDataOutputStream.create(fs, input);
		//dataSet.dump(out);
		// delete old output file
    fs.delete(output, true);
  }

  @Test
  public void test() throws Exception {
  	ExtractSample constructSample = new ExtractSample();
  	constructSample.setConf(conf);
    
    int exitCode = constructSample.run(new String[] {
    																			Float.toString(SAMPLING_RATIO),
    																			input.toString(), 
    																			output.toString()});
    Assert.assertEquals(0, exitCode);
    
    validateOutput();
  }
  
  void validateOutput() throws IOException {
  	FileStatus inputStatus = fs.getFileStatus(input);
  	long inputLen = inputStatus.getLen();
  	FileStatus outputStatus = fs.getFileStatus(output.suffix("/part-r-00000"));
  	long outputLen = outputStatus.getLen();
  	TestSamplerUtil.assertApproxEquals(inputLen * SAMPLING_RATIO, outputLen);
  }
}
