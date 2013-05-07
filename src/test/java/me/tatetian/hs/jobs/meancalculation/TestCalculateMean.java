package me.tatetian.hs.jobs.meancalculation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import me.tatetian.hs.dataset.DataSet;
import me.tatetian.hs.dataset.DataSetFactory;
import me.tatetian.hs.io.CascadingSampledDataOutputStream;
import me.tatetian.hs.jobs.meancalculation.CalculateMean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCalculateMean {
	// For the convenience of test, it's desirable to have block size smaller
	private static final int BLOCK_SIZE = 32 * 1024 * 1024;
	
  protected Configuration conf;
  protected Path input;
  protected Path output;
  protected FileSystem fs;
  
	// For the convenience of test, it's desirable to have block size smaller
//	private static final int BLOCK_SIZE = 8 * 1024 * 1024;
  
  @Before
  public void setup() throws IOException {
    conf = new Configuration();
    conf.set("fs.default.name", "file:///");
    conf.set("mapred.job.tracker", "local");
    conf.setInt("fs.local.block.size", BLOCK_SIZE);
    
    input = new Path("tmp/test_mean_calculation.data");
    output = new Path("tmp/mean_output");
    	
    fs = FileSystem.getLocal(conf);
    // overwrite input file
    double mean = 80, sd = 40; 
		DataSet dataSet = DataSetFactory.makeNormalDist(mean, sd);
		dataSet.setNumReords(1000 * 1000 * 50);
		//dataSet.setNumReords(1000 * 1000);
		CascadingSampledDataOutputStream out = CascadingSampledDataOutputStream.create(fs, input);
		dataSet.dump(out);
		// delete old output file
    fs.delete(output, true);
  }

  @Test
  public void test() throws Exception {
  	CalculateMean calculateMean = getCalculateMeanInstance(); 
  	calculateMean.setConf(conf);
    
    int exitCode = calculateMean.run(new String[] {input.toString(), output.toString()});
    Assert.assertEquals(0, exitCode);
    
    validateOuput();
  }

  private void validateOuput() throws IOException {
    InputStream in = null;
    try {
      in = fs.open(new Path("tmp/mean_output/part-r-00000"));
      
      BufferedReader br = new BufferedReader(new InputStreamReader(in));
      Assert.assertEquals(true, br.readLine().startsWith("count"));
      Assert.assertEquals(true, br.readLine().startsWith("sum"));
      Assert.assertEquals(true, br.readLine().startsWith("average"));
    } finally {
      IOUtils.closeStream(in);
    }
  }
  
  protected CalculateMean getCalculateMeanInstance() {
  	return new CalculateMean();
  }
}
