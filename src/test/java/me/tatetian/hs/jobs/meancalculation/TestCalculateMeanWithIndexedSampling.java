package me.tatetian.hs.jobs.meancalculation;

import java.io.IOException;

import me.tatetian.hs.jobs.ConstructIndex;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;

public class TestCalculateMeanWithIndexedSampling extends TestCalculateMean {
	// For the convenience of test, it's desirable to have block size smaller
	private static final int BLOCK_SIZE = 8 * 1024 * 1024;
	
	@Override
	protected CalculateMean getCalculateMeanInstance() {
		constructIndex();
 	
		float samplingRatio = 0.05f;
		conf.setFloat("cps.sampling.ratio", samplingRatio);
		return new CalculateMeanWithIndexedSampling(samplingRatio);
	}
	
	private void constructIndex() {
    conf.setInt("dfs.blocksize", BLOCK_SIZE);
		
		// Remove old output
		Path output = new Path("tmp/test_mean_calculation_construct_index"); 
    try {
			fs.delete(output, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
    
		// Construct index
  	ConstructIndex constructIndex = new ConstructIndex();
  	constructIndex.setConf(conf); 
    int exitCode = -1;
		try {
			exitCode = constructIndex.run(new String[] {input.toString(), output.toString()});
		} catch (Exception e) {
			e.printStackTrace();
		}
    Assert.assertEquals(0, exitCode);
	}
}
