package me.tatetian.hs.jobs.meancalculation;

import java.io.IOException;

import me.tatetian.hs.jobs.ConstructIndex;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;

public class TestCalculateMeanWithIndexedSampling extends TestCalculateMean {
	
	@Override
	protected CalculateMean getCalculateMeanInstance() {
		samplingRatio = 0.01f;
		
		//constructIndex();
 	
		return new CalculateMeanWithIndexedSampling();
	}
	
	private void constructIndex() {
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
